const http  = require('http');
const https = require('https');
const url   = require('url');
const zlib  = require('zlib');
var JSZip   = require('jszip');
if (JSZip.default) JSZip = JSZip.default;
const { Pool } = require('pg');

// ── Import auth routes ────────────────────────────────────────
const handleAuth = require('./auth');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const PORT = process.env.PORT || 3737;

// ── ANSI colors for nice terminal output ──
const C = { reset:'\x1b[0m', green:'\x1b[32m', yellow:'\x1b[33m', red:'\x1b[31m', cyan:'\x1b[36m', gray:'\x1b[90m', bold:'\x1b[1m' };

function log(level, msg) {
  var ts  = new Date().toTimeString().slice(0,8);
  var col = level==='OK'?C.green : level==='ERR'?C.red : level==='REQ'?C.cyan : C.yellow;
  console.log(C.gray+'['+ts+'] '+C.reset+col+'['+level+']'+C.reset+' '+msg);
}

// ── Parse body helper ──
function getBody(req, callback) {
  let body = '';
  req.on('data', chunk => { body += chunk.toString(); });
  req.on('end', () => {
    if (!body) return callback({});
    try {
      callback(JSON.parse(body));
    } catch (e) {
      console.error("JSON parse error:", e);
      callback({});
    }
  });
}

// ── CORS headers helper — applied to EVERY response ──
function setCORSHeaders(res) {
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS, PATCH');
  res.setHeader('Access-Control-Allow-Headers', '*');
  res.setHeader('Access-Control-Expose-Headers','*');
}

function escapeXml(str) {
  return String(str)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&apos;');
}

function soapRequest(fusionUrl, soapPath, basicAuth, action, body) {
  return new Promise(function (resolve, reject) {
    var parsed = url.parse(fusionUrl);
    var protocol = parsed.protocol === 'https:' ? https : http;
    var bodyBuf = Buffer.from(body, 'utf-8');
    var opts = {
      hostname: parsed.hostname,
      port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      path: soapPath, method: 'POST',
      headers: {
        'Authorization'  : basicAuth,
        'Content-Type'   : 'text/xml; charset=utf-8',
        'Content-Length' : bodyBuf.length,
        'SOAPAction'     : action
      },
      rejectUnauthorized: false
    };
    var r = protocol.request(opts, function (resp) {
      var chunks = [];
      resp.on('data', function (c) { chunks.push(c); });
      resp.on('end', function () {
        resolve({ status: resp.statusCode, body: Buffer.concat(chunks).toString('utf-8') });
      });
    });
    r.on('error', reject);
    r.write(bodyBuf);
    r.end();
  });
}

function parseFolderItems(soapXml, parentPath) {
  var items = [];
  var itemRegex = /<[^>]*?:?item\b[^>]*>([\s\S]*?)<\/[^>]*?:?item>/gi;
  var match;
  while ((match = itemRegex.exec(soapXml)) !== null) {
    var block     = match[1];
    var nameMatch = block.match(/<[^>]*?:?name[^>]*>([^<]+)<\/[^>]*?:?name>/i);
    var typeMatch = block.match(/<[^>]*?:?objectType[^>]*>([^<]+)<\/[^>]*?:?objectType>/i);
    var pathMatch = block.match(/<[^>]*?:?path[^>]*>([^<]+)<\/[^>]*?:?path>/i);
    if (!nameMatch) continue;
    var name     = nameMatch[1].trim();
    var objType  = typeMatch ? typeMatch[1].trim().toLowerCase() : '';
    var fullPath = pathMatch ? pathMatch[1].trim() : (parentPath + '/' + name);
    if (objType === 'folder' || objType === 'briefingbook') {
      items.push({ name: name, path: fullPath, type: 'folder' });
    } else if (name.endsWith('.xdm') || objType === 'datamodel' || objType === 'xdm') {
      items.push({ name: name, path: fullPath, type: 'xdm' });
    }
  }
  if (items.length === 0) {
    var pathItems = soapXml.match(/\/shared\/[^"<\s]+/g);
    if (pathItems) {
      var seen = new Set();
      pathItems.forEach(function (p) {
        if (seen.has(p)) return;
        seen.add(p);
        var parts    = p.split('/');
        var lastName = parts[parts.length - 1];
        if (lastName.endsWith('.xdm')) {
          items.push({ name: lastName, path: p, type: 'xdm' });
        }
      });
    }
  }
  return items;
}

function extractSqlFromXdm(xdmXml) {
  var cdataMatch = xdmXml.match(/<sql[^>]*>\s*<!\[CDATA\[([\s\S]*?)\]\]>\s*<\/sql>/i);
  if (cdataMatch) return cdataMatch[1].trim();
  var sqlMatch = xdmXml.match(/<sql[^>]*>([\s\S]*?)<\/sql>/i);
  if (sqlMatch) return sqlMatch[1].trim();
  return '';
}

function extractDataSource(xdmXml) {
  var dsMatch = xdmXml.match(/defaultDataSourceRef="([^"]+)"/i);
  if (dsMatch) return dsMatch[1];
  var dsMatch2 = xdmXml.match(/dataSourceRef="([^"]+)"/i);
  if (dsMatch2) return dsMatch2[1];
  return '';
}


// ── Create the server ──
var server = http.createServer(function(req, res) {

  // Set CORS headers FIRST on every request
  setCORSHeaders(res);

  // Handle preflight OPTIONS for ALL routes
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  log('REQ', req.method + ' ' + req.url);

  // ── AUTH ROUTES (login, register, ping, user-count) ───────────
  if (handleAuth(req, res, pool, log, getBody)) return;

  // ── CONNECTIONS ───────────────────────────────────────────────

  // POST /connections — save a new connection
  if (req.url === '/connections' && req.method === 'POST') {
    return getBody(req, async data => {
      const { user_id, name, type, host, port, database_name, username, password } = data;

      if (!user_id || !name || !type) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ message: 'Missing required fields: user_id, name, type' }));
      }

      try {
        const result = await pool.query(
          `INSERT INTO connections (user_id, name, type, host, port, database_name, username, password)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
          [user_id, name, type, host || null, port || null, database_name || null, username || null, password || null]
        );

        res.writeHead(201, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ message: 'Connection saved', connection: result.rows[0] }));
        log('OK', 'Connection saved: ' + name + ' for user ' + user_id);

      } catch (err) {
        console.error(err);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ message: 'DB error' }));
      }
    });
  }

  // GET /connections?user_id=X — fetch all connections for a user
  if (req.url.startsWith('/connections') && req.method === 'GET') {
    return (async () => {
      const parsedUrl = url.parse(req.url, true);
      const user_id = parsedUrl.query.user_id;

      if (!user_id) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ message: 'Missing user_id query param' }));
      }

      try {
        const result = await pool.query(
          'SELECT * FROM connections WHERE user_id=$1 ORDER BY created_at ASC',
          [user_id]
        );

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ connections: result.rows }));
        log('OK', 'Fetched ' + result.rows.length + ' connections for user ' + user_id);

      } catch (err) {
        console.error(err);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ message: 'DB error' }));
      }
    })();
  }

  // DELETE /connections/:id — remove a connection
  var deleteMatch = req.url.match(/^\/connections\/(\d+)$/);
  if (deleteMatch && req.method === 'DELETE') {
    var connId = parseInt(deleteMatch[1], 10);
    return (async () => {
      try {
        const result = await pool.query('DELETE FROM connections WHERE id=$1 RETURNING id', [connId]);

        if (result.rows.length === 0) {
          res.writeHead(404, { 'Content-Type': 'application/json' });
          return res.end(JSON.stringify({ message: 'Connection not found' }));
        }

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ message: 'Connection deleted' }));
        log('OK', 'Deleted connection id=' + connId);

      } catch (err) {
        console.error(err);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ message: 'DB error' }));
      }
    })();
  }

  // ── UPLOAD CATALOG ────────────────────────────────────────────
  if (req.url === '/upload-catalog' && req.method === 'POST') {
    return getBody(req, async function(data) {
      var fusionUrl = (data.fusionUrl || '').trim().replace(/\/+$/, '').replace(/^http:/, 'https:');
      var username  = (data.username  || '').trim();
      var password  = (data.password  || '').trim();

      if (!fusionUrl || !username || !password) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ ok: false, message: 'Missing fusionUrl, username, or password' }));
      }

      var parsedFusion;
      try {
        parsedFusion = url.parse(fusionUrl);
      } catch(e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ ok: false, message: 'Invalid Fusion URL: ' + e.message }));
      }

      // Helper: do one HTTP/S request
      function doRequest(parsedUrl, method, headers, body) {
        return new Promise(function(resolve, reject) {
          var protocol = parsedUrl.protocol === 'https:' ? https : http;
          var opts = {
            hostname: parsedUrl.hostname,
            port: parsedUrl.port || (parsedUrl.protocol === 'https:' ? 443 : 80),
            path: parsedUrl.path || '/',
            method: method,
            headers: headers,
            rejectUnauthorized: false
          };
          var r = protocol.request(opts, function(resp) {
            var chunks = [];
            var encoding = resp.headers['content-encoding'];
            var stream = resp;
            if (encoding === 'gzip') {
              stream = resp.pipe(zlib.createGunzip());
            } else if (encoding === 'deflate') {
              stream = resp.pipe(zlib.createInflate());
            }
            stream.on('data', function(c) { chunks.push(c); });
            stream.on('end', function() {
              resolve({ status: resp.statusCode, headers: resp.headers, body: Buffer.concat(chunks).toString() });
            });
            stream.on('error', reject);
          });
          r.on('error', reject);
          if (body) r.write(body);
          r.end();
        });
      }

      // Helper: upload BIP object
      async function uploadBIPObject(fusionUrl, username, password, objectType, path, zippedB64) {
        var soap = '<?xml version="1.0" encoding="UTF-8"?>' +
          '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v2="http://xmlns.oracle.com/oxp/service/v2">' +
          '<soapenv:Header/>' +
          '<soapenv:Body>' +
          '<v2:uploadObject>' +
          '<v2:userID>' + username + '</v2:userID>' +
          '<v2:password>' + password + '</v2:password>' +
          '<v2:objectType>' + objectType + '</v2:objectType>' +
          '<v2:reportObjectAbsolutePathURL>' + path + '</v2:reportObjectAbsolutePathURL>' +
          '<v2:objectZippedData>' + zippedB64 + '</v2:objectZippedData>' +
          '</v2:uploadObject>' +
          '</soapenv:Body>' +
          '</soapenv:Envelope>';
        var buf    = Buffer.from(soap, 'utf8');
        var parsed = url.parse(fusionUrl + '/xmlpserver/services/v2/CatalogService');
        return await doRequest(parsed, 'POST', {
          'Content-Type'   : 'text/xml; charset=UTF-8',
          'Content-Length' : buf.length,
          'SOAPAction'     : 'uploadObject',
          'Accept-Encoding': 'identity'
        }, buf);
      }

      var lastStatus = 0;
      var lastBody   = '';
      var uploaded   = false;

      // ── Step 1: Login ──────────────────────────────────────
      var loginSoap = '<?xml version="1.0" encoding="UTF-8"?>' +
        '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:saw="com.siebel.analytics.web/soap/v2">' +
        '<soapenv:Body>' +
        '<saw:logon>' +
        '<saw:name>' + username + '</saw:name>' +
        '<saw:password>' + password + '</saw:password>' +
        '</saw:logon>' +
        '</soapenv:Body>' +
        '</soapenv:Envelope>';

      var sessionID = '';
      try {
        var loginBuf    = Buffer.from(loginSoap, 'utf8');
        var loginParsed = url.parse(fusionUrl + '/analytics-ws/saw.dll?SoapImpl=nQSessionService');
        var loginResult = await doRequest(loginParsed, 'POST', {
          'Content-Type'   : 'text/xml; charset=UTF-8',
          'Content-Length' : loginBuf.length,
          'SOAPAction'     : 'logon',
          'Accept-Encoding': 'identity'
        }, loginBuf);
        log('REQ', 'Login status: ' + loginResult.status);
        var sessionMatch = loginResult.body.match(/<sessionID[^>]*>([^<]+)<\/sessionID>/i) ||
                           loginResult.body.match(/<sawsoap:sessionID[^>]*>([^<]+)<\/sawsoap:sessionID>/i);
        if (sessionMatch) {
          sessionID = sessionMatch[1];
          log('REQ', 'Session ID obtained: ' + sessionID.substring(0, 20) + '...');
        }
      } catch(e) {
        log('ERR', 'Login failed: ' + e.message);
      }

      if (sessionID) {
        try {

          // ── Step 2: Create folder /shared/Custom/QueryForgeDataZen ──
          log('REQ', 'Creating folder...');
          var createFolderSoap = '<?xml version="1.0" encoding="UTF-8"?>' +
            '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:saw="com.siebel.analytics.web/soap/v2">' +
            '<soapenv:Body>' +
            '<saw:createFolder>' +
            '<saw:path>/shared/Custom/QueryForgeDataZen</saw:path>' +
            '<saw:createIfNotExists>true</saw:createIfNotExists>' +
            '<saw:sessionID>' + sessionID + '</saw:sessionID>' +
            '</saw:createFolder>' +
            '</soapenv:Body>' +
            '</soapenv:Envelope>';
          var folderBuf    = Buffer.from(createFolderSoap, 'utf8');
          var folderParsed = url.parse(fusionUrl + '/analytics-ws/saw.dll?SoapImpl=webCatalogService');
          var folderResult = await doRequest(folderParsed, 'POST', {
            'Content-Type'   : 'text/xml; charset=UTF-8',
            'Content-Length' : folderBuf.length,
            'SOAPAction'     : 'createFolder',
            'Accept-Encoding': 'identity'
          }, folderBuf);
          log('REQ', 'CreateFolder status: ' + folderResult.status);
          log('REQ', 'CreateFolder body: ' + folderResult.body);

      // ── Step 3: Upload Data Model ──────────────────────
          log('REQ', 'Uploading data model...');
          var dataModelXml = '<?xml version="1.0" encoding="utf-8"?>\n' +
            '<dataModel xmlns="http://xmlns.oracle.com/oxp/xmlp" version="2.0" ' +
            'xmlns:xdm="http://xmlns.oracle.com/oxp/xmlp" ' +
            'xmlns:xsd="http://wwww.w3.org/2001/XMLSchema" ' +
            'defaultDataSourceRef="ApplicationDB_FSCM">\n' +
            '<description><![CDATA[QueryForgeDataZenDataModel_csv]]></description>\n' +
            '<dataProperties>\n' +
            '<property name="include_parameters" value="false"/>\n' +
            '<property name="include_null_Element" value="false"/>\n' +
            '<property name="include_rowsettag" value="false"/>\n' +
            '<property name="xml_tag_case" value="upper"/>\n' +
            '<property name="generate_output_format" value="xml"/>\n' +
            '</dataProperties>\n' +
            '<dataSets>\n' +
            '<dataSet name="sqlResultsSet" type="simple">\n' +
            '<sql dataSourceRef="ApplicationDB_FSCM" nsQuery="true" sp="true" xmlRowTagName="" bindMultiValueAsCommaSepStr="false">\n' +
            '<![CDATA[DECLARE\n' +
            '    type sys_refcursor is REF CURSOR;\n' +
            '    xdo_cursor  sys_refcursor;\n' +
            '    l_sql_query  RAW(32767);\n' +
            '    var11       CLOB := \'\';\n' +
            '    l_encoded_clob CLOB := :sql_query;\n' +
            '    l_decoded_clob CLOB;\n' +
            '    l_chunk_size INTEGER := 32000;\n' +
            '    l_buffer VARCHAR2(32767);\n' +
            '    l_pos INTEGER := 1;\n' +
            '    l_length INTEGER := DBMS_LOB.getlength(l_encoded_clob);\n' +
            'BEGIN\n' +
            '    DBMS_LOB.createtemporary(l_decoded_clob, TRUE);\n' +
            '    WHILE l_pos <= l_length LOOP\n' +
            '        DBMS_LOB.READ(l_encoded_clob, l_chunk_size, l_pos, l_buffer);\n' +
            '        l_sql_query := UTL_RAW.CAST_TO_RAW(l_buffer);\n' +
            '        var11 := TO_CLOB(UTL_RAW.CAST_TO_VARCHAR2(UTL_ENCODE.BASE64_DECODE(l_sql_query)));\n' +
            '        DBMS_LOB.writeappend(l_decoded_clob, LENGTH(var11), var11);\n' +
            '        l_pos := l_pos + l_chunk_size;\n' +
            '    END LOOP;\n' +
            '    OPEN :xdo_cursor FOR l_decoded_clob;\n' +
            '    DBMS_LOB.freetemporary(l_decoded_clob);\n' +
            'END;]]>\n' +
            '</sql>\n' +
            '</dataSet>\n' +
            '</dataSets>\n' +
            '<output rootName="DATA_DS" uniqueRowName="false">\n' +
            '<nodeList name="sqlResultsSet"/>\n' +
            '</output>\n' +
            '<eventTriggers/>\n' +
            '<lexicals/>\n' +
            '<parameters>\n' +
            '<parameter name="sql_query" dataType="xsd:string" rowPlacement="1">\n' +
            '<input label="sql_query"/>\n' +
            '</parameter>\n' +
            '<parameter name="xdo_cursor" dataType="xsd:string" rowPlacement="1">\n' +
            '<input label="xdo_cursor"/>\n' +
            '</parameter>\n' +
            '</parameters>\n' +
            '<valueSets/>\n' +
            '<bursting/>\n' +
            '</dataModel>';

          var zip1 = new JSZip();
          zip1.file('QueryForgeDataZenDataModel_csv.xdm', dataModelXml);
          var dmZipped = await zip1.generateAsync({ type: 'nodebuffer', compression: 'DEFLATE' });
          var dmB64    = dmZipped.toString('base64');
          var dmResult = await uploadBIPObject(fusionUrl, username, password, 'xdmz',
            '/Custom/QueryForgeDataZen/QueryForgeDataZenDataModel_csv', dmB64);
          log('REQ', 'UploadDM status: ' + dmResult.status);
          log('REQ', 'UploadDM body: ' + dmResult.body);

          // ── Step 4: Upload Report ──────────────────────────
          log('REQ', 'Uploading report...');
         var reportXml = '<?xml version = \'1.0\' encoding = \'utf-8\'?>\n' +
  '<report xmlns="http://xmlns.oracle.com/oxp/xmlp" xmlns:xsd="http://wwww.w3.org/2001/XMLSchema" version="2.0" ' +
  'dataModel="true" useBipParameters="true" producerName="fin-financialCommon" ' +
  'parameterVOName="" parameterTaskFlow="" customDataControl="" ' +
  'cachePerUser="false" cacheSmartRefresh="false" cacheUserRefresh="false">\n' +
  '   <dataModel url="/Custom/QueryForgeDataZen/QueryForgeDataZenDataModel_csv.xdm"/>\n' +
  '   <description><![CDATA[QueryForgeDataZenReport_csv]]></description>\n' +
  '   <property name="showControls" value="true"/>\n' +
  '   <property name="online" value="true"/>\n' +
  '   <property name="openLinkInNewWindow" value="true"/>\n' +
  '   <property name="autoRun" value="false"/>\n' +
  '   <property name="cacheDocument" value="false"/>\n' +
  '   <property name="showReportLinks" value="true"/>\n' +
  '   <property name="asynchronousRun" value="false"/>\n' +
  '   <property name="priority" value="normal"/>\n' +
  '   <property name="sendOutputAsUrl" value="insLevel"/>\n' +
  '   <property name="cacheDuration" value="30"/>\n' +
  '   <property name="controledByExtApp" value="true"/>\n' +
  '   <property name="ignoreEmailDomainRestrictions" value="false"/>\n' +
  '   <property name="saveXMLForRepublishing" value="true"/>\n' +
  '   <property name="disableMakeOutputPublic" value="false"/>\n' +
  '   <parameters paramPerLine="3" style="parameterLocation:in-horizontal;promptLocation:side;"/>\n' +
  '   <templates default="blank">\n' +
  '      <template label="blank" url="blank.xpt" type="xpt" outputFormat="csv" defaultFormat="csv" locale="en_US" active="true" viewOnline="true"/>\n' +
  '   </templates>\n' +
  '</report>';

          var zip2 = new JSZip();
          zip2.file('QueryForgeDataZenReport_csv.xdo', reportXml);
          var rptZipped = await zip2.generateAsync({ type: 'nodebuffer', compression: 'DEFLATE' });
          var rptB64    = rptZipped.toString('base64');
          var rptResult = await uploadBIPObject(fusionUrl, username, password, 'xdoz',
            '/Custom/QueryForgeDataZen/QueryForgeDataZenReport_csv', rptB64);
          log('REQ', 'UploadReport status: ' + rptResult.status);
          log('REQ', 'UploadReport body: ' + rptResult.body);

          // ── Step 5: Upload Blank Template ─────────────────
          log('REQ', 'Uploading template...');
          var templateXml = '<?xml version="1.0" encoding="utf-8"?>\n' +
            '<xsl:stylesheet version="1.0" ' +
            'xmlns:xsl="http://www.w3.org/1999/XSL/Transform">\n' +
            '<xsl:output method="text" encoding="utf-8"/>\n' +
            '<xsl:template match="/">\n' +
            '<xsl:for-each select="//ROW">\n' +
            '<xsl:value-of select="."/>\n' +
            '</xsl:for-each>\n' +
            '</xsl:template>\n' +
            '</xsl:stylesheet>';

          var zip3 = new JSZip();
          zip3.file('blank.xpt', templateXml);
          var tplZipped = await zip3.generateAsync({ type: 'nodebuffer', compression: 'DEFLATE' });
          var tplB64    = tplZipped.toString('base64');
          var tplResult = await uploadBIPObject(fusionUrl, username, password, 'xssz',
            '/Custom/QueryForgeDataZen/QueryForgeDataZenReport_csv/blank', tplB64);
          log('REQ', 'UploadTemplate status: ' + tplResult.status);
          log('REQ', 'UploadTemplate body: ' + tplResult.body);

          lastStatus = tplResult.status;
          lastBody   = tplResult.body;

          if (dmResult.status === 200 && !dmResult.body.includes('Fault') &&
              rptResult.status === 200 && !rptResult.body.includes('Fault') &&
              tplResult.status === 200 && !tplResult.body.includes('Fault')) {
            uploaded = true;
          }

        } catch(e) {
          log('ERR', 'Error: ' + e.message);
          lastBody = e.message;
        }

      } else {
        lastStatus = 401;
        lastBody = 'Could not obtain session token — check credentials';
      }

      if (uploaded) {
        log('OK', 'All objects deployed successfully');
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, message: 'Deployed successfully', status: lastStatus }));
      } else {
        log('ERR', 'Failed — last status: ' + lastStatus);
        res.writeHead(lastStatus || 502, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, message: 'Failed', detail: lastBody, status: lastStatus }));
      }
    });
  }

  // ── SERVE CATALOG BROWSER HTML ───────────────────────────────
  if (req.url === '/catalog' && req.method === 'GET') {
    var fs = require('fs');
    try {
      var cbHtml = fs.readFileSync(__dirname + '/catalog-browser.html', 'utf-8');
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(cbHtml);
    } catch(e) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('catalog-browser.html not found');
    }
    return;
  }

  // ── CATALOG: LIST FOLDER CHILDREN ────────────────────────────
  if (req.url === '/catalog/folders' && req.method === 'POST') {
    return getBody(req, async function (data) {
      var fusionUrl  = (data.fusionUrl || '').trim().replace(/\/+$/, '');
      var username   = (data.username  || '').trim();
      var password   = (data.password  || '').trim();
      var folderPath = (data.path || '/shared').trim();
      if (!fusionUrl || !username || !password) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ ok: false, message: 'Missing fusionUrl, username, or password' }));
      }
      var basicAuth = 'Basic ' + Buffer.from(username + ':' + password).toString('base64');

// Step 1: Get session token first
var loginSoap =
  '<?xml version="1.0" encoding="UTF-8"?>' +
  '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:saw="com.siebel.analytics.web/soap/v2">' +
  '<soapenv:Body><saw:logon>' +
  '<saw:name>' + escapeXml(username) + '</saw:name>' +
  '<saw:password>' + escapeXml(password) + '</saw:password>' +
  '</saw:logon></soapenv:Body></soapenv:Envelope>';

var loginResult = await soapRequest(fusionUrl, '/analytics/saw.dll?SoapImpl=nQSessionService', basicAuth, 'logon', loginSoap);
log('REQ', 'Logon status: ' + loginResult.status);
log('REQ', 'Logon body: ' + loginResult.body.slice(0, 500));
var sessionMatch = loginResult.body.match(/<sessionID[^>]*>([^<]+)<\/sessionID>/i);
if (!sessionMatch) {
  res.writeHead(401, { 'Content-Type': 'application/json' });
  return res.end(JSON.stringify({ ok: false, message: 'Could not obtain session token — check credentials', debug: loginResult.body.slice(0, 300) }));
}
var sessionID = sessionMatch[1];

// Step 2: Now fetch folder contents with sessionID
var soapBody =
  '<?xml version="1.0" encoding="utf-8"?>' +
  '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:saw="com.siebel.analytics.web/soap/v2">' +
  '<soapenv:Body>' +
  '<saw:getFolderContents>' +
  '<saw:path>' + escapeXml(folderPath) + '</saw:path>' +
  '<saw:sessionID>' + sessionID + '</saw:sessionID>' +
  '</saw:getFolderContents>' +
  '</soapenv:Body></soapenv:Envelope>';
      try {
        //var result = await soapRequest(fusionUrl, '/analytics/saw.dll?SoapImpl=nQSessionService', basicAuth, 'getFolderContents', soapBody);
        var result = await soapRequest(fusionUrl, '/analytics-ws/saw.dll?SoapImpl=webCatalogService', basicAuth, 'getFolderContents', soapBody);
        if (result.status !== 200) {
          res.writeHead(result.status, { 'Content-Type': 'application/json' });
          return res.end(JSON.stringify({ ok: false, message: 'Fusion SOAP error HTTP ' + result.status, detail: result.body }));
        }
        var items = parseFolderItems(result.body, folderPath);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, items: items }));
      } catch (e) {
        res.writeHead(502, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, message: 'Request failed: ' + e.message }));
      }
    });
  }

  // ── CATALOG: FETCH XDM AND EXTRACT SQL ───────────────────────
  if (req.url === '/catalog/xdm' && req.method === 'POST') {
    return getBody(req, async function (data) {
      var fusionUrl = (data.fusionUrl || '').trim().replace(/\/+$/, '');
      var username  = (data.username  || '').trim();
      var password  = (data.password  || '').trim();
      var xdmPath   = (data.path || '').trim();
      if (!fusionUrl || !username || !password || !xdmPath) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ ok: false, message: 'Missing required fields' }));
      }
      var basicAuth = 'Basic ' + Buffer.from(username + ':' + password).toString('base64');
      var soapBody =
        '<?xml version="1.0" encoding="utf-8"?>' +
        '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" ' +
        'xmlns:v2="http://xmlns.oracle.com/oxp/service/v2">' +
        '<soapenv:Body>' +
        '<v2:downloadObject>' +
        '<v2:reportObjectPath>' + escapeXml(xdmPath) + '</v2:reportObjectPath>' +
        '</v2:downloadObject>' +
        '</soapenv:Body>' +
        '</soapenv:Envelope>';
      try {
        var result = await soapRequest(fusionUrl, '/xmlpserver/services/v2/BIPReportService', basicAuth, 'downloadObject', soapBody);
        if (result.status !== 200) {
          res.writeHead(result.status, { 'Content-Type': 'application/json' });
          return res.end(JSON.stringify({ ok: false, message: 'Failed to fetch XDM: HTTP ' + result.status, detail: result.body }));
        }
        var b64Match = result.body.match(/<[^>]*downloadObjectReturn[^>]*>([^<]+)<\/[^>]*downloadObjectReturn>/);
        if (!b64Match) {
          b64Match = result.body.match(/<[^>]*return[^>]*>([A-Za-z0-9+/=\s]+)<\/[^>]*return>/);
        }
        var xdmXml = '';
        if (b64Match) {
          var b64 = b64Match[1].replace(/\s/g, '');
          xdmXml = Buffer.from(b64, 'base64').toString('utf-8');
        } else {
          var inlineMatch = result.body.match(/<dataModel[\s\S]*<\/dataModel>/);
          if (inlineMatch) {
            xdmXml = inlineMatch[0];
          } else {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            return res.end(JSON.stringify({ ok: false, message: 'Could not extract XDM content', raw: result.body.slice(0, 500) }));
          }
        }
        var sql        = extractSqlFromXdm(xdmXml);
        var dataSource = extractDataSource(xdmXml);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, sql: sql, dataSource: dataSource, xdmPath: xdmPath }));
      } catch (e) {
        res.writeHead(502, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, message: 'Request failed: ' + e.message }));
      }
    });
  }
  
  // ── PROXY (pass-through for everything else) ─────────────────
  var targetUrl = req.headers['x-target-url'];

  if (!targetUrl) {
    res.writeHead(400, { 'Content-Type': 'text/plain' });
    res.end('Missing X-Target-URL header.');
    return;
  }

  var parsed;
  try {
    parsed = url.parse(targetUrl);
  } catch(e) {
    res.writeHead(400, { 'Content-Type': 'text/plain' });
    res.end('Invalid URL: ' + targetUrl);
    return;
  }

  var fwdHeaders = {};
  Object.keys(req.headers).forEach(function(k) {
    if (k !== 'x-target-url' && k !== 'host' && k !== 'origin' && k !== 'referer') {
      fwdHeaders[k] = req.headers[k];
    }
  });
  fwdHeaders['host'] = parsed.hostname;

  var options = {
    hostname : parsed.hostname,
    port     : parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
    path     : parsed.path || '/',
    method   : req.method,
    headers  : fwdHeaders,
    rejectUnauthorized: false
  };

  log('REQ', req.method + ' → ' + parsed.hostname + options.path);

  var protocol = parsed.protocol === 'https:' ? https : http;

  var proxyReq = protocol.request(options, function(proxyRes) {
    var skip = ['transfer-encoding','connection','keep-alive','proxy-authenticate','proxy-authorization','te','trailers','upgrade'];
    var outHeaders = {};
    Object.keys(proxyRes.headers).forEach(function(k) {
      if (skip.indexOf(k) === -1) outHeaders[k] = proxyRes.headers[k];
    });

    outHeaders['Access-Control-Allow-Origin']  = '*';
    outHeaders['Access-Control-Expose-Headers']= '*';

    res.writeHead(proxyRes.statusCode, outHeaders);
    proxyRes.pipe(res, { end:true });
    log('OK', 'Response ' + proxyRes.statusCode + ' from ' + parsed.hostname);
  });

  proxyReq.on('error', function(e) {
    log('ERR', e.message);
    if (!res.headersSent) {
      res.writeHead(502, { 'Content-Type': 'text/plain' });
    }
    res.end('Proxy error: ' + e.message);
  });

  req.pipe(proxyReq, { end:true });
});

server.listen(PORT, '0.0.0.0', function() {
  console.log(`Proxy running on port ${PORT}`);
});
