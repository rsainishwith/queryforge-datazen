/* ── SQL Highlight Patterns ─────────────────────────────────── */
var SQL_KW=/\b(SELECT|FROM|WHERE|AND|OR|NOT|IN|EXISTS|BETWEEN|LIKE|IS|NULL|JOIN|LEFT|RIGHT|INNER|OUTER|FULL|CROSS|ON|GROUP\s+BY|ORDER\s+BY|HAVING|UNION|ALL|DISTINCT|AS|LIMIT|OFFSET|INSERT|INTO|VALUES|UPDATE|SET|DELETE|CREATE|TABLE|INDEX|VIEW|DROP|ALTER|ADD|COLUMN|PRIMARY|KEY|FOREIGN|REFERENCES|CONSTRAINT|DEFAULT|UNIQUE|CHECK|ROWNUM|FETCH|NEXT|ROWS|ONLY|WITH|CASE|WHEN|THEN|ELSE|END|BEGIN|COMMIT|ROLLBACK|TRUNCATE|TOP|ASC|DESC|BY|CONNECT|PRIOR|LEVEL|PIVOT|MERGE|USING|MATCHED|DECLARE|PROCEDURE|FUNCTION|PACKAGE|RETURN|EXCEPTION|DUAL|SYSDATE|SYSTIMESTAMP)\b/gi;
var SQL_FN=/\b(COUNT|SUM|AVG|MAX|MIN|COALESCE|NVL|NVL2|DECODE|TRIM|LTRIM|RTRIM|UPPER|LOWER|SUBSTR|INSTR|LENGTH|TO_DATE|TO_CHAR|TO_NUMBER|TO_TIMESTAMP|TRUNC|ROUND|FLOOR|CEIL|MOD|ABS|SIGN|POWER|SQRT|CONCAT|REPLACE|TRANSLATE|LPAD|RPAD|RANK|DENSE_RANK|ROW_NUMBER|LEAD|LAG|OVER|PARTITION|LISTAGG|WITHIN|EXTRACT|MONTHS_BETWEEN|ADD_MONTHS|LAST_DAY|NEXT_DAY|CAST|NULLIF|GREATEST|LEAST|REGEXP_LIKE|REGEXP_SUBSTR|REGEXP_REPLACE|SYS_GUID|WM_CONCAT)\s*(?=\()/gi;

/* ── State ──────────────────────────────────────────────────── */
var tabCounter=0, tabs=[], activeTab=0;
var connections=[], activeConn=null;
var resultData=[], resultCols=[];
var bindVarHistory = {};
var fontSize=13, running=false;
var sortCol=null, sortAsc=true;
var colFilters={};

/* ── Catalog State ───────────────────────────────────────────── */
var catalogLoaded=false, activeCatNode=null;

/* ── Filter popup working state ─────────────────────────────── */
var fpCol=null, fpAllValues=[], fpPending=new Set();

/* ══════════ INIT ══════════════════════════════════════════════ */
window.onload=function(){
  var session=null;
  try{
    session=JSON.parse(sessionStorage.getItem('qfdz_session')||localStorage.getItem('qfdz_session')||'null');
  }catch(e){}
  if(session&&session.id){
    document.getElementById('titlebar-username').textContent=session.name||session.username;
    document.getElementById('titlebar-user').style.display='flex';
  }
  loadConns();
  addTab();
  var ta=document.getElementById('sqled');
  ta.addEventListener('input',function(){doHL();doLN();autoCorrectSQL();});
  ta.addEventListener('scroll',syncScroll);
  ta.addEventListener('keydown',handleKeys);
  ta.addEventListener('keyup',updatePos);
  ta.addEventListener('click',updatePos);
  initResizer();
  initCatalogResizer();
  doHL();doLN();
  setTimeout(function(){checkProxy(function(){});},500);
  initSidebarCollapsed();
  document.addEventListener('mousedown',function(e){
    var popup=document.getElementById('filter-popup');
    if(popup.classList.contains('show')&&!popup.contains(e.target)&&!e.target.closest('.th-filter-btn')){
      fpClose();
    }
  });
};

/* ══════════ TABS ══════════════════════════════════════════════ */
function addTab(){
  tabCounter++;
  var tabNum = tabs.filter(function(t){return !t.readonly;}).length + 1;
  tabs.push({id:tabCounter,name:'New '+tabNum,sql:'',results:null,cols:[],elapsed:null});
  renderTabs();
  activateTab(tabs.length-1);
}
function removeTab(i,e){
  e.stopPropagation();
  if(tabs.length===1){tabs[0].sql='';document.getElementById('sqled').value='';doHL();doLN();return;}
  // Save editor into the tab being closed ONLY if it's not readonly
  if(tabs[i] && !tabs[i].readonly){
    tabs[i].sql = document.getElementById('sqled').value;
  }
  tabs.splice(i,1);
  if(activeTab>=i && activeTab>0) activeTab=activeTab-1;
  if(activeTab>=tabs.length) activeTab=tabs.length-1;
  // Directly load the target tab SQL into editor — bypass activateTab's save logic
  var target = tabs[activeTab];
  document.getElementById('sqled').value = target.sql||'';
  document.getElementById('sqled').readOnly = target.readonly||false;
  document.getElementById('sqled').style.opacity = target.readonly?'0.8':'1';
  doHL();doLN();
  colFilters={};sortCol=null;
  if(target.results){
    resultData=target.results;resultCols=target.cols;
    renderTable();
  }else{
    resultData=[];resultCols=[];
    document.getElementById('rarea').innerHTML='<div class="nodata">No data found</div>';
    document.getElementById('res-info').textContent='No data found';
    document.getElementById('srows').textContent='';
    document.getElementById('stime').textContent='';
  }
  renderTabs();
}
function activateTab(i){
  // Save current tab SQL only if not readonly
  if(tabs[activeTab] && !tabs[activeTab].readonly){
    tabs[activeTab].sql=document.getElementById('sqled').value;
  }
  activeTab=i;
  document.getElementById('sqled').value=tabs[i].sql||'';
  document.getElementById('sqled').readOnly=tabs[i].readonly||false;
  document.getElementById('sqled').style.opacity=tabs[i].readonly?'0.8':'1';
  doHL();doLN();
  colFilters={};sortCol=null;
  if(tabs[i].results){
    resultData=tabs[i].results;resultCols=tabs[i].cols;
    renderTable();
  }else{
    resultData=[];resultCols=[];
    document.getElementById('rarea').innerHTML='<div class="nodata">No data found</div>';
    document.getElementById('res-info').textContent='No data found';
    document.getElementById('srows').textContent='';
    document.getElementById('stime').textContent='';
  }
  renderTabs();
}
function renderTabs(){
  var bar=document.getElementById('tabbar');
  bar.innerHTML=tabs.map(function(t,i){
    return '<div class="tab'+(i===activeTab?' active':'')+'" onclick="activateTab('+i+')">'
     +(t.readonly?'🔒':'📄')+' '+esc(t.name)
      +' <span class="tab-x" onclick="removeTab('+i+',event)">✕</span></div>';
  }).join('')+'<div class="tab-add" onclick="addTab()" title="New tab">+</div>';
}

/* ══════════ CONNECTIONS ═══════════════════════════════════════ */
function getCurrentUserId(){
  try{
    var s=JSON.parse(sessionStorage.getItem('qfdz_session')||localStorage.getItem('qfdz_session')||'null');
    return s&&s.id?String(s.id):null;
  }catch(e){return null;}
}

function doLogout(){
  var uid=getCurrentUserId();
  if(uid){localStorage.removeItem('csWebConns_u'+uid);}
  sessionStorage.removeItem('qfdz_session');
  localStorage.removeItem('qfdz_session');
  window.location.href='login.html';
}

async function loadConns(){
  var uid=getCurrentUserId();
  var localKey=uid?'csWebConns_u'+uid:'csWebConns_guest';
  if(!uid){
    try{connections=JSON.parse(localStorage.getItem(localKey)||'[]');}catch(e){connections=[];}
    renderConnSel();renderSavedConns();checkBanner();return;
  }
  try{
    var r=await fetch(PROXY_URL+'/connections?user_id='+encodeURIComponent(uid));
    var d=await r.json();
    connections=(d.connections||[]).map(function(c){
      return {id:c.id,name:c.name,type:c.type,url:c.host,user:c.username,pass:c.password,host:c.host,port:c.port,database_name:c.database_name,reportPath:'/Custom/QueryForgeDataZen/QueryForgeDataZenReport_csv.xdo'
              //reportPath:'/Custom/CloudSQL/CloudSQLReport_csv.xdo'
             };
    });
  }catch(e){
    console.warn('Could not fetch connections from server, using localStorage fallback',e);
    try{connections=JSON.parse(localStorage.getItem(localKey)||'[]');}catch(e2){connections=[];}
  }
  renderConnSel();renderSavedConns();checkBanner();
}

function saveConns(){
  var uid=getCurrentUserId();
  var localKey=uid?'csWebConns_u'+uid:'csWebConns_guest';
  try{localStorage.setItem(localKey,JSON.stringify(connections));}catch(e){}
}
function renderConnSel(){
  var sel=document.getElementById('csel'),cur=sel.value;
  sel.innerHTML='<option value="">— Select a connection —</option>';
  connections.forEach(function(c,i){var o=document.createElement('option');o.value=i;o.textContent=c.name;sel.appendChild(o);});
  if(cur!=='')sel.value=cur;
}
function selectConn(){
  var v=document.getElementById('csel').value;
  if(v===''){activeConn=null;setStatus('','Not connected');document.getElementById('title-conn').textContent='No connection selected';return;}
  activeConn=connections[parseInt(v)];
  setStatus('ok',activeConn.name);
  document.getElementById('title-conn').textContent='Connected to ' + activeConn.name;
  loadCatalogTree();
}
function switchMTab(name){
  document.querySelectorAll('.mtab').forEach(function(t,i){var names=['new','saved'];t.classList.toggle('on',names[i]===name);document.getElementById('panel-'+names[i]).classList.toggle('on',names[i]===name);});
  if(name==='saved')renderSavedConns();
}
function renderSavedConns(){
  var el=document.getElementById('saved-list');
  if(!connections.length){el.innerHTML='<div style="color:#888;font-size:12px;padding:8px 0;">No connections saved yet.</div>';return;}
  el.innerHTML='<table style="width:100%;font-size:12px;border-collapse:collapse;"><thead><tr style="background:#e8f0fb;"><th style="padding:5px 8px;text-align:left;">Name</th><th style="padding:5px 8px;text-align:left;">URL</th><th></th></tr></thead><tbody>'
    +connections.map(function(c,i){return '<tr><td style="padding:5px 8px;border-bottom:1px solid #eef2f8;">'+esc(c.name)+'</td><td style="padding:5px 8px;border-bottom:1px solid #eef2f8;color:#555;">'+esc(c.url)+'</td><td style="padding:5px 8px;border-bottom:1px solid #eef2f8;"><button onclick="loadConn('+i+')" style="font-size:11px;padding:2px 8px;border:1px solid #1565c0;background:#fff;color:#1565c0;border-radius:3px;cursor:pointer;margin-right:4px;">Load</button><button onclick="deleteConn('+i+')" style="font-size:11px;padding:2px 8px;border:1px solid #b71c1c;background:#fff;color:#b71c1c;border-radius:3px;cursor:pointer;">Delete</button></td></tr>';}).join('')+'</tbody></table>';
}
function loadConn(i){document.getElementById('csel').value=i;selectConn();closeConnModal();}
async function deleteConn(i){
  if(!confirm('Delete "'+connections[i].name+'"?'))return;
  var conn=connections[i];
  var uid=getCurrentUserId();
  if(uid&&conn.id){
    try{await fetch(PROXY_URL+'/connections/'+conn.id,{method:'DELETE'});}catch(e){console.warn('Server delete failed',e);}
  }
  connections.splice(i,1);saveConns();renderConnSel();renderSavedConns();checkBanner();
  if(activeConn&&!connections.includes(activeConn)){activeConn=null;setStatus('','Not connected');document.getElementById('title-conn').textContent='No connection selected';}
}
function openConnModal(){document.getElementById('conn-overlay').classList.add('show');setMStatus('','');}
function closeConnModal(){document.getElementById('conn-overlay').classList.remove('show');}
function setMStatus(type,msg){var el=document.getElementById('mc-status');el.className='mstatus'+(type?' '+type:'');el.textContent=msg;}
function testConn(){
  var url=document.getElementById('mc-url').value.trim();
  if(!url){setMStatus('err','Enter a URL');return;}
  setMStatus('info','Checking URL format…');
  setTimeout(function(){try{new URL(url);setMStatus('ok','✓ URL valid.');}catch(e){setMStatus('err','Invalid URL: '+e.message);}},600);
}
async function saveConn(){
  var name=document.getElementById('mc-name').value.trim();
  var url=document.getElementById('mc-url').value.trim().replace(/\/$/,'');
  var user=document.getElementById('mc-user').value.trim();
  var pass=document.getElementById('mc-pass').value;
  //var path = '/Custom/CloudSQL/CloudSQLReport_csv.xdo';
  var path = '/Custom/QueryForgeDataZen/QueryForgeDataZenReport_csv.xdo';
  if(!name){setMStatus('err','Enter a connection name');return;}
  if(!url){setMStatus('err','Enter the Oracle URL');return;}
  if(!user){setMStatus('err','Enter username');return;}
  var uid=getCurrentUserId();
  if(uid){
    try{
      var r=await fetch(PROXY_URL+'/connections',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({user_id:parseInt(uid),name:name,type:'oracle',host:url,username:user,password:pass})});
      var d=await r.json();
      if(!r.ok){setMStatus('err',d.message||'Failed to save');return;}
      var saved=d.connection;
      connections.push({id:saved.id,name:name,url:url,user:user,pass:pass,reportPath:path});
    }catch(e){
      console.warn('Server save failed, saving locally',e);
      connections.push({name,url,user,pass,reportPath:path});
      saveConns();
    }
  }else{
    connections.push({name,url,user,pass,reportPath:path});
    saveConns();
  }
  renderConnSel();
document.getElementById('csel').value=connections.length-1;
selectConn();closeConnModal();checkBanner();

// Auto-deploy catalog to Fusion
setMStatus('info','Deploying catalog to Fusion...');
try{
  var dr=await fetch(PROXY_URL+'/upload-catalog',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({fusionUrl:url,username:user,password:pass})});
  var dd=await dr.json();
  if(dd.ok){setMStatus('ok','Connection saved & catalog deployed!');}
  else{setMStatus('warn','Connection saved. Catalog deploy failed: '+dd.message);}
}catch(e){
  setMStatus('warn','Connection saved. Catalog deploy failed: '+e.message);
}
}
function checkBanner(){document.getElementById('setup-banner').classList.toggle('show',connections.length===0);}

/* ══════════ PROXY ═════════════════════════════════════════════ */
var PROXY_URL='https://queryforge-backend-mroa.onrender.com',proxyAvailable=false,proxyChecked=false;
function checkProxy(cb){
  if(proxyChecked){cb(proxyAvailable);return;}
  var el=document.getElementById('proxy-badge');
  if(el){el.textContent='⏳ Checking for Active Server…';el.style.color='#92400e';el.style.background='#fef3c7';}
  fetch(PROXY_URL,{method:'OPTIONS',signal:AbortSignal.timeout(20000)})
    .then(function(){proxyAvailable=true;proxyChecked=true;updateProxyStatus(true);cb(true);})
    .catch(function(){
      setTimeout(function(){
        fetch(PROXY_URL,{method:'OPTIONS',signal:AbortSignal.timeout(15000)})
          .then(function(){proxyAvailable=true;proxyChecked=true;updateProxyStatus(true);cb(true);})
          .catch(function(){proxyAvailable=false;proxyChecked=true;updateProxyStatus(false);cb(false);});
      },3000);
    });
}
function updateProxyStatus(ok){
  var el=document.getElementById('proxy-badge');if(!el)return;
  el.textContent=ok?'🟢 Server Active':'🔴 No Active Server';
  el.style.color=ok?'#1b5e20':'#b71c1c';el.style.background=ok?'#e8f5e9':'#ffebee';
}
function makeFetch(targetUrl,options){
  if(proxyChecked&&!proxyAvailable){var age=Date.now()-(makeFetch._lastCheck||0);if(age>15000){proxyChecked=false;makeFetch._lastCheck=Date.now();}}
  return new Promise(function(resolve){
    checkProxy(function(hasProxy){
      if(hasProxy){var h=Object.assign({},options.headers||{});h['X-Target-URL']=targetUrl;resolve(fetch(PROXY_URL,Object.assign({},options,{headers:h})));}
      else resolve(fetch(targetUrl,options));
    });
  });
}
function soapCall(url,body,auth){return makeFetch(url,{method:'POST',headers:{'Content-Type':'application/soap+xml; charset=UTF-8','Authorization':auth},body:body});}

/* ══════════ ORACLE ════════════════════════════════════════════ */
function basicAuth(c){return 'Basic '+btoa(c.user+':'+c.pass);}
function toBase64(s){try{return btoa(unescape(encodeURIComponent(s)));}catch(e){return btoa(s);}}
function buildRunReportSOAP(conn,sql){
  //var b64=toBase64(sql),rp=conn.reportPath||'/Custom/CloudSQL/CloudSQLReport_csv.xdo';
  var b64=toBase64(sql),rp=conn.reportPath||'/Custom/QueryForgeDataZen/QueryForgeDataZenReport_csv.xdo';
  return '<?xml version="1.0" encoding="UTF-8"?><soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:pub="http://xmlns.oracle.com/oxp/service/PublicReportService"><soap:Header/><soap:Body><pub:runReport><pub:reportRequest><pub:reportAbsolutePath>'+rp+'</pub:reportAbsolutePath><pub:sizeOfDataChunkDownload>-1</pub:sizeOfDataChunkDownload><pub:flattenXML>false</pub:flattenXML><pub:parameterNameValues><pub:item><pub:name>sql_query</pub:name><pub:values><pub:item><![CDATA['+b64+']]></pub:item></pub:values></pub:item><pub:item><pub:name>xdo_cursor</pub:name><pub:values><pub:item></pub:item></pub:values></pub:item></pub:parameterNameValues></pub:reportRequest></pub:runReport></soap:Body></soap:Envelope>';
}
function parseCSVResponse(xml){
  var m=xml.match(/<([a-zA-Z0-9_:]+)reportBytes[^>]*>([\s\S]*?)<\/\1reportBytes>/);
  if(!m)return{rows:[],cols:[],error:'No reportBytes found.\n'+xml};
  var b64=(m[2]||'').trim(),csv='';
  try{
    var binary=atob(b64);
    var bytes=new Uint8Array(binary.length);
    for(var i=0;i<binary.length;i++) bytes[i]=binary.charCodeAt(i);
    csv=new TextDecoder('utf-8').decode(bytes);
    if(csv.charCodeAt(0)===0xFEFF) csv=csv.slice(1);
  }
  catch(e){return{rows:[],cols:[],error:'Base64 decode failed: '+e.message};}
  return parseCSV(csv);
}
function parseCSV(text){
  var lines=text.trim().split('\n');
  if(!lines.length||!lines[0].trim())return{rows:[],cols:[]};
  function pr(line){
    var r=[],cur='',inQ=false;
    for(var i=0;i<line.length;i++){
      var c=line[i];
      if(c==='"'){if(inQ&&line[i+1]==='"'){cur+='"';i++;}else inQ=!inQ;}
      else if(c===','&&!inQ){r.push(cur);cur='';}
      else cur+=c;
    }
    r.push(cur);return r;
  }
  var cols=pr(lines[0]),rows=[];
  for(var i=1;i<lines.length;i++){
    if(!lines[i].trim())continue;
    var v=pr(lines[i]),obj={};
    cols.forEach(function(c,j){obj[c]=v[j]!==undefined?v[j]:'';});
    rows.push(obj);
  }
  return{rows,cols};
}

/* ══════════ RUN QUERY ═════════════════════════════════════════ */
function runQuery(){
  if(running && !setLoading._active){ running=false; setLoading(false); setStage(''); }
  var ta = document.getElementById('sqled');
  tabs[activeTab].sql = ta.value;
  var fullText  = ta.value;
  var cursorPos = ta.selectionStart;
  var selStart  = ta.selectionStart;
  var selEnd    = ta.selectionEnd;
  var sql = '';
  var mode = '';
  if(selEnd > selStart){
    sql  = fullText.slice(selStart, selEnd).trim();
    mode = 'selection';
  } else {
    var stmts = [];
    var re = /[^;]+/g, m;
    while((m = re.exec(fullText)) !== null){
      var s = m.index, e = m.index + m[0].length;
      var text = m[0].trim();
      if(text) stmts.push({ text: text, start: s, end: e });
    }
    var found = null;
    for(var i = 0; i < stmts.length; i++){
      if(cursorPos >= stmts[i].start && cursorPos <= stmts[i].end + 1){ found = stmts[i]; break; }
    }
    if(!found && stmts.length) found = stmts[stmts.length - 1];
    sql  = found ? found.text : fullText.trim();
    mode = stmts.length > 1 ? 'statement' : 'full';
  }
  if(!activeConn){ showErr('No connection selected.\nChoose or add a connection first.'); return; }
  if(!sql){ showErr(mode === 'selection' ? 'Selected text is empty.' : 'Write a SQL query first.'); return; }
  sql = sql.replace(/;\s*$/, '');
  setStage(mode === 'selection' ? '▶ Running selection…' : mode === 'statement' ? '▶ Running statement at cursor…' : '');
  var params = detectBindVars(sql);
  if(params.length > 0){
    running = true;
    setLoading(true);
    setStage('Waiting for bind variable values…');
    openBindVarsModal(sql, params);
    return;
  }
  _executeSQL(sql);
}
function _executeSQL(sql){
  var val = document.getElementById('fetchrows').value;
  var limit = (val === 'ALL') ? Infinity : parseInt(val) || 100;
  clearResults();
  running=true;setLoading(true);setStage('Running...');
  var auth=basicAuth(activeConn),conn=activeConn,t0=Date.now();
  soapCall(conn.url+'/xmlpserver/services/ExternalReportWSSService',buildRunReportSOAP(conn,sql),auth)
    .then(function(resp){
      if(resp.status===401)throw new Error('Authentication failed (HTTP 401).');
      if(resp.status===403)throw new Error('Access denied (HTTP 403).');
      //if(resp.status===404)throw new Error('Report not found (HTTP 404).\n'+(conn.reportPath||'/Custom/CloudSQL/CloudSQLReport_csv.xdo'));
      if(resp.status===404)throw new Error('Report not found (HTTP 404).\n'+(conn.reportPath||'/Custom/QueryForgeDataZen/QueryForgeDataZenReport_csv.xdo'));
      //if(!resp.ok)return resp.text().then(function(t){throw new Error('HTTP '+resp.status+':\n'+t.slice(0,500));});
      if(!resp.ok)return resp.text().then(function(t){
       var oraMatch=t.match(/(ORA-\d+[^<\n]*)/i);
        if(oraMatch){throw new Error(oraMatch[1].trim());}
        var plainText=t.replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim();
        // Extract everything after the LAST colon chain — that's the real error
        var afterGenerate=plainText.match(/generateReport[^>]*?failed[^>]*?:\s*(.+)/i);
        var afterData=plainText.match(/DataException:\s*(.+)/i);
        var afterServer=plainText.match(/ServerException:\s*(.+)/i);
        // Get the deepest/last meaningful message
        var lastColon=plainText.lastIndexOf(': ');
        var deepMsg=lastColon>-1?plainText.slice(lastColon+2).trim():'';
        if(deepMsg&&deepMsg.length>5)throw new Error(deepMsg.slice(0,400));
        else if(afterData)throw new Error(afterData[1].trim().slice(0,400));
        else if(afterServer)throw new Error(afterServer[1].trim().slice(0,400));
        else if(afterGenerate)throw new Error(afterGenerate[1].trim().slice(0,400));
        else throw new Error(plainText.slice(0,400));
      });
      return resp.text();
    })
    .then(function(xml){
      var elapsed=((Date.now()-t0)/1000).toFixed(2);
     var fm=xml.match(/<(?:faultstring|message)[^>]*>([\s\S]*?)<\/(?:faultstring|message)>/);
      if(fm&&xml.indexOf('<pub:reportBytes')===-1){
        var rawMsg=fm[1].trim();
        var friendlyMsg='';
        // First priority: ORA- error (actual Oracle DB error)
        var oraMatch=rawMsg.match(/(ORA-\d+[^<\n]*)/i);
        if(oraMatch){
          friendlyMsg=oraMatch[1].trim();
        } else {
          // Strip all XML tags to get plain text
          var plainText=rawMsg.replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim();
          // Try to extract the most useful part after known prefixes
          var dataEx=plainText.match(/DataException:\s*(.+)/i);
          var serverEx=plainText.match(/ServerException:\s*(.+)/i);
          var generateEx=plainText.match(/generateReport[^:]*:\s*(.+)/i);
          if(dataEx) friendlyMsg=dataEx[1].trim().slice(0,300);
          else if(serverEx) friendlyMsg=serverEx[1].trim().slice(0,300);
          else if(generateEx) friendlyMsg=generateEx[1].trim().slice(0,300);
          else friendlyMsg=plainText.slice(0,300);
        }
        throw new Error(friendlyMsg);
      }
     setStage('Parsing data…');
      return new Promise(function(resolve,reject){
        setTimeout(function(){
          try{
            var p=parseCSVResponse(xml);
            if(p.error){showErr(p.error);resolve();return;}
            setStage('Rendering '+p.rows.length+' rows…');
            setTimeout(function(){
              var rows=(limit===Infinity)?p.rows:p.rows.slice(0,limit);
              var cols=p.cols;
              tabs[activeTab].results=rows;tabs[activeTab].cols=cols;tabs[activeTab].elapsed=elapsed;
              resultData=rows;resultCols=cols;colFilters={};sortCol=null;
              renderTable();setStatus('ok',conn.name);
              resolve();
            },0);
          }catch(e){reject(e);}
        },0);
      });
    })
    .catch(function(e){
      var msg=e.message||String(e);
      if(/fetch|Failed to fetch|NetworkError|Load failed/i.test(msg)){
        msg='Unable to reach the server.\nPlease check your connection or contact your administrator.';
      } else if(/401|Authentication/i.test(msg)){
        msg='Authentication failed.\nPlease check your username and password.';
      } else if(/403/i.test(msg)){
        msg='Access denied.\nYou do not have permission to run this report.';
      } else if(/404/i.test(msg)){
        msg='Report not found on the server.\nPlease verify the connection setup.';
      } else if(/500/i.test(msg)){
        msg=msg; // already cleaned above — pass through friendly ORA- message
      }
      showErr(msg);setStatus('err',conn?conn.name:'Error');
    })
    .finally(function(){running=false;setLoading(false);setStage('');});
}
function stopQuery(){running=false;setLoading(false);setStage('Stopped');setTimeout(function(){setStage('');},2000);}

/* ══════════ TABLE RENDER ══════════════════════════════════════ */
function getFilteredData(){
  return resultData.filter(function(row){
    return resultCols.every(function(c){
      var f=colFilters[c];
      if(!f)return true;
      return f.has(String(row[c]==null?'':row[c]));
    });
  });
}

/* ══════════ VIRTUAL SCROLL STATE ══════════════════════════════ */
var vsFiltered=[];

function renderTable(){
  var filtered=getFilteredData();
  vsFiltered=filtered;
  var elapsed=tabs[activeTab]?(tabs[activeTab].elapsed||'0'):'0';
  var total=resultData.length;
  document.getElementById('res-info').textContent=
    filtered.length+' row'+(filtered.length!==1?'s':'')
    +(filtered.length!==total?' (filtered from '+total+')':'')
    +' in '+elapsed+'s';
  document.getElementById('srows').textContent=filtered.length+' rows';
  document.getElementById('stime').textContent=elapsed+'s';
  document.getElementById('sir-input').value='';
  document.getElementById('sir-count').textContent='';
  _sirMatches=[];_sirIdx=-1;_sirLastQ='';

  if(!filtered.length){
    document.getElementById('rarea').innerHTML='<div class="nodata">Query returned no rows</div>';
    if(gtcOpen)buildGtcList('');
    return;
  }

  var COL_W=160;
  var totalW=40+resultCols.length*COL_W;
  var colgroup='<colgroup><col style="width:40px;">'
    +resultCols.map(function(){return '<col style="width:'+COL_W+'px;">';}).join('')
    +'</colgroup>';

  var h='<div style="width:100%;overflow-x:auto;overflow-y:auto;height:100%;">'
    +'<table id="vs-table" style="table-layout:fixed;width:'+totalW+'px;min-width:100%;border-collapse:collapse;">'
    +colgroup
    +'<thead><tr><th class="rn-col" style="width:40px;position:sticky;top:0;z-index:5;">#</th>';
  resultCols.forEach(function(c){
    var arrow=sortCol===c?(sortAsc?' ▲':' ▼'):'';
    var hasFilter=colFilters[c]!=null;
    var iconFill=hasFilter?'currentColor':'none';
    h+='<th style="position:sticky;top:0;z-index:5;">'
      +'<div class="th-inner">'
      +'<span class="th-label" onclick="clickSort(\''+escJ(c)+'\')" title="Sort by '+esc(c)+'">'+esc(c)+arrow+'</span>'
      +'<button class="th-filter-btn'+(hasFilter?' active':'')+'" '
      +'onclick="fpOpen(event,\''+escJ(c)+'\')" title="Filter">'
      +'<svg width="11" height="11" viewBox="0 0 24 24" fill="'+iconFill+'" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">'
      +'<polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3"/>'
      +'</svg>'
      +'</button>'
      +'</div>'
      +'<div class="col-resizer" onmousedown="startResize(event)"></div>'
      +'</th>';
  });
  h+='</tr></thead><tbody id="vs-tbody">';
  for(var i=0;i<filtered.length;i++){
    var row=filtered[i];
    h+='<tr><td class="rn-col">'+(i+1)+'</td>';
    resultCols.forEach(function(c){
      var v=row[c];
      if(v===null||v===undefined||v==='')h+='<td class="null-cell">(null)</td>';
      else h+='<td>'+esc(String(v))+'</td>';
    });
    h+='</tr>';
  }
  h+='</tbody></table></div>';

  document.getElementById('rarea').innerHTML=h;

  (function(){
    var startX,startW,th,colIdx;
    window.startResize=function(e){
      th=e.target.closest('th');
      colIdx=Array.from(th.parentElement.children).indexOf(th);
      startX=e.clientX;startW=th.offsetWidth;
      document.addEventListener('mousemove',onMove);
      document.addEventListener('mouseup',onUp);
      e.preventDefault();
    };
    function onMove(e){
      var w=Math.max(60,startW+(e.clientX-startX));
      var cols=document.querySelectorAll('#vs-table colgroup col');
      if(cols[colIdx])cols[colIdx].style.width=w+'px';
      var total=0;
      cols.forEach(function(c){total+=parseInt(c.style.width)||COL_W;});
      document.getElementById('vs-table').style.width=total+'px';
    }
    function onUp(){
      document.removeEventListener('mousemove',onMove);
      document.removeEventListener('mouseup',onUp);
    }
  })();

  if(gtcOpen)buildGtcList(document.getElementById('gtc-search').value);
}

/* ══════════ SORT ══════════════════════════════════════════════ */
function clickSort(col){
  if(sortCol===col)sortAsc=!sortAsc;else{sortCol=col;sortAsc=true;}
  resultData.sort(function(a,b){return cmpVals(a[col],b[col],sortAsc);});
  if(tabs[activeTab])tabs[activeTab].results=resultData.slice();
  renderTable();
}
function cmpVals(v1,v2,asc){
  if(v1==null&&v2==null)return 0;
  if(v1==null)return 1;if(v2==null)return -1;
  var n1=parseFloat(v1),n2=parseFloat(v2);
  if(!isNaN(n1)&&!isNaN(n2))return asc?n1-n2:n2-n1;
  return asc?String(v1).localeCompare(String(v2)):String(v2).localeCompare(String(v1));
}

/* ══════════ EXCEL FILTER POPUP ════════════════════════════════ */
function fpOpen(e,col){
  e.stopPropagation();
  fpCol=col;
  var popup=document.getElementById('filter-popup');
  var seen=new Set();
  resultData.forEach(function(row){seen.add(String(row[col]==null?'':row[col]));});
  fpAllValues=Array.from(seen).sort(function(a,b){
    var na=parseFloat(a),nb=parseFloat(b);
    if(!isNaN(na)&&!isNaN(nb))return na-nb;
    return a.localeCompare(b);
  });
  var existing=colFilters[col];
  fpPending=existing?new Set(existing):new Set(fpAllValues);
  document.getElementById('fp-search').value='';
  fpRenderList(fpAllValues);
  popup.style.visibility='hidden';
  popup.style.top='0px';
  popup.style.left='0px';
  popup.style.bottom='auto';
  popup.classList.add('show');
  var btn=e.currentTarget, rect=btn.getBoundingClientRect();
  var popupH=popup.offsetHeight;
  var popupW=popup.offsetWidth||280;
  var spaceBelow=window.innerHeight-rect.bottom-6;
  var spaceAbove=rect.top-6;
  var left=rect.left;
  if(left+popupW>window.innerWidth-4)left=window.innerWidth-popupW-4;
  if(left<4)left=4;
  popup.style.left=left+'px';
  if(spaceBelow>=popupH){
    popup.style.top=(rect.bottom+4)+'px';popup.style.bottom='auto';
  } else if(spaceAbove>=popupH){
    popup.style.top='auto';popup.style.bottom=(window.innerHeight-rect.top+4)+'px';
  } else {
    if(spaceBelow>=spaceAbove){
      popup.style.top=(rect.bottom+4)+'px';popup.style.bottom='auto';popup.style.maxHeight=spaceBelow+'px';
    } else {
      popup.style.top='auto';popup.style.bottom=(window.innerHeight-rect.top+4)+'px';popup.style.maxHeight=spaceAbove+'px';
    }
  }
  popup.style.visibility='visible';
  document.getElementById('fp-sort-asc').classList.toggle('active-sort', sortCol===col && sortAsc);
  document.getElementById('fp-sort-desc').classList.toggle('active-sort', sortCol===col && !sortAsc);
  var lbl=col.length>20?col.slice(0,18)+'…':col;
  var lblEl=document.getElementById('fp-col-label');
  if(lblEl)lblEl.textContent=lbl;
}
function fpRenderList(values){
  var list=document.getElementById('fp-list');
  if(!values.length){list.innerHTML='<div style="padding:8px 14px;font-size:12px;color:#888;">No matching values</div>';return;}
  var allChecked=values.length>0&&values.every(function(v){return fpPending.has(v);});
  var h='<div class="fp-list-item" onclick="fpToggleAll()"><input type="checkbox" '+(allChecked?'checked':'')+'><label><b>Select All</b></label></div>';
  values.forEach(function(v){
    var checked=fpPending.has(v);
    var display=(v==='')?'<i style="color:#aaa">(Blank)</i>':esc(v);
    h+='<div class="fp-list-item" onclick="fpToggleVal(\''+escJ(v)+'\')"><input type="checkbox" '+(checked?'checked':'')+'><label title="'+esc(v)+'">'+display+'</label></div>';
  });
  list.innerHTML=h;
}
function fpSearchValues(q){
  var vals=q.trim()?fpAllValues.filter(function(v){return v.toLowerCase().includes(q.toLowerCase());}):fpAllValues;
  fpRenderList(vals);
}
function fpToggleVal(v){
  if(fpPending.has(v))fpPending.delete(v);else fpPending.add(v);
  fpSearchValues(document.getElementById('fp-search').value);
}
function fpToggleAll(){
  var q=document.getElementById('fp-search').value.trim();
  var visible=q?fpAllValues.filter(function(v){return v.toLowerCase().includes(q.toLowerCase());}):fpAllValues;
  var allOn=visible.every(function(v){return fpPending.has(v);});
  if(allOn){visible.forEach(function(v){fpPending.delete(v);});}
  else{visible.forEach(function(v){fpPending.add(v);});}
  fpRenderList(visible.length===fpAllValues.length?fpAllValues:visible);
}
function fpSort(dir){
  var col=fpCol;
  fpClose();
  if(!col)return;
  sortCol=col; sortAsc=(dir==='asc');
  resultData.sort(function(a,b){return cmpVals(a[col],b[col],sortAsc);});
  if(tabs[activeTab])tabs[activeTab].results=resultData.slice();
  renderTable();
}
function fpClearFilter(){
  var col=fpCol;fpClose();if(!col)return;
  colFilters[col]=null;renderTable();
}
function fpApply(){
  if(!fpCol)return;
  if(fpPending.size>=fpAllValues.length&&fpAllValues.every(function(v){return fpPending.has(v);})){
    colFilters[fpCol]=null;
  }else{
    colFilters[fpCol]=new Set(fpPending);
  }
  fpClose();renderTable();
}
function fpClose(){
  var popup=document.getElementById('filter-popup');
  popup.classList.remove('show');
  popup.style.maxHeight='';
  popup.style.visibility='';
  fpCol=null;
}

/* ══════════ EXPORT ════════════════════════════════════════════ */
function exportCSV(){
  var data=getFilteredData();if(!data.length){alert('No data to export.');return;}
  function q(v){return '"'+String(v==null?'':v).replace(/"/g,'""')+'"';}
  var lines=[resultCols.map(q).join(',')];
  data.forEach(function(r){lines.push(resultCols.map(function(c){return q(r[c]);}).join(','));});
  var blob=new Blob([lines.join('\r\n')],{type:'text/csv'});
  var a=document.createElement('a');a.href=URL.createObjectURL(blob);
  a.download='cloudsql_'+new Date().toISOString().slice(0,10)+'.csv';a.click();
}
function showErr(msg){document.getElementById('rarea').innerHTML='<div class="errdata">⚠ '+esc(msg)+'</div>';document.getElementById('res-info').textContent='Error';document.getElementById('srows').textContent='';document.getElementById('stime').textContent='';}
function clearResults(){document.getElementById('rarea').innerHTML='';document.getElementById('res-info').textContent='Running…';document.getElementById('srows').textContent='';document.getElementById('stime').textContent='';}

/* ══════════ RESIZER ═══════════════════════════════════════════ */
function initResizer(){
  var resizer=document.getElementById('resizer'),es=document.getElementById('editor-section');
  var dragging=false,startY,startH;
  resizer.addEventListener('mousedown',function(e){dragging=true;startY=e.clientY;startH=es.offsetHeight;document.body.style.userSelect='none';document.body.style.cursor='row-resize';e.preventDefault();});
  document.addEventListener('mousemove',function(e){if(!dragging)return;var nh=startH+(e.clientY-startY),c=document.getElementById('pane-container'),mn=80,mx=c.offsetHeight-120;es.style.height=Math.max(mn,Math.min(mx,nh))+'px';});
  document.addEventListener('mouseup',function(){if(!dragging)return;dragging=false;document.body.style.userSelect='';document.body.style.cursor='';});
}

/* ══════════ EDITOR ════════════════════════════════════════════ */
function handleKeys(e){
  if(e.key==='Tab'){e.preventDefault();var ta=e.target,s=ta.selectionStart;ta.value=ta.value.slice(0,s)+'  '+ta.value.slice(ta.selectionEnd);ta.selectionStart=ta.selectionEnd=s+2;doHL();doLN();}
  if(e.key==='F5'||((e.ctrlKey||e.metaKey)&&e.key==='Enter')){e.preventDefault();runQuery();}
  if((e.ctrlKey||e.metaKey)&&e.key==='f'){e.preventDefault();findInSQL();}
}
function updatePos(){var ta=document.getElementById('sqled'),v=ta.value.slice(0,ta.selectionStart),ls=v.split('\n');document.getElementById('spos').textContent='Ln '+ls.length+', Col '+(ls[ls.length-1].length+1);}
function syncScroll(){var ta=document.getElementById('sqled');document.getElementById('hl').scrollTop=ta.scrollTop;document.getElementById('hl').scrollLeft=ta.scrollLeft;document.getElementById('lnums').scrollTop=ta.scrollTop;}
function doLN(){var ta=document.getElementById('sqled'),n=(ta.value||'').split('\n').length,a=[];for(var i=1;i<=n;i++)a.push(i);document.getElementById('lnums').textContent=a.join('\n');}
function doHL(){document.getElementById('hl').innerHTML=sqlHL(document.getElementById('sqled').value||'');}
function autoCorrectSQL(){
  var ta=document.getElementById('sqled');
  var pos=ta.selectionStart;
  var val=ta.value;

  // Only trigger after a space or newline
  var lastChar=val[pos-1];
  if(lastChar!==' '&&lastChar!=='\n'&&lastChar!=='\t')return;

  // Common typo corrections (lowercase typo → correct)
  var typos={
    'selct':'SELECT','slect':'SELECT','seelct':'SELECT','selet':'SELECT',
    'freom':'FROM','fomr':'FROM','fro':'FROM',
    'wher':'WHERE','whre':'WHERE','wehre':'WHERE',
    'grop':'GROUP','grpup':'GROUP',
    'ordr':'ORDER','orderd':'ORDER',
    'havng':'HAVING','haivng':'HAVING',
    'joi':'JOIN','jion':'JOIN',
    'distint':'DISTINCT','distnct':'DISTINCT',
    'isert':'INSERT','inser':'INSERT',
    'updat':'UPDATE','updaet':'UPDATE',
    'delet':'DELETE','dleet':'DELETE',
    'creat':'CREATE','craete':'CREATE',
    'betwen':'BETWEEN','beteen':'BETWEEN',
    'liek':'LIKE','lke':'LIKE',
    'coun':'COUNT','conut':'COUNT',
    'nul':'NULL','nll':'NULL',
    'uinon':'UNION','unoin':'UNION',
    'alais':'ALIAS','alas':'AS',
    'limt':'LIMIT','liimt':'LIMIT'
  };

  // Get the word just before cursor
  var before=val.slice(0,pos-1); // exclude the space just typed
  var wordMatch=before.match(/(\S+)$/);
  if(!wordMatch)return;
  var word=wordMatch[1];
  var wordStart=pos-1-word.length;

  // Check typo map first
  var upper=word.toUpperCase();
  var corrected=typos[word.toLowerCase()]||null;

  // If not a typo, check if it's a known keyword to uppercase
  if(!corrected){
    var allKW=[
      'SELECT','FROM','WHERE','AND','OR','NOT','IN','EXISTS','BETWEEN','LIKE','IS','NULL',
      'JOIN','LEFT','RIGHT','INNER','OUTER','FULL','CROSS','ON','GROUP','ORDER','HAVING',
      'UNION','ALL','DISTINCT','AS','LIMIT','OFFSET','INSERT','INTO','VALUES','UPDATE',
      'SET','DELETE','CREATE','TABLE','INDEX','VIEW','DROP','ALTER','ADD','COLUMN',
      'PRIMARY','KEY','FOREIGN','REFERENCES','CONSTRAINT','DEFAULT','UNIQUE','CHECK',
      'WITH','CASE','WHEN','THEN','ELSE','END','BEGIN','COMMIT','ROLLBACK','TRUNCATE',
      'ASC','DESC','BY','CONNECT','PRIOR','LEVEL','PIVOT','MERGE','USING','MATCHED',
      'DECLARE','PROCEDURE','FUNCTION','PACKAGE','RETURN','EXCEPTION','DUAL','SYSDATE',
      'SYSTIMESTAMP','ROWNUM','FETCH','NEXT','ROWS','ONLY','COUNT','SUM','AVG','MAX',
      'MIN','COALESCE','NVL','TRIM','UPPER','LOWER','SUBSTR','LENGTH','TO_DATE',
      'TO_CHAR','TO_NUMBER','TRUNC','ROUND','REPLACE','RANK','DENSE_RANK','ROW_NUMBER',
      'LEAD','LAG','OVER','PARTITION','EXTRACT','CAST','NULLIF','GREATEST','LEAST'
    ];
    if(allKW.indexOf(upper)>-1) corrected=upper;
  }

  if(corrected && corrected!==word){
    ta.value=val.slice(0,wordStart)+corrected+val.slice(wordStart+word.length);
    ta.selectionStart=ta.selectionEnd=pos;
    doHL();doLN();
  }
}
function sqlHL(code){
  var s=code.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  s=s.replace(/(--[^\n]*)/g,'<span class="cm">$1</span>');
  s=s.replace(/('(?:[^'\\]|\\.)*')/g,'<span class="str">$1</span>');
  s=s.replace(SQL_FN,function(m){return '<span class="fn">'+m+'</span>';});
  s=s.replace(SQL_KW,function(m){return '<span class="kw">'+m+'</span>';});
  s=s.replace(/\b(\d+\.?\d*)\b/g,'<span class="num">$1</span>');
  return s;
}

function clearEditor(){document.getElementById('sqled').value='';doHL();doLN();}
function formatSQL(){
  var ta=document.getElementById('sqled'), v=ta.value;
  var literals=[];
  
  // Preserve string literals
  v=v.replace(/'(?:[^'\\]|''|\\.)*'/g,function(m){literals.push(m);return '\x00STR'+(literals.length-1)+'\x00';});
  
  // Normalize whitespace
  v=v.replace(/\s+/g,' ').trim();
  
  // Add line breaks before keywords
  v=v.replace(/\b(SELECT|FROM|WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|UNION|UNION\s+ALL|INTERSECT|MINUS)\b/gi, '\n$1');
  v=v.replace(/\b(JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|INNER\s+JOIN|FULL\s+JOIN|LEFT\s+OUTER\s+JOIN|RIGHT\s+OUTER\s+JOIN|CROSS\s+JOIN|ON)\b/gi, '\n$1');
  v=v.replace(/\b(AND|OR)\b/gi, '\n$1');
  v=v.replace(/\b(CASE|WHEN|THEN|ELSE|END)\b/gi, '\n  $1');
  
  // Split and format
  var lines=v.split('\n');
  var out=[];
  
  lines.forEach(function(line){
    var trimmed=line.trim();
    if(!trimmed) return;
    
    var lower=trimmed.toLowerCase();
    var indent='';
    
    // Main clauses - no indent
    if(/^(select|from|where|group by|having|order by|union|union all|intersect|minus)/.test(lower)){
      indent='';
    }
    // JOIN clauses - 4 spaces
    else if(/^(join|left join|right join|inner join|full join|cross join|on)/.test(lower)){
      indent='    ';
    }
    // AND/OR - 4 spaces
    else if(/^(and|or)/.test(lower)){
      indent='    ';
    }
    // CASE/WHEN/THEN/ELSE/END - 4-8 spaces
    else if(/^(case|when|then|else|end)/.test(lower)){
      indent='        ';
      if(/^case/.test(lower)) indent='    ';
      if(/^end/.test(lower)) indent='    ';
    }
    // Regular columns/tables - 4 spaces under SELECT/FROM
    else {
      indent='    ';
    }
    
    out.push(indent+lower);
  });
  
  v=out.join('\n');
  
  // Restore string literals
  v=v.replace(/\x00STR(\d+)\x00/g, function(_,i){ return literals[parseInt(i)]; });
  
  // Clean up
  v=v.replace(/^\n+/,'').replace(/\n{3,}/g,'\n\n').trim();
  
  ta.value=v;
  doHL();
  doLN();
   console.log('Formatted SQL:', ta.value.substring(0, 100));
}

function changeFontSize(d){fontSize=Math.max(10,Math.min(20,fontSize+d));['sqled','hl','lnums'].forEach(function(id){document.getElementById(id).style.fontSize=fontSize+'px';});}
function findInSQL(){var q=prompt('Find in SQL:');if(!q)return;var ta=document.getElementById('sqled'),idx=ta.value.toLowerCase().indexOf(q.toLowerCase());if(idx>-1){ta.focus();ta.setSelectionRange(idx,idx+q.length);}else alert('"'+q+'" not found.');}

/* ══════════ STATUS ════════════════════════════════════════════ */
function setStatus(state,label){document.getElementById('sdot').className='sdot'+(state?' '+state:'');document.getElementById('sconn').textContent=label||'Not connected';}
function setLoading(on){
  setLoading._active=!!on;
  document.getElementById('spinner').style.display=on?'inline-block':'none';
  if(on)document.getElementById('sdot').className='sdot busy';
}
function setStage(msg){document.getElementById('res-info').textContent = msg;}
function openHelp(){document.getElementById('help-overlay').classList.add('show');}

/* ══════════ BIND VARIABLES ════════════════════════════════════ */
var _bvPendingSQL = '';
var _bvParams = [];

function detectBindVars(sql){
  // Strip single-line comments
  var cleaned = sql.replace(/--[^\n]*/g, '');
  // Strip block comments
  cleaned = cleaned.replace(/\/\*[\s\S]*?\*\//g, '');
  // Strip string literals
  cleaned = cleaned.replace(/'(?:[^'\\]|''|\\.)*'/g, "''");
  // Skip internal BIP parameters used by the data model
  var INTERNAL = new Set(['SQL_QUERY','XDO_CURSOR','P_BIND_DUMMY']);
  var re = /(?<![:]):([A-Za-z_][A-Za-z0-9_]*)/g;
  var seen = new Set(), params = [];
  var m;
  while ((m = re.exec(cleaned)) !== null){
    var name = m[1].toUpperCase();
    if (!seen.has(name) && !INTERNAL.has(name)){
      seen.add(name);
      params.push(m[1]);
    }
  }
  return params;
}
    
function openBindVarsModal(sql, params){
  _bvPendingSQL = sql;
  _bvParams = params;
  var subtitle = document.getElementById('bv-subtitle');
  subtitle.textContent = params.length + ' variable' + (params.length !== 1 ? 's' : '') + ' detected';
  var body = document.getElementById('bv-body');
  var html = '';
  params.forEach(function(p){
    html += '<div class="bv-field" style="display:flex;flex-direction:column;gap:4px;">'
      + '<label>:' + esc(p) + '</label>'
      + '<div style="display:flex;gap:6px;align-items:center;">'
      + '<input type="text" id="bv-input-' + esc(p) + '" autocomplete="off" spellcheck="false" placeholder="Enter value…" style="flex:1;" value="' + esc(bindVarHistory[p.toUpperCase()] || '') + '"/>'
      + '<select id="bv-type-' + esc(p) + '" title="Data type" style="padding:3px 6px;border:1px solid #374151;border-radius:3px;background:#0f172a;color:#e5e7eb;font-size:11px;cursor:pointer;outline:none;">'
      + '<option value="string" selected>VARCHAR</option>'
      + '<option value="number">NUMBER</option>'
      + '<option value="date">DATE</option>'
      + '</select>'
      + '</div>'
      + '</div>';
  });
  body.innerHTML = html;
  document.getElementById('bv-overlay').classList.add('show');
  var first = body.querySelector('input');
  if (first) setTimeout(function() { first.focus(); }, 80);
  var inputs = body.querySelectorAll('input');
  inputs.forEach(function(inp, idx){
    inp.addEventListener('keydown', function(e){
      if (e.key === 'Enter'){
        e.preventDefault();
        if (idx < inputs.length - 1) inputs[idx + 1].focus();
        else bvSubmit();
      }
    });
  });
}
function bvCancel(){
  document.getElementById('bv-overlay').classList.remove('show');
  _bvPendingSQL = '';_bvParams = [];
  running = false;setLoading(false);setStage('Cancelled');
  setTimeout(function(){ setStage(''); }, 1500);
}
function bvSubmit(){
  var sql = _bvPendingSQL;
  _bvParams.forEach(function(p){
    var el    = document.getElementById('bv-input-' + esc(p));
    var tsel  = document.getElementById('bv-type-' + p);
    var val   = el ? el.value : '';
    bindVarHistory[p.toUpperCase()] = val;
    var dtype = tsel ? tsel.value : 'string';
    var re    = new RegExp('(?<![:])\:' + p + '(?![A-Za-z0-9_])', 'gi');
    var replacement;
    if (dtype === 'number'){
      replacement = /^-?\d+(\.\d+)?$/.test(val.trim()) ? val.trim() : '0';
    } else if (dtype === 'date'){
      replacement = "TO_DATE('" + val.replace(/'/g, "''") + "','YYYY-MM-DD')";
    } else {
      replacement = "'" + val.replace(/'/g, "''") + "'";
    }
    sql = sql.replace(re, replacement);
  });
  document.getElementById('bv-overlay').classList.remove('show');
  _bvPendingSQL = '';_bvParams = [];
  _executeSQL(sql);
}

/* ══════════ GO TO COLUMN PANEL ════════════════════════════════ */
var gtcOpen = false;

function toggleGtcPanel(){
  gtcOpen = !gtcOpen;
  var panel = document.getElementById('gtc-panel');
  panel.style.display = gtcOpen ? 'flex' : 'none';
  if (gtcOpen){ buildGtcList(''); setTimeout(function(){ document.getElementById('gtc-search').focus(); }, 60); }
}
function buildGtcList(filter){
  var list = document.getElementById('gtc-list');
  var cols = resultCols;
  var q = filter.toLowerCase();
  var filtered = q ? cols.filter(function(c){ return c.toLowerCase().includes(q); }) : cols;
  if (!filtered.length){ list.innerHTML = '<div style="padding:8px 12px;font-size:12px;color:#6b7280;">No columns</div>'; return; }
  list.innerHTML = filtered.map(function(c){
    return '<div class="gtc-item" onclick="gtcScrollTo(\'' + escJ(c) + '\')" title="' + esc(c) + '">' + esc(c) + '</div>';
  }).join('');
}
function gtcFilter(q){ buildGtcList(q); }
function gtcScrollTo(colName){
  var idx = resultCols.indexOf(colName);
  if (idx === -1) return;
  var table = document.querySelector('#rarea table');
  if (!table) return;
  var th = table.querySelectorAll('thead th')[idx + 1];
  if (!th) th = table.querySelectorAll('thead th')[idx];
  if (!th) return;
  var wrap = document.getElementById('rarea');
  wrap.scrollLeft = th.offsetLeft - wrap.offsetLeft - 8;
  document.querySelectorAll('.gtc-item').forEach(function(el){ el.classList.remove('hl'); });
  document.querySelectorAll('.gtc-item').forEach(function(el){
    if (el.textContent === colName) el.classList.add('hl');
  });
}

/* ══════════ SEARCH IN RESULT ══════════════════════════════════ */
var _sirMatches = [], _sirIdx = -1, _sirLastQ = '';

function sirSearch(q){
  _sirMatches = []; _sirIdx = -1; _sirLastQ = '';
  document.getElementById('sir-count').textContent = '';
  // Clear all highlights
  document.querySelectorAll('#vs-tbody td.sir-hl, #vs-tbody td.sir-hl-cur').forEach(function(td){
    td.classList.remove('sir-hl','sir-hl-cur');
  });
  if(!q.trim()){ _sirLastQ=''; return; }
  var lq = q.toLowerCase();
  _sirLastQ = lq;
  var filtered = vsFiltered;
  filtered.forEach(function(row, rowIdx){
    resultCols.forEach(function(c){
      var v = row[c];
      if(v !== null && v !== undefined && v !== '' && String(v).toLowerCase().includes(lq)){
        _sirMatches.push({ rowIdx: rowIdx, col: c });
      }
    });
  });
  if(_sirMatches.length){
    _sirIdx = 0;
    document.getElementById('sir-count').textContent = '1 / ' + _sirMatches.length;
    sirScrollToMatch(0);
  } else {
    document.getElementById('sir-count').textContent = '0 results';
  }
}

function sirScrollToMatch(idx){
  if(idx < 0 || idx >= _sirMatches.length) return;
  var match = _sirMatches[idx];
  if(!match) return;
  // Scroll the table wrapper div (first child of rarea)
  var scroller = document.querySelector('#rarea > div');
  if(!scroller) return;
  // Find the actual TR row by row index
  var tbody = document.getElementById('vs-tbody');
  if(!tbody) return;
  var row = tbody.querySelectorAll('tr')[match.rowIdx];
  if(row) row.scrollIntoView({block:'center', behavior:'smooth'});
  // Highlight — clear old, add new
  document.querySelectorAll('#vs-tbody td.sir-hl, #vs-tbody td.sir-hl-cur').forEach(function(td){
    td.classList.remove('sir-hl','sir-hl-cur');
  });
  document.querySelectorAll('#vs-tbody td.null-cell.sir-hl, #vs-tbody td.null-cell.sir-hl-cur').forEach(function(td){
    td.classList.remove('sir-hl','sir-hl-cur');
  });
  // Add highlights to all matches
  _sirMatches.forEach(function(m, i){
    var tr = tbody.querySelectorAll('tr')[m.rowIdx];
    if(!tr) return;
    var colIdx = resultCols.indexOf(m.col);
    var td = tr.querySelectorAll('td')[colIdx+1];
    if(td) td.classList.add(i === idx ? 'sir-hl-cur' : 'sir-hl');
  });
}

function sirKeyNav(e){
  if(!_sirMatches.length) return;
  if(e.key === 'Enter' || e.key === 'F3'){
    e.preventDefault();
    _sirIdx = e.shiftKey ? (_sirIdx - 1 + _sirMatches.length) % _sirMatches.length : (_sirIdx + 1) % _sirMatches.length;
    document.getElementById('sir-count').textContent = (_sirIdx + 1) + ' / ' + _sirMatches.length;
    sirScrollToMatch(_sirIdx);
  }
}

  function sirNav(dir){
  if(!_sirMatches.length) return;
  _sirIdx = (_sirIdx + dir + _sirMatches.length) % _sirMatches.length;
  document.getElementById('sir-count').textContent = (_sirIdx + 1) + ' / ' + _sirMatches.length;
  sirScrollToMatch(_sirIdx);
}

/* ══════════ SIR STYLES (injected dynamically) ═════════════════ */
(function(){
  var s = document.createElement('style');
  s.textContent = 'td.sir-hl{background:#fffacd !important;color:#000 !important;}td.sir-hl-cur{background:#FFFF00 !important;color:#000 !important;outline:2px solid #ff8c00 !important;outline-offset:-2px !important;font-weight:bold !important;}';
  document.head.appendChild(s);
})();

/* ══════════ HELPERS ═══════════════════════════════════════════ */
function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}
function escJ(s){return String(s).replace(/\\/g,'\\\\').replace(/'/g,"\\'").replace(/\r/g,'').replace(/\n/g,'');}

/* ══════════ AUTOCOMPLETE ENGINE ═══════════════════════════════ */
(function(){
  var dropdown, selIdx = -1, acItems = [], acWord = '', acWordStart = 0;
  var KW_LIST = [
    'SELECT','FROM','WHERE','AND','OR','NOT','IN','EXISTS','BETWEEN','LIKE','IS','NULL',
    'JOIN','LEFT JOIN','RIGHT JOIN','INNER JOIN','OUTER JOIN','FULL JOIN','CROSS JOIN',
    'ON','GROUP BY','ORDER BY','HAVING','UNION','UNION ALL','DISTINCT','AS','LIMIT',
    'OFFSET','INSERT INTO','VALUES','UPDATE','SET','DELETE FROM','CREATE TABLE',
    'DROP TABLE','ALTER TABLE','ADD COLUMN','WITH','CASE','WHEN','THEN','ELSE','END',
    'ROWNUM','FETCH NEXT','ROWS ONLY','CONNECT BY','PRIOR','LEVEL','PIVOT','MERGE',
    'USING','MATCHED','ASC','DESC','COMMIT','ROLLBACK','TRUNCATE','DUAL','SYSDATE',
    'SYSTIMESTAMP','BEGIN','DECLARE','RETURN'
  ];
  var FN_LIST = [
    'COUNT(','SUM(','AVG(','MAX(','MIN(','COALESCE(','NVL(','NVL2(','DECODE(',
    'TRIM(','LTRIM(','RTRIM(','UPPER(','LOWER(','SUBSTR(','INSTR(','LENGTH(',
    'TO_DATE(','TO_CHAR(','TO_NUMBER(','TO_TIMESTAMP(','TRUNC(','ROUND(','FLOOR(',
    'CEIL(','MOD(','ABS(','SIGN(','POWER(','SQRT(','CONCAT(','REPLACE(','TRANSLATE(',
    'LPAD(','RPAD(','RANK(','DENSE_RANK(','ROW_NUMBER(','LEAD(','LAG(','OVER(',
    'PARTITION BY','LISTAGG(','EXTRACT(','MONTHS_BETWEEN(','ADD_MONTHS(','LAST_DAY(',
    'NEXT_DAY(','CAST(','NULLIF(','GREATEST(','LEAST(','REGEXP_LIKE(','REGEXP_SUBSTR(',
    'REGEXP_REPLACE(','SYS_GUID(','WM_CONCAT(',
    'XMLAGG(','XMLELEMENT(','XMLFOREST(','JSON_VALUE(','JSON_QUERY(',
    'STANDARD_HASH(','ORA_HASH(','RATIO_TO_REPORT(','PERCENT_RANK(',
    'CUME_DIST(','NTILE(','FIRST_VALUE(','LAST_VALUE(','NTH_VALUE('
  ];
  function init(){
    dropdown = document.createElement('div');
    dropdown.id = 'ac-dropdown';
    document.body.appendChild(dropdown);
    var ta = document.getElementById('sqled');
    ta.addEventListener('input', onInput);
    ta.addEventListener('keydown', onKeyDown, true);
    ta.addEventListener('blur', function(){ setTimeout(hide, 150); });
    document.addEventListener('mousedown', function(e){ if(e.target.closest('#ac-dropdown')) return; hide(); });
  }
  function getWordAtCursor(ta){
    var pos = ta.selectionStart, val = ta.value;
    var start = pos;
    while(start > 0 && /[\w$.]/.test(val[start-1])) start--;
    return { word: val.slice(start, pos), start: start, end: pos };
  }
  function getContext(val, cursorPos){
    var before = val.slice(0, cursorPos).toUpperCase().replace(/\s+/g,' ');
    var ctxKws = ['SELECT','FROM','WHERE','SET','ON','HAVING','GROUP BY','ORDER BY','JOIN','LEFT JOIN','RIGHT JOIN','INNER JOIN','OUTER JOIN','VALUES','INTO'];
    var lastKw = '', lastIdx = -1;
    ctxKws.forEach(function(k){ var idx = before.lastIndexOf(k); if(idx > lastIdx){ lastIdx = idx; lastKw = k; } });
    var tables = [];
    var tblRe = /(?:FROM|JOIN)\s+([A-Z0-9_$#]+)/gi;
    var m;
    while((m = tblRe.exec(val)) !== null){ tables.push(m[1].toUpperCase()); }
    return { lastKw: lastKw, tables: tables };
  }
  function getSuggestions(word, val, cursorPos){
    if(word.length < 2) return [];
    var wu = word.toUpperCase();
    var ctx = getContext(val, cursorPos);
    var results = [];
    if(['FROM','JOIN','LEFT JOIN','RIGHT JOIN','INNER JOIN','OUTER JOIN'].indexOf(ctx.lastKw) > -1){
      Object.keys(acMeta).forEach(function(t){ if(t.toUpperCase().startsWith(wu)) results.push({label:t, type:'tbl'}); });
    }
    var colCtx = ['SELECT','WHERE','SET','ON','HAVING','GROUP BY','ORDER BY'];
    if(colCtx.indexOf(ctx.lastKw) > -1){
      var colsSeen = new Set();
      ctx.tables.forEach(function(t){
        var cols = acMeta[t] || acMeta[t.toUpperCase()] || [];
        cols.forEach(function(c){ if(c.toUpperCase().startsWith(wu) && !colsSeen.has(c)){ colsSeen.add(c); results.push({label:c, type:'col', table:t}); } });
      });
      if(!ctx.tables.length){
        var allSeen = new Set();
        Object.keys(acMeta).forEach(function(t){
          (acMeta[t]||[]).forEach(function(c){ if(c.toUpperCase().startsWith(wu) && !allSeen.has(c)){ allSeen.add(c); results.push({label:c, type:'col', table:t}); } });
        });
      }
    }
    FN_LIST.forEach(function(f){ if(f.toUpperCase().startsWith(wu)) results.push({label:f, type:'fn'}); });
    KW_LIST.forEach(function(k){ if(k.toUpperCase().startsWith(wu)) results.push({label:k, type:'kw'}); });
    var seen = new Set(), final = [];
    results.forEach(function(r){ var key = r.type+':'+r.label; if(!seen.has(key)){ seen.add(key); final.push(r); } });
    return final.slice(0, 30);
  }
  function getCaretCoords(ta){
    var div = document.createElement('div');
    var style = window.getComputedStyle(ta);
    ['fontFamily','fontSize','fontWeight','lineHeight','letterSpacing','padding','paddingTop','paddingLeft','paddingRight','paddingBottom','border','borderTop','borderLeft','wordWrap','whiteSpace','width'].forEach(function(p){ div.style[p] = style[p]; });
    div.style.position = 'absolute';div.style.visibility = 'hidden';div.style.whiteSpace = 'pre-wrap';
    div.style.wordBreak = 'break-word';div.style.top = '-9999px';div.style.left = '-9999px';
    document.body.appendChild(div);
    var before = ta.value.slice(0, ta.selectionStart);
    div.textContent = before;
    var span = document.createElement('span');span.textContent = '|';div.appendChild(span);
    var taRect = ta.getBoundingClientRect();
    var spanRect = span.getBoundingClientRect();
    var divRect = div.getBoundingClientRect();
    document.body.removeChild(div);
    var x = taRect.left + (spanRect.left - divRect.left) - ta.scrollLeft;
    var y = taRect.top  + (spanRect.top  - divRect.top)  - ta.scrollTop;
    return { x: x, y: y };
  }
  function show(items, ta){
    if(!items.length){ hide(); return; }
    acItems = items; selIdx = -1;
    var html = items.map(function(item, i){
      var badge = '<span class="ac-badge '+item.type+'">'+(item.type==='tbl'?'TABLE':item.type==='col'?'COL':item.type==='fn'?'FN':'KW')+'</span>';
      var label = esc(item.label);
      if(acWord){
        var re = new RegExp('^('+acWord.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+')','i');
        label = label.replace(re,'<span class="ac-match">$1</span>');
      }
      return '<div class="ac-item" data-idx="'+i+'" onmousedown="event.preventDefault()" onclick="_acPick('+i+')">'+badge+' '+label+'</div>';
    }).join('');
    dropdown.innerHTML = html;
    dropdown.style.display = 'block';
    var coords = getCaretCoords(ta);
    var lineH = parseInt(window.getComputedStyle(ta).lineHeight) || 20;
    var top = coords.y + lineH + 4;
    var left = coords.x;
    if(left + 240 > window.innerWidth) left = window.innerWidth - 244;
    if(top + 220 > window.innerHeight) top = coords.y - 224;
    dropdown.style.top  = top + 'px';
    dropdown.style.left = left + 'px';
  }
  function hide(){ dropdown.style.display='none'; selIdx=-1; acItems=[]; }
  function highlight(idx){
    dropdown.querySelectorAll('.ac-item').forEach(function(el,i){ el.classList.toggle('ac-sel', i===idx); });
    var sel = dropdown.querySelectorAll('.ac-item')[idx];
    if(sel) sel.scrollIntoView({block:'nearest'});
  }
  window._acPick = function(idx){
    var item = acItems[idx];
    if(!item) return;
    var ta = document.getElementById('sqled');
    var val = ta.value;
    var insert = item.label;
    ta.value = val.slice(0, acWordStart) + insert + val.slice(acWordStart + acWord.length);
    var newPos = acWordStart + insert.length;
    ta.selectionStart = ta.selectionEnd = newPos;
    ta.focus();
    hide();
    doHL(); doLN();
  };
  function onInput(){
    var ta = document.getElementById('sqled');
    var w = getWordAtCursor(ta);
    acWord = w.word; acWordStart = w.start;
    if(!acWord){ hide(); return; }
    var items = getSuggestions(acWord, ta.value, ta.selectionStart);
    show(items, ta);
  }
  function onKeyDown(e){
    if(dropdown.style.display === 'none') return;
    if(e.key === 'ArrowDown'){e.preventDefault();e.stopPropagation();selIdx=(selIdx+1)%acItems.length;highlight(selIdx);return;}
    if(e.key === 'ArrowUp'){e.preventDefault();e.stopPropagation();selIdx=(selIdx-1+acItems.length)%acItems.length;highlight(selIdx);return;}
    if(e.key === 'Enter' || e.key === 'Tab'){if(selIdx>-1){e.preventDefault();e.stopPropagation();_acPick(selIdx);return;}hide();return;}
    if(e.key === 'Escape'){e.preventDefault();hide();return;}
  }
  if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();

/* ══════════ LOAD METADATA ═════════════════════════════════════ */
var acMeta = {}, acMetaLoaded = false;

function loadMetadata(){
  if(!activeConn){ alert('Select a connection first.'); return; }
  setMetaStatus('loading', '⏳ Loading metadata…');
  var sql = "SELECT TABLE_NAME, COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE OWNER = SYS_CONTEXT('USERENV','CURRENT_SCHEMA') ORDER BY TABLE_NAME, COLUMN_ID";
  var auth = basicAuth(activeConn), conn = activeConn;
  soapCall(conn.url+'/xmlpserver/services/ExternalReportWSSService', buildRunReportSOAP(conn, sql), auth)
    .then(function(resp){ return resp.text(); })
    .then(function(xml){
      var p = parseCSVResponse(xml);
      if(p.error || !p.rows.length){ setMetaStatus('failed', '⚠ Metadata: no data — click ⚡ Meta to retry'); return; }
      acMeta = {};
      p.rows.forEach(function(r){
        var t = (r['TABLE_NAME']||r['table_name']||'').trim();
        var c = (r['COLUMN_NAME']||r['column_name']||'').trim();
        if(t && c){ if(!acMeta[t]) acMeta[t] = []; acMeta[t].push(c); }
      });
      acMetaLoaded = true;
      var tCount = Object.keys(acMeta).length;
      setMetaStatus('ok', '⚡ Metadata: '+tCount+' tables loaded');
    })
    .catch(function(){ setMetaStatus('failed', '⚠ Metadata failed — click ⚡ Meta to retry'); });
}
function setMetaStatus(state, msg){
  var el = document.getElementById('meta-status');
  if(!el) return;
  el.textContent = msg;
  el.style.color   = state==='ok' ? '#4ade80' : state==='failed' ? '#f87171' : '#fbbf24';
  el.style.display = 'inline';
}

/* ── Toggle catalog visibility ───────────────────────────────── */
var catalogVisible=false;
function toggleCatalog(){
  catalogVisible=!catalogVisible;
  var cat=document.getElementById('sidebar-catalog');
  var resizer=document.getElementById('sidebar-resizer');
  var sidebar=document.getElementById('sidebar');
  if(catalogVisible){
    cat.style.display='flex';
    resizer.style.display='block';
    sidebar.style.width='200px';
  }else{
    cat.style.display='none';
    resizer.style.display='none';
    sidebar.style.width='36px';
  }
}
function initSidebarCollapsed(){
  var cat=document.getElementById('sidebar-catalog');
  var resizer=document.getElementById('sidebar-resizer');
  var sidebar=document.getElementById('sidebar');
  cat.style.display='none';
  resizer.style.display='none';
  sidebar.style.width='36px';
}

/* ══════════ CATALOG BROWSER ═══════════════════════════════════ */
function setCatalogStatus(type, msg){
  var el=document.getElementById('catalog-status');
  if(!el)return;
  el.className='catalog-status '+(type||'');
  el.textContent=msg||'';
}

function refreshCatalog(){ loadCatalogTree(); }

function loadCatalogTree(){
  var wrap=document.getElementById('catalog-tree-wrap');
  if(!activeConn){
    wrap.innerHTML='<div class="catalog-empty-msg">Select a connection to browse catalog</div>';
    setCatalogStatus('','No connection');
    return;
  }
  wrap.innerHTML='<div class="catalog-empty-msg">Loading catalog…</div>';
  setCatalogStatus('loading','Loading…');
  catLoadFolder('/',wrap,0);
}

async function catLoadFolder(path, container, depth){
  try{
    var resp=await fetch(PROXY_URL+'/catalog/folders',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({fusionUrl:activeConn.url,username:activeConn.user,password:activeConn.pass,path:path})
    });
    var data=await resp.json();

    if(depth===0) container.innerHTML='';

    if(!data.ok){
      container.innerHTML='<div class="catalog-empty-msg" style="color:#f87171;">⚠ '+(data.message||'Failed to load')+'</div>';
      setCatalogStatus('error','Error');
      return;
    }
    if(!data.items||data.items.length===0){
      var empty=document.createElement('div');
      empty.className='cat-row loading-row';
      empty.style.setProperty('--depth',depth);
      empty.innerHTML='<span class="cat-label" style="color:#475569;font-style:italic;">Empty folder</span>';
      container.appendChild(empty);
      if(depth===0) setCatalogStatus('connected','Connected');
      return;
    }

    // Sort folders first then xdm
    data.items.sort(function(a,b){
      if(a.type===b.type)return a.name.localeCompare(b.name);
      return a.type==='folder'?-1:1;
    });

    data.items.forEach(function(item){
      var node=catBuildNode(item,depth);
      container.appendChild(node);
    });

    if(depth===0) setCatalogStatus('connected','Connected');

  }catch(e){
    container.innerHTML='<div class="catalog-empty-msg" style="color:#f87171;">⚠ '+e.message+'</div>';
    setCatalogStatus('error','Failed');
  }
}

function catBuildNode(item, depth){
  var node=document.createElement('div');
  node.className='cat-node';

  var row=document.createElement('div');
  row.className='cat-row';
  row.style.setProperty('--depth',depth);

  if(item.type==='folder'){
    var toggle=document.createElement('span');
    toggle.className='cat-toggle';
    toggle.innerHTML='<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round"><polyline points="9 18 15 12 9 6"/></svg>';

    var icon=document.createElement('span');
    icon.className='cat-icon icon-folder';
    icon.innerHTML='<svg viewBox="0 0 24 24" fill="currentColor" stroke="none"><path d="M10 4H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2h-8l-2-2z" opacity="0.85"/></svg>';

    var label=document.createElement('span');
    label.className='cat-label';
    label.textContent=item.name;

    row.appendChild(toggle);
    row.appendChild(icon);
    row.appendChild(label);

    var children=document.createElement('div');
    children.className='cat-children';

    var loaded=false;
    row.addEventListener('click',async function(){
      var isOpen=children.classList.contains('open');
      if(isOpen){
        children.classList.remove('open');
        toggle.classList.remove('open');
      }else{
        children.classList.add('open');
        toggle.classList.add('open');
        if(!loaded){
          loaded=true;
          var spinRow=document.createElement('div');
          spinRow.className='cat-row loading-row';
          spinRow.style.setProperty('--depth',depth+1);
          spinRow.innerHTML='<span class="cat-label">Loading…</span>';
          children.appendChild(spinRow);
          await catLoadFolder(item.path,children,depth+1);
          if(spinRow.parentNode)spinRow.parentNode.removeChild(spinRow);
        }
      }
    });

    node.appendChild(row);
    node.appendChild(children);

  }else if(item.type==='xdm'){
    var toggle=document.createElement('span');
    toggle.className='cat-toggle leaf';
    toggle.innerHTML='<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3"><polyline points="9 18 15 12 9 6"/></svg>';

    var icon=document.createElement('span');
    icon.className='cat-icon icon-xdm';
    icon.innerHTML='<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>';

    var label=document.createElement('span');
    label.className='cat-label';
    // Show report name without extension
    label.textContent=item.name.replace(/\.xdm$/i,'');
    label.title=item.path;

    row.appendChild(toggle);
    row.appendChild(icon);
    row.appendChild(label);

    row.addEventListener('click',function(){ catOpenXdm(item,row); });
    node.appendChild(row);
  }

  return node;
}

async function catOpenXdm(item, rowEl){
  // Mark active in tree
  if(activeCatNode)activeCatNode.classList.remove('active');
  activeCatNode=rowEl;
  rowEl.classList.add('active');

  setCatalogStatus('loading','Fetching…');

  try{
    var resp=await fetch(PROXY_URL+'/catalog/xdm',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({fusionUrl:activeConn.url,username:activeConn.user,password:activeConn.pass,path:item.path})
    });
    var data=await resp.json();

    if(!data.ok||!data.sql){
      setCatalogStatus('error','No SQL found');
      showErr(data.message||'No SQL found in this data model');
      return;
    }

    // Get report name without extension
    var reportName=item.name.replace(/\.xdm$/i,'');

    // Add new READONLY tab
    tabCounter++;
    tabs.push({
      id:tabCounter,
      name:reportName,
      sql:data.sql,
      results:null,
      cols:[],
      elapsed:null,
      readonly:true  // ← readonly flag
    });
    renderTabs();
    activateTab(tabs.length-1);
    setCatalogStatus('connected','Connected');

  }catch(e){
    setCatalogStatus('error','Failed');
    showErr('Catalog error: '+e.message);
  }
}

/* ── Sidebar resizer ─────────────────────────────────────────── */
function initCatalogResizer(){
  var resizer=document.getElementById('sidebar-resizer');
  var sidebar=document.getElementById('sidebar');
  var dragging=false, startX, startW;
  if(!resizer||!sidebar)return;
  resizer.addEventListener('mousedown',function(e){
    dragging=true;startX=e.clientX;startW=sidebar.offsetWidth;
    document.body.style.userSelect='none';document.body.style.cursor='col-resize';
    e.preventDefault();
  });
  document.addEventListener('mousemove',function(e){
    if(!dragging)return;
    var nw=startW+(e.clientX-startX);
    sidebar.style.width=Math.max(120,Math.min(380,nw))+'px';
  });
  document.addEventListener('mouseup',function(){
    if(!dragging)return;
    dragging=false;document.body.style.userSelect='';document.body.style.cursor='';
  });
}
