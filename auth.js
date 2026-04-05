// ── AUTH ROUTES ───────────────────────────────────────────────
// Handles: /ping, /user-count, /register, /login

module.exports = function handleAuth(req, res, pool, log, getBody) {

  // ── PING ───────────────────────────────────────────────────────
  if (req.url === '/ping' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return true;
  }

  // ── USER COUNT ─────────────────────────────────────────────────
  if (req.url === '/user-count' && req.method === 'GET') {
    pool.query('SELECT COUNT(*) FROM users')
      .then(result => {
        const count = parseInt(result.rows[0].count, 10);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ count }));
      })
      .catch(err => {
        console.error(err);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ message: 'DB error' }));
      });
    return true;
  }

  // ── REGISTER ──────────────────────────────────────────────────
  if (req.url === '/register' && req.method === 'POST') {
    getBody(req, async data => {
      const { name, username, email, password } = data;

      if (!name || !username || !email || !password) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ message: 'Missing fields' }));
      }

      try {
        const check = await pool.query(
          'SELECT * FROM users WHERE username=$1 OR email=$2',
          [username, email]
        );

        if (check.rows.length > 0) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          return res.end(JSON.stringify({ message: 'User exists' }));
        }

        await pool.query(
          'INSERT INTO users(name, username, email, password) VALUES($1,$2,$3,$4)',
          [name, username, email, password]
        );

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ message: 'Registered' }));
        log('OK', 'New user registered: ' + username);

      } catch (err) {
        console.error(err);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ message: 'DB error' }));
      }
    });
    return true;
  }

  // ── LOGIN ─────────────────────────────────────────────────────
  if (req.url === '/login' && req.method === 'POST') {
    getBody(req, async data => {
      const { identifier, password } = data;

      if (!identifier || !password) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ message: 'Missing fields' }));
      }

      try {
        const result = await pool.query(
          'SELECT * FROM users WHERE (username=$1 OR email=$1) AND password=$2',
          [identifier, password]
        );

        if (result.rows.length === 0) {
          res.writeHead(401, { 'Content-Type': 'application/json' });
          return res.end(JSON.stringify({ message: 'Invalid credentials' }));
        }

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          message: 'Login success',
          user: result.rows[0]
        }));
        log('OK', 'Login: ' + identifier);

      } catch (err) {
        console.error(err);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ message: 'DB error' }));
      }
    });
    return true;
  }

  // Route not handled here
  return false;
};
