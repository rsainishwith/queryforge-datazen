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
      // Check username first
      const usernameCheck = await pool.query(
        'SELECT * FROM users WHERE username=$1',
        [username]
      );
      if (usernameCheck.rows.length > 0) {
        res.writeHead(409, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ message: 'Username already taken' }));
      }

      // Then check email
      const emailCheck = await pool.query(
        'SELECT * FROM users WHERE email=$1',
        [email]
      );
      if (emailCheck.rows.length > 0) {
        res.writeHead(409, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ message: 'Email already registered' }));
      }

      // Insert new user
      await pool.query(
        'INSERT INTO users(name, username, email, password) VALUES($1,$2,$3,$4)',
        [name, username, email, password]
      );

      res.writeHead(201, { 'Content-Type': 'application/json' });
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

// ── FORGOT PASSWORD ────────────────────────────────────────
if (req.url === '/forgot-password' && req.method === 'POST') {
  getBody(req, async data => {
    const { email } = data;

    if (!email) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ message: 'Email is required' }));
    }

    try {
      // Check if user exists
      const userResult = await pool.query(
        'SELECT id FROM users WHERE email=$1',
        [email]
      );

      if (userResult.rows.length === 0) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ message: 'If email exists, reset link will be sent' }));
      }

      const userId = userResult.rows[0].id;

      // Generate random token
      const token = require('crypto').randomBytes(32).toString('hex');
      const expiresAt = new Date(Date.now() + 15 * 60 * 1000);

      // Save token to database
      await pool.query(
        'INSERT INTO reset_tokens(user_id, token, email, expires_at) VALUES($1,$2,$3,$4)',
        [userId, token, email, expiresAt]
      );

      // Send email
      const nodemailer = require('nodemailer');
      const transporter = nodemailer.createTransport({
        service: 'gmail',
        auth: {
          user: process.env.EMAIL_USER,
          pass: process.env.EMAIL_PASS
        }
      });

      const resetLink = `${process.env.FRONTEND_URL}reset-password.html?token=${token}`;

      await transporter.sendMail({
        from: process.env.EMAIL_USER,
        to: email,
        subject: 'QueryForge DataZen - Password Reset',
        html: `
          <h2>Password Reset Request</h2>
          <p>Click the link below to reset your password. This link expires in 15 minutes.</p>
          <a href="${resetLink}" style="background:green;color:white;padding:10px 20px;text-decoration:none;border-radius:5px;display:inline-block;">Reset Password</a>
          <p>Or copy this link: ${resetLink}</p>
          <p>If you didn't request this, ignore this email.</p>
        `
      });

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ message: 'Reset link sent to email' }));
      log('OK', 'Password reset email sent to: ' + email);

    } catch (err) {
      console.error(err);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ message: 'Failed to send reset email' }));
    }
  });
  return true;
}

// ── RESET PASSWORD ─────────────────────────────────────────
if (req.url === '/reset-password' && req.method === 'POST') {
  getBody(req, async data => {
    const { token, newPassword } = data;

    if (!token || !newPassword) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ message: 'Token and password required' }));
    }

    try {
      const tokenResult = await pool.query(
        'SELECT user_id, expires_at FROM reset_tokens WHERE token=$1',
        [token]
      );

      if (tokenResult.rows.length === 0) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ message: 'Invalid token' }));
      }

      const { user_id, expires_at } = tokenResult.rows[0];

      if (new Date() > new Date(expires_at)) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ message: 'Token expired' }));
      }

      await pool.query(
        'UPDATE users SET password=$1 WHERE id=$2',
        [newPassword, user_id]
      );

      await pool.query(
        'DELETE FROM reset_tokens WHERE token=$1',
        [token]
      );

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ message: 'Password reset successful' }));
      log('OK', 'Password reset for user: ' + user_id);

    } catch (err) {
      console.error(err);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ message: 'Failed to reset password' }));
    }
  });
  return true;
}

// Route not handled here
return false;
};
