// ── Storage Helpers ──────────────────────────────────────────
const USERS_KEY = 'qfdz_users';

function getUsers() {
  try { return JSON.parse(localStorage.getItem(USERS_KEY) || '[]'); }
  catch { return []; }
}

function saveUser(user) {
  const users = getUsers();
  users.push(user);
  localStorage.setItem(USERS_KEY, JSON.stringify(users));
}

function findUser(identifier) {
  return getUsers().find(u =>
    u.username.toLowerCase() === identifier.toLowerCase() ||
    u.email.toLowerCase() === identifier.toLowerCase()
  );
}

function updateUserCount() {
  fetch(SERVER + '/user-count')
    .then(res => res.ok ? res.json() : null)
    .then(data => {
      if (data && typeof data.count === 'number') {
        const count = data.count;
        document.getElementById('user-count-label').textContent =
          `⬡ ${count} user${count !== 1 ? 's' : ''} registered`;
      }
    })
    .catch(() => {
      document.getElementById('user-count-label').textContent = '⬡ — users registered';
    });
}

// ── Tab Switching ────────────────────────────────────────────
function switchTab(tab, btn, title, file) {
  document.querySelectorAll('.form').forEach(f => f.classList.remove('active'));
  document.getElementById('form-' + tab).classList.add('active');
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  if (file) document.getElementById('titlebar-label').textContent = file;
  // Reset forgot form
  if (tab !== 'forgot') {
    document.getElementById('forgot-fields').style.display = 'block';
    document.getElementById('forgot-success').style.display = 'none';
  }
  clearAllErrors();
}

// ── Password Toggle ──────────────────────────────────────────
function togglePwd(id, btn) {
  const inp = document.getElementById(id);
  const show = inp.type === 'password';
  inp.type = show ? 'text' : 'password';
  btn.innerHTML = show
    ? `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/><line x1="1" y1="1" x2="23" y2="23"/></svg>`
    : `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>`;
}

// ── Password Strength ─────────────────────────────────────────
function checkStrength(val) {
  const segs = ['s1','s2','s3','s4'].map(id => document.getElementById(id));
  const lbl  = document.getElementById('strength-label');
  segs.forEach(s => s.className = 'strength-seg');
  if (!val) { lbl.textContent = '—'; lbl.style.color = 'var(--text3)'; return; }
  let score = 0;
  if (val.length >= 6)  score++;
  if (val.length >= 10) score++;
  if (/[A-Z]/.test(val) && /[0-9]/.test(val)) score++;
  if (/[^A-Za-z0-9]/.test(val)) score++;
  const cls  = score <= 1 ? 'weak' : score <= 2 ? 'medium' : 'strong';
  const txts = { weak:'weak', medium:'medium', strong:'strong ✓' };
  const clrs = { weak:'var(--error)', medium:'var(--orange)', strong:'var(--accent)' };
  for (let i = 0; i < Math.min(score+1,4); i++) segs[i].classList.add(cls);
  lbl.textContent = '// ' + txts[cls];
  lbl.style.color = clrs[cls];
}

// ── Error Helpers ─────────────────────────────────────────────
function showErr(id) { const el = document.getElementById(id); if(el) el.style.display = 'block'; }
function hideErr(id) { const el = document.getElementById(id); if(el) el.style.display = 'none'; }
function clearAllErrors() {
  document.querySelectorAll('.field-error').forEach(e => e.style.display = 'none');
}

// ── Toast ─────────────────────────────────────────────────────
let toastTimer;
function showToast(type, title, msg) {
  const t   = document.getElementById('toast');
  const ico = document.getElementById('toast-icon-svg');
  document.getElementById('toast-title').textContent = title;
  document.getElementById('toast-msg').textContent   = msg;
  t.className = 'toast ' + type;
  ico.innerHTML = type === 'success'
    ? `<polyline points="20 6 9 17 4 12"/>`
    : `<circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/>`;
  t.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.remove('show'), 3500);
}

// ── Shake ────────────────────────────────────────────────────
function shake() {
  const c = document.getElementById('main-card');
  c.style.animation = 'none'; void c.offsetHeight;
  c.style.animation = 'shake 0.4s ease';
  setTimeout(() => c.style.animation = '', 400);
}

// ── LOGIN ────────────────────────────────────────────────────
function handleLogin() {
  clearAllErrors();

  const u = document.getElementById('login-user').value.trim();
  const p = document.getElementById('login-pwd').value;

  let valid = true;

  if (!u) { showErr('err-login-user'); valid = false; }
  if (!p) { showErr('err-login-pwd');  valid = false; }

  if (!valid) { shake(); return; }

  // Prevent double-submit
  const btn = document.querySelector('#form-login .btn-primary');
  if (btn.disabled) return;
  btn.disabled = true;
  btn.textContent = '⏳ Signing in…';

  // API CALL
  fetch('https://queryforge-backend-jjxx.onrender.com/login', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      identifier: u,
      password: btoa(p)
    })
  })
  .then(res => {
    return res.json().then(data => ({ status: res.status, data }));
  })
  .then(({ status, data }) => {

    if (data.message !== 'Login success' || status < 200 || status >= 300) {
      showErr('err-login-fail');
      shake();
      btn.disabled = false;
      btn.textContent = '→ Sign In';
      return;
    }

    const user = data.user;

    // Save session
    const remember = document.getElementById('remember-me').checked;

    const sessionData = {
      id: user.id,
      username: user.username,
      name: user.name,
      email: user.email,
      loginTime: new Date().toISOString()
    };

    if (remember) {
      localStorage.setItem('qfdz_session', JSON.stringify(sessionData));
    } else {
      sessionStorage.setItem('qfdz_session', JSON.stringify(sessionData));
    }

    showToast('success', 'Welcome back, ' + user.name.split(' ')[0] + '!', 'Redirecting to your workspace...');

    setTimeout(() => {
      window.location.href = 'https://queryforge-datazen.vercel.app/';
    }, 1000);

  })
  .catch(err => {
    console.error('Login error:', err);
    showToast('error', 'Network error', 'Could not reach server. Check your connection and try again.');
    shake();
    btn.disabled = false;
    btn.textContent = '→ Sign In';
  });
}

// ── REGISTER ─────────────────────────────────────────────────
function handleRegister() {
  clearAllErrors();

  const name     = document.getElementById('reg-name').value.trim();
  const username = document.getElementById('reg-username').value.trim();
  const email    = document.getElementById('reg-email').value.trim();
  const pwd      = document.getElementById('reg-pwd').value;
  const confirm  = document.getElementById('reg-confirm').value;

  let valid = true;

  if (!name)    { showErr('err-reg-name'); valid = false; }
  if (!username){ showErr('err-reg-username'); valid = false; }
  if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) { showErr('err-reg-email'); valid = false; }
  if (!pwd || pwd.length < 6) { showErr('err-reg-pwd'); valid = false; }
  if (pwd !== confirm) { showErr('err-reg-confirm'); valid = false; }

  if (!valid) { shake(); return; }

  // Prevent double-submit
  const btn = document.querySelector('#form-register .btn-primary');
  if (btn.disabled) return;
  btn.disabled = true;
  btn.textContent = '⏳ Creating account…';

  // API CALL
  fetch('https://queryforge-backend-jjxx.onrender.com/register', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      name,
      username,
      email,
      password: btoa(pwd)
    })
  })
  .then(res => {
    return res.json().then(data => ({ status: res.status, data }));
  })
  .then(({ status, data }) => {

    // Handle known conflict errors
    if (status === 409 || data.message === 'User exists' || data.message === 'Username taken') {
      showErr('err-reg-username-taken');
      shake();
      return;
    }

    if (data.message === 'Email exists' || data.message === 'Email already registered') {
      showErr('err-reg-email-taken');
      shake();
      return;
    }

    // Any non-success status that isn't a known conflict
    if (status < 200 || status >= 300) {
      const msg = data.message || data.error || 'Registration failed. Please try again.';
      showToast('error', 'Registration failed', msg);
      shake();
      return;
    }

    // Success
    showToast('success', 'Account created!', 'Welcome to QueryForge DataZen, ' + name.split(' ')[0] + '!');

    // Clear form
    ['reg-name','reg-username','reg-email','reg-pwd','reg-confirm'].forEach(id => {
      document.getElementById(id).value = '';
    });
    checkStrength('');

    // Switch to login
    updateUserCount();
    setTimeout(() => {
      switchTab('login', document.querySelectorAll('.tab-btn')[0], 'Sign In', 'auth.session');
    }, 1500);

  })
  .catch(err => {
    console.error('Register error:', err);
    showToast('error', 'Network error', 'Could not reach server. Check your connection and try again.');
    shake();
  })
  .finally(() => {
    btn.disabled = false;
    btn.textContent = '→ Create Account';
  });
}

// ── FORGOT PASSWORD ───────────────────────────────────────────
function handleForgot() {
  clearAllErrors();

  const email = document.getElementById('forgot-email').value.trim();
  if (!email) { showErr('err-forgot'); shake(); return; }

  // Show success (no backend yet)
  document.getElementById('forgot-fields').style.display = 'none';
  document.getElementById('forgot-success').style.display = 'block';

  showToast('success', 'Reset link sent', 'Check inbox for ' + email);
}

// ── Clock ─────────────────────────────────────────────────────
function updateClock() {
  const now = new Date();
  document.getElementById('status-time').textContent =
    now.toTimeString().slice(0,8);
}
setInterval(updateClock, 1000);
updateClock();

// ── Server Wake-up & Status ───────────────────────────────────
const SERVER = 'https://queryforge-backend-jjxx.onrender.com';
const dot    = document.getElementById('server-dot');
const slabel = document.getElementById('server-status-label');

function setServerStatus(state) {
  dot.className = 'status-dot' + (state === 'waking' ? ' waking' : state === 'offline' ? ' offline' : '');
  if (state === 'online') {
    slabel.textContent = 'Server Active';
  } else if (state === 'waking') {
    slabel.textContent = 'Waking server…';
  } else {
    slabel.innerHTML = 'Server Offline &nbsp;<button onclick="wakeServer()" style="font-family:JetBrains Mono,monospace;font-size:10px;background:var(--bg3);border:1px solid var(--border2);color:var(--orange);padding:1px 7px;border-radius:4px;cursor:pointer;">↺ Retry</button>';
  }
}

// Disable submit buttons until server is confirmed reachable
function setFormsReady(ready) {
  document.querySelectorAll('.btn-primary').forEach(b => {
    b.disabled = !ready;
    if (!ready) b.style.opacity = '0.55';
    else        b.style.opacity = '';
  });
}

// Endpoints to probe — any 200/4xx means server is awake
const WAKE_ENDPOINTS = [
  { url: SERVER + '/ping',       method: 'GET'  },
  { url: SERVER + '/user-count', method: 'GET'  },
  { url: SERVER + '/login',      method: 'POST' }, // 400/422 = server is alive
];

async function probeServer() {
  for (const ep of WAKE_ENDPOINTS) {
    try {
      const res = await fetch(ep.url, {
        method: ep.method,
        headers: ep.method === 'POST' ? { 'Content-Type': 'application/json' } : {},
        body: ep.method === 'POST' ? JSON.stringify({}) : undefined,
        signal: AbortSignal.timeout(6000)
      });
      // Any HTTP response (even 400/404/422) means the server is UP
      if (res.status > 0) return true;
    } catch (_) { /* network error — server still asleep */ }
  }
  return false;
}

async function wakeServer() {
  setServerStatus('waking');
  setFormsReady(false);

  const MAX_ATTEMPTS = 10;
  const DELAY_MS     = 3000; // 3s between retries — Render cold start is ~15-25s

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    slabel.textContent = `Waking server… (${attempt}/${MAX_ATTEMPTS})`;

    const alive = await probeServer();
    if (alive) {
      setServerStatus('online');
      setFormsReady(true);
      updateUserCount();
      return;
    }

    if (attempt < MAX_ATTEMPTS) {
      await new Promise(r => setTimeout(r, DELAY_MS));
    }
  }

  // All retries exhausted
  setServerStatus('offline');
  setFormsReady(true);
}

wakeServer();

// ── Init ─────────────────────────────────────────────────────
// Auto-fill from session if exists
const session = JSON.parse(localStorage.getItem('qfdz_session') || 'null');
if (session) {
  document.getElementById('login-user').value = session.username || '';
}
