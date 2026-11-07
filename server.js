const express = require('express'), http = require('http'), path = require('path'), crypto = require('crypto');
const { WebSocketServer } = require('ws'), { Pool } = require('pg');
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: process.env.PGSSL ? { rejectUnauthorized: false } : false });
const DEF = { speed: 1, jump: 1, layup: 1, vert: 1, stamina: 1 };
const hash = (p, s) => crypto.scryptSync(p, s, 32);
const sessions = new Map(); // token -> username
const online = new Map();   // username -> { ws, x, z, f, prof, lastResult }
const pub = r => ({ name: r.name, coins: r.coins, xp: r.xp, stats: r.stats });

pool.query('create table if not exists users(name text primary key, salt text, hash text, coins int default 0, xp int default 0, stats jsonb)').catch(e => console.error('DB setup failed', e));

app.post('/api/auth', async (req, res) => {
  try {
    const { u, p, reg } = req.body || {};
    if (!/^[A-Za-z0-9_]{3,16}$/.test(u || '') || typeof p !== 'string' || p.length < 6)
      return res.status(400).json({ err: 'Username: 3-16 letters, numbers or _. Password: 6+ characters.' });
    let r = (await pool.query('select * from users where lower(name)=lower($1)', [u])).rows[0];
    if (reg) {
      if (r) return res.status(409).json({ err: 'That username is taken.' });
      const salt = crypto.randomBytes(16).toString('hex');
      r = (await pool.query('insert into users(name,salt,hash,coins,xp,stats) values($1,$2,$3,0,0,$4) returning *',
        [u, salt, hash(p, salt).toString('hex'), DEF])).rows[0];
    } else if (!r || !crypto.timingSafeEqual(hash(p, r.salt), Buffer.from(r.hash, 'hex'))) {
      return res.status(401).json({ err: 'Wrong username or password.' });
    }
    const token = crypto.randomBytes(24).toString('hex');
    sessions.set(token, r.name);
    res.json({ token, profile: pub(r) });
  } catch (e) { console.error(e); res.status(500).json({ err: 'Server error, try again.' }); }
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });
const send = (ws, o) => ws.readyState === 1 && ws.send(JSON.stringify(o));
const all = o => { for (const c of online.values()) send(c.ws, o); };
const save = async c => { const r = c.prof; await pool.query('update users set coins=$2,xp=$3,stats=$4 where name=$1', [r.name, r.coins, r.xp, r.stats]); };

wss.on('connection', async (ws, req) => {
  const name = sessions.get(new URL(req.url, 'http://x').searchParams.get('token'));
  if (!name) return ws.close(4001, 'auth');
  const old = online.get(name);
  if (old) { send(old.ws, { t: 'kicked' }); old.ws.close(); }
  const row = (await pool.query('select * from users where name=$1', [name])).rows[0];
  if (!row) return ws.close(4001, 'auth');
  const c = { ws, x: 0, z: 0, f: 0, prof: pub(row), lastResult: 0, lastChat: 0 };
  online.set(name, c);
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));
  send(ws, { t: 'profile', p: c.prof });
  ws.on('message', async raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'pos') { c.x = +m.x || 0; c.z = +m.z || 0; c.f = +m.f || 0; }
    else if (m.t === 'chat') {
      const now = Date.now(); if (now - c.lastChat < 700) return; c.lastChat = now;
      const text = String(m.m || '').slice(0, 200).trim(); if (text) all({ t: 'chat', n: name, m: text });
    } else if (m.t === 'result') {
      const now = Date.now(); if (now - c.lastResult < 20000) return; c.lastResult = now;
      c.prof.coins += m.win ? 80 : 30; c.prof.xp += m.win ? 260 : 100;
      await save(c); send(ws, { t: 'profile', p: c.prof });
    } else if (m.t === 'upgrade' && DEF[m.stat] !== undefined) {
      const lv = c.prof.stats[m.stat] || 1, cost = 100 * lv;
      if (c.prof.coins >= cost) { c.prof.coins -= cost; c.prof.stats[m.stat] = lv + 1; await save(c); }
      send(ws, { t: 'profile', p: c.prof });
    }
  });
  ws.on('close', () => { if (online.get(name) === c) online.delete(name); });
});

setInterval(() => {
  const p = {}; for (const [n, c] of online) p[n] = [c.x, c.z, c.f];
  all({ t: 'players', p });
}, 100);
setInterval(() => wss.clients.forEach(w => { if (!w.isAlive) return w.terminate(); w.isAlive = false; w.ping(); }), 30000);

server.listen(process.env.PORT || 3000, () => console.log('Hoopers running'));
