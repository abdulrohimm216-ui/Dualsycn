// DUALSYNC signaling server (terpisah dari index.html).
// Jalankan: npm i ws && node server.js
// Deploy ke host yang menyediakan HTTPS/WSS (Render, Railway, Fly.io, dsb.)
// lalu isi alamatnya (wss://...) di Pengaturan server pada aplikasi.
const { WebSocketServer } = require('ws');
const http = require('http'), fs = require('fs'), path = require('path');
// Server HTTP juga menyajikan index.html, jadi satu alamat HTTPS cukup untuk semuanya
const srv = http.createServer((req, res) => {
  fs.readFile(path.join(__dirname, 'index.html'), (e, d) => {
    if (e) { res.writeHead(404); return res.end('index.html tidak ditemukan'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); res.end(d);
  });
});
const wss = new WebSocketServer({ server: srv });
srv.listen(process.env.PORT || 8080);

const rooms = new Map();                       // kode -> [ws, ws]
const ABC = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // tanpa huruf/angka yang mirip
const gen = () => { let c; do { c = Array.from({length:6}, () => ABC[Math.random()*ABC.length|0]).join(''); } while (rooms.has(c)); return c; };
const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const other = ws => (rooms.get(ws.code) || []).find(w => w !== ws);

function leave(ws) {
  const code = ws.code; if (!code) return;
  ws.code = null;
  const left = (rooms.get(code) || []).filter(w => w !== ws);
  rooms.set(code, left);
  left.forEach(w => send(w, { type: 'peer-left' }));
  // Room kosong dihapus setelah 60 detik agar reconnect masih bisa masuk
  if (!left.length) setTimeout(() => { if (!(rooms.get(code) || []).length) rooms.delete(code); }, 60000);
}

wss.on('connection', ws => {
  ws.alive = true;
  ws.on('pong', () => ws.alive = true);
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.type === 'create') {
      const c = gen(); rooms.set(c, [ws]); ws.code = c; ws.name = String(m.name || 'Aku').slice(0, 20);
      send(ws, { type: 'created', code: c });
    } else if (m.type === 'join') {
      const code = String(m.code || '').toUpperCase();
      if (!rooms.has(code)) return send(ws, { type: 'error', message: 'Room tidak ditemukan atau sudah berakhir.' });
      const live = rooms.get(code).filter(w => w.readyState === 1 && w !== ws); // buang socket mati
      if (live.length >= 2) return send(ws, { type: 'error', message: 'Room sudah penuh.' });
      ws.code = code; ws.name = String(m.name || 'Aku').slice(0, 20);
      rooms.set(code, [...live, ws]);
      send(ws, { type: 'joined', code, peer: live[0] ? live[0].name : null });
      live.forEach(w => send(w, { type: 'peer-joined', name: ws.name }));
    } else if (m.type === 'signal' || m.type === 'msg') {
      send(other(ws), { type: m.type, data: m.data });   // 'msg' disiapkan untuk sinkronisasi musik
    } else if (m.type === 'time') {
      send(ws, { type: 'time', t: Date.now() });          // jam server untuk sinkronisasi musik
    } else if (m.type === 'leave') leave(ws);
  });
  ws.on('close', () => leave(ws));
});

// Heartbeat: putus socket yang sudah mati, sekaligus menjaga koneksi lewat proxy
setInterval(() => wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; ws.ping(); }), 25000);
console.log('DUALSYNC signaling siap');
