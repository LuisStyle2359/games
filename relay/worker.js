// A small MQTT 3.1.1 broker (over WebSocket) for the chat, as a second server next to the public ones.
// It only implements what the page uses: QoS 0, retained messages, last will, '+'/'#' wildcards.
// Messages are end-to-end encrypted by the page, so this server only ever sees ciphertext.

const PREFIX = 'dv-2563318804-chat/';
const MAX_PAYLOAD = 256 * 1024;
const MAX_RETAINED = 16 * 1024;
const MAX_SUBS = 32;
const enc = new TextEncoder(), dec = new TextDecoder();

export default {
  async fetch(req, env) {
    if (req.headers.get('Upgrade') !== 'websocket') return new Response('ok\n', {headers: {'content-type': 'text/plain'}});
    const game = new URL(req.url).pathname.match(/^\/game\/([A-Za-z0-9]{8,48})$/);
    // Pinned to western Europe, where everyone plays; otherwise an object lives wherever it was first used.
    if (game) return env.ARENA.get(env.ARENA.idFromName(game[1]), {locationHint: 'weur'}).fetch(req);
    const room = new URL(req.url).pathname.match(/^\/(kick|tron)\/([A-Za-z0-9]{8,48})$/);
    if (room) { const ns = room[1] === 'kick' ? env.KICK : env.TRON; return ns.get(ns.idFromName(room[2]), {locationHint: 'weur'}).fetch(req); }
    return env.HUB.get(env.HUB.idFromName('eu'), {locationHint: 'weur'}).fetch(req);
  }
};

function varLen(n) {
  const out = [];
  do { let b = n % 128; n = Math.floor(n / 128); if (n) b |= 128; out.push(b); } while (n);
  return out;
}
function packet(head, ...parts) {
  const len = parts.reduce((n, p) => n + p.length, 0), vl = varLen(len);
  const out = new Uint8Array(1 + vl.length + len);
  out[0] = head;
  out.set(vl, 1);
  let o = 1 + vl.length;
  for (const p of parts) { out.set(p, o); o += p.length; }
  return out;
}
function strBytes(s) { const b = enc.encode(s); return [new Uint8Array([b.length >> 8, b.length & 255]), b]; }
function publishPacket(topic, payload, retain) { return packet(0x30 | (retain ? 1 : 0), ...strBytes(topic), payload); }
function matches(filter, topic) {
  const f = filter.split('/'), t = topic.split('/');
  for (let i = 0; i < f.length; i++) {
    if (f[i] === '#') return true;
    if (i >= t.length || (f[i] !== '+' && f[i] !== t[i])) return false;
  }
  return f.length === t.length;
}

class Reader {
  constructor(b) { this.b = b; this.i = 0; }
  u8() { if (this.i >= this.b.length) throw new Error('short'); return this.b[this.i++]; }
  u16() { return (this.u8() << 8) | this.u8(); }
  bytes() { const n = this.u16(); if (this.i + n > this.b.length) throw new Error('short'); const out = this.b.subarray(this.i, this.i + n); this.i += n; return out; }
  str() { return dec.decode(this.bytes()); }
  rest() { return this.b.subarray(this.i); }
}

export class Hub {
  constructor(ctx) { this.ctx = ctx; }

  async fetch(req) {
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ok: false, subs: [], will: null});
    const protos = (req.headers.get('Sec-WebSocket-Protocol') || '').split(',').map(s => s.trim());
    return new Response(null, {status: 101, webSocket: client, headers: protos.includes('mqtt') ? {'Sec-WebSocket-Protocol': 'mqtt'} : {}});
  }

  async webSocketMessage(ws, msg) {
    const data = typeof msg === 'string' ? enc.encode(msg) : new Uint8Array(msg);
    try {
      for (let i = 0; i < data.length;) {
        let n = 0, mul = 1, j = i + 1, b;
        do {
          if (j >= data.length || mul > 128 ** 3) throw new Error('bad length');
          b = data[j++];
          n += (b & 127) * mul;
          mul *= 128;
        } while (b & 128);
        if (j + n > data.length) throw new Error('short');
        await this.handle(ws, data[i], data.subarray(j, j + n));
        i = j + n;
      }
    } catch (e) {
      ws.close(1002, 'protocol error');
    }
  }

  async handle(ws, head, body) {
    const type = head >> 4, att = ws.deserializeAttachment(), r = new Reader(body);
    if (type !== 1 && !att.ok) throw new Error('not connected');
    if (type === 1) {
      r.str(); r.u8();
      const flags = r.u8();
      r.u16();
      r.str();
      att.will = null;
      if (flags & 4) {
        const t = r.str(), p = r.bytes();
        if (t.startsWith(PREFIX) && p.length <= MAX_RETAINED) att.will = {t, p: dec.decode(p), r: !!(flags & 0x20)};
      }
      att.ok = true;
      ws.serializeAttachment(att);
      ws.send(new Uint8Array([0x20, 2, 0, 0]));
    } else if (type === 3) {
      const qos = (head >> 1) & 3, topic = r.str();
      const id = qos ? r.u16() : 0;
      const payload = r.rest();
      if (qos === 1) ws.send(new Uint8Array([0x40, 2, id >> 8, id & 255]));
      if (topic.startsWith(PREFIX) && !/[+#]/.test(topic) && payload.length <= MAX_PAYLOAD) await this.publish(topic, payload, !!(head & 1));
    } else if (type === 8) {
      const id = r.u16(), granted = [];
      const added = [];
      while (r.i < body.length) {
        const f = r.str();
        r.u8();
        const ok = f.startsWith(PREFIX) && att.subs.length < MAX_SUBS;
        granted.push(ok ? 0 : 0x80);
        if (ok && !att.subs.includes(f)) { att.subs.push(f); added.push(f); }
      }
      ws.serializeAttachment(att);
      ws.send(packet(0x90, new Uint8Array([id >> 8, id & 255, ...granted])));
      for (const f of added) {
        const cut = f.search(/[+#]/);
        const stored = await this.ctx.storage.list({prefix: cut < 0 ? f : f.slice(0, cut), limit: 1000});
        for (const [t, p] of stored) if (matches(f, t)) ws.send(publishPacket(t, enc.encode(p), true));
      }
    } else if (type === 10) {
      const id = r.u16();
      while (r.i < body.length) { const f = r.str(); att.subs = att.subs.filter(s => s !== f); }
      ws.serializeAttachment(att);
      ws.send(new Uint8Array([0xb0, 2, id >> 8, id & 255]));
    } else if (type === 12) {
      ws.send(new Uint8Array([0xd0, 0]));
    } else if (type === 14) {
      att.will = null;
      ws.serializeAttachment(att);
      ws.close(1000, 'bye');
    }
  }

  async publish(topic, payload, retain) {
    if (retain) {
      if (!payload.length) await this.ctx.storage.delete(topic);
      else if (payload.length <= MAX_RETAINED) await this.ctx.storage.put(topic, dec.decode(payload));
    }
    const out = publishPacket(topic, payload, false);
    for (const ws of this.ctx.getWebSockets()) {
      const att = ws.deserializeAttachment();
      if (att && att.ok && att.subs.some(f => matches(f, topic))) {
        try { ws.send(out); } catch (e) {}
      }
    }
  }

  async gone(ws) {
    const att = ws.deserializeAttachment();
    if (!att || !att.will) return;
    const will = att.will;
    att.will = null;
    ws.serializeAttachment(att);
    await this.publish(will.t, enc.encode(will.p), will.r);
  }

  async webSocketClose(ws, code) {
    await this.gone(ws);
    try { ws.close(code === 1005 || code === 1006 ? 1000 : code, 'bye'); } catch (e) {}
  }

  async webSocketError(ws) { await this.gone(ws); }
}

// Ping Pong: the server runs the ball so both players see the same game. Each player only sends where
// their paddle is; the page draws its own paddle right away and the rest from these updates.
const W = 640, H = 360, PH = 70, BIG = 105, MINI = 44, PW = 10, PX = 24, BR = 7, WIN = 7, CAP = 10, V0 = 330, VFAST = 1150, VMAX = 1900,
  TURBO = 2000, PADDLE_V = 1600, TICK = 1000 / 120, ITEM_R = 16, ITEMS = ['big', 'shield', 'magnet', 'slow', 'mini', 'curve'], SLOW_MS = 4000, SLOW_F = 0.5, ITEM_LIFE = 12000,
  GOD_CHANCE = 0.025, GOD_MS = 8000, GOD_PH = 140, CURVE_MS = 12000, FACE_GAP = W - 2 * (PX + PW / 2 + BR);
// The owner's admin key (public half): a signed request from the owner's page can spawn the divine item.
const ADMIN_PUB = {kty: 'EC', crv: 'P-256', x: '30m7AWoz9Hby1aY-forxylWjKyUcxg5hykvUolJKZVg', y: '-BaBgI413Atu8-Ep57UHLwkFhcZbidMNGSwQ0hR1LpA'};

export class Arena {
  constructor(ctx) {
    this.ctx = ctx;
    this.key = '';
    this.seats = [null, null];
    this.names = ['', ''];
    this.left = [0, 0];
    this.timer = 0;
    this.b = {x: W / 2, y: H / 2, vx: 0, vy: 0};
    this.pad = [{y: H / 2, ty: H / 2}, {y: H / 2, ty: H / 2}];
    this.sc = [0, 0];
    this.st = 'wait';
    this.until = 0;
    this.w = 0;
    this.dir = Math.random() < 0.5 ? -1 : 1;
    this.rtt = [80, 80];
    this.hist = [[], []];
    this.pend = [null, null];
    this.votes = [null, null];
    this.pu = null;
    this.coin = false;
    this.item = null;
    this.nextItem = 0;
    this.since = ITEMS.map(() => 0);
    this.ph = [PH, PH];
    this.bigUntil = [0, 0];
    this.god = [0, 0];
    this.curve = [0, 0];
    this.spin = 0;
    this.adminT = 0;
    this.miniUntil = [0, 0];
    this.slow = [0, 0];
    this.shield = [false, false];
    this.magnet = [false, false];
    this.stuck = -1;
    this.stuckUntil = 0;
    this.aim = [H / 2, H / 2];
    this.turbo = false;
    this.last = -1;
    this.ev = {n: 0};
    // Pause: when both players have the game out of sight (Esc, another tab), the match stops until both are back.
    this.away = [false, false];
    this.pausedAt = 0;
    this.pausedSt = '';
  }

  async fetch(req) {
    const u = new URL(req.url), seat = Number(u.searchParams.get('seat')), key = u.searchParams.get('key') || '';
    if ((seat !== 1 && seat !== 2) || !/^[a-z0-9]{12,40}$/.test(key)) return new Response('bad request', {status: 400});
    if (this.key && this.key !== key) return new Response('forbidden', {status: 403});
    this.key = key;
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    const i = seat - 1, old = this.seats[i];
    if (old) { this.seats[i] = null; try { old.close(1000, 'replaced'); } catch (e) {} }
    this.seats[i] = server;
    this.away[i] = false;
    this.names[i] = (u.searchParams.get('name') || '').slice(0, 24);
    server.addEventListener('message', e => this.onMessage(i, e.data));
    const gone = () => { if (this.seats[i] === server) { this.seats[i] = null; this.left[i] = Date.now(); } };
    server.addEventListener('close', gone);
    server.addEventListener('error', gone);
    if (this.st === 'wait' && this.seats[0] && this.seats[1]) { this.st = 'vote'; this.until = Date.now() + 20000; }
    if (!this.timer) this.timer = setInterval(() => this.tick(), TICK);
    return new Response(null, {status: 101, webSocket: client});
  }

  onMessage(i, data) {
    let msg;
    try { msg = JSON.parse(data); } catch (e) { return; }
    const now = Date.now();
    if (msg.t === 'm' && Number.isFinite(msg.y)) {
      if (this.stuck === i) { this.aim[i] = Math.max(BR, Math.min(H - BR, msg.y)); return; }
      const y = Math.max(this.ph[i] / 2, Math.min(H - this.ph[i] / 2, msg.y)), h = this.hist[i];
      this.pad[i].ty = y;
      h.push([now, y]);
      while (h.length && now - h[0][0] > 800) h.shift();
    } else if (msg.t === 'ping' && Number.isFinite(msg.c)) {
      if (Number.isFinite(msg.rtt)) this.rtt[i] = Math.max(0, Math.min(400, msg.rtt));
      try { this.seats[i].send(JSON.stringify({t: 'pong', c: msg.c})); } catch (e) {}
    } else if (msg.t === 'vote' && this.st === 'vote' && typeof msg.v === 'boolean') {
      this.votes[i] = msg.v;
      if (this.votes[0] !== null && this.votes[1] !== null) this.decide();
    } else if (msg.t === 'fire' && this.stuck === i) {
      this.release(now);
    } else if (msg.t === 'god' && Number.isFinite(msg.ts) && typeof msg.sig === 'string') {
      this.adminGod(msg.ts, msg.sig);
    } else if (msg.t === 'away' && typeof msg.v === 'boolean') {
      this.away[i] = msg.v;
    } else if (msg.t === 'quit' && this.st !== 'over') {
      this.finish(2 - i);
    }
  }

  // Same vote wins; a tie (one for, one against) is a coin toss. A missing vote just doesn't count.
  decide() {
    const [a, b] = this.votes;
    this.coin = a !== null && b !== null && a !== b;
    this.pu = this.coin ? Math.random() < 0.5 : (a !== null ? a : b === true);
    this.nextItem = Date.now() + 3000 + 6000;
    this.serve('count', 3000);
  }

  serve(st, ms) {
    this.st = st;
    this.until = Date.now() + ms;
    Object.assign(this.b, {x: W / 2, y: H / 2, vx: 0, vy: 0});
    this.pend = [null, null];
    this.stuck = -1;
    this.turbo = false;
    this.spin = 0;
    this.last = -1;
  }

  launch() {
    const a = (Math.random() * 2 - 1) * Math.PI / 6;
    this.b.vx = this.dir * V0 * Math.cos(a);
    this.b.vy = V0 * Math.sin(a);
  }

  finish(winnerSeat) {
    this.st = 'over';
    this.w = winnerSeat;
    this.until = Date.now() + 8000;
  }

  event(k, i, kind) { this.ev = {n: this.ev.n + 1, k, i, kind}; }

  tick() {
    const now = Date.now(), prevTick = this.tickAt;
    this.tickAt = now;
    if (this.st === 'over') {
      if (now > this.until || (!this.seats[0] && !this.seats[1])) { clearInterval(this.timer); this.timer = 0; }
    } else if (this.st === 'vote') {
      if (now > this.until) this.decide();
    } else if (this.st !== 'wait') {
      for (let i = 0; i < 2; i++) if (!this.seats[i] && now - this.left[i] > 10000) this.finish(2 - i);
      if (this.st !== 'over' && this.seats[0] && this.seats[1]) {
        if (this.st === 'pause') { if (!this.away[0] && !this.away[1]) this.resume(now); }
        else if (this.away[0] && this.away[1]) this.pause(now);
        if (this.st === 'resume' && now >= this.until) this.st = 'play';
        if ((this.st === 'count' || this.st === 'goal') && now >= this.until) { this.st = 'play'; this.launch(); }
        // The ball moves by the time that really passed, so a late timer never slows or speeds it.
        const el = Math.min(0.05, Math.max(0, now - (prevTick || now - TICK)) / 1000);
        if (this.st === 'play') { this.powers(now); this.judge(now); this.physics(el, now); }
      }
    } else if (!this.seats[0] && !this.seats[1] && now - Math.max(...this.left) > 60000) {
      clearInterval(this.timer);
      this.timer = 0;
    }
    this.broadcast(now);
  }

  pause(now) {
    this.pausedSt = this.st === 'resume' ? 'play' : this.st;
    this.pausedAt = now;
    this.st = 'pause';
  }

  // Back from a pause: every running timer moves on by the paused time, so power-ups, items and a held magnet
  // shot have as long left as before. A rally goes on from where it stopped after a 3 s countdown; a pause
  // between points ends in the usual countdown before the serve.
  resume(now) {
    const d = now - this.pausedAt + (this.pausedSt === 'play' ? 3000 : 0), sh = t => t ? t + d : t;
    for (const k of ['bigUntil', 'miniUntil', 'god', 'curve', 'slow']) this[k] = this[k].map(sh);
    if (this.item) this.item.t += d;
    this.nextItem = sh(this.nextItem);
    this.stuckUntil = sh(this.stuckUntil);
    for (const h of this.hist) for (const e of h) e[0] += d;
    for (const p of this.pend) if (p) p.t += d;
    this.st = this.pausedSt === 'play' ? 'resume' : this.pausedSt;
    this.until = now + 3000;
  }

  powers(now) {
    for (let i = 0; i < 2; i++) {
      if (this.bigUntil[i] && now > this.bigUntil[i]) { this.bigUntil[i] = 0; this.ph[i] = PH; }
      if (this.miniUntil[i] && now > this.miniUntil[i]) { this.miniUntil[i] = 0; this.ph[i] = PH; }
      if (this.god[i] && now > this.god[i]) { this.god[i] = 0; this.ph[i] = this.bigUntil[i] ? BIG : PH; this.event('godend', i); }
    }
    if (!this.pu && !(this.item && this.item.k === 'god')) return;
    if (!this.item && now > this.nextItem) {
      // A 2.5 % roll for the divine item first; otherwise the usual fair pick.
      this.item = {k: Math.random() < GOD_CHANCE ? 'god' : this.pickItem(), x: W / 2 + (Math.random() * 2 - 1) * 110, y: 50 + Math.random() * (H - 100), t: now};
    }
    // An item nobody hits goes away after a while and makes room for a new one.
    if (this.item && now - this.item.t > ITEM_LIFE) {
      this.event('gone', -1, this.item.k);
      this.item = null;
      this.nextItem = now + 1500 + Math.random() * 1500;
    }
    const it = this.item, b = this.b;
    if (it && this.last >= 0 && this.stuck < 0 && Math.hypot(b.x - it.x, b.y - it.y) < BR + ITEM_R) {
      const i = this.last;
      if (it.k === 'big') { this.ph[i] = BIG; this.bigUntil[i] = now + 10000; this.miniUntil[i] = 0; }
      // Slow motion: for a few seconds the ball moves at half speed while it comes toward the taker.
      if (it.k === 'slow') this.slow[i] = now + SLOW_MS;
      // Mini paddle: the opponent's paddle shrinks for a while.
      if (it.k === 'mini') { const j = 1 - i; this.ph[j] = MINI; this.miniUntil[j] = now + 8000; this.bigUntil[j] = 0; }
      // Divine paddle: for 8 s a double-size paddle, every return a fireball, and a goal nothing gets through.
      // Curveball: for 12 s every return of the taker bends on its way across.
      if (it.k === 'curve') this.curve[i] = now + CURVE_MS;
      if (it.k === 'god') { this.god[i] = now + GOD_MS; this.ph[i] = GOD_PH; this.miniUntil[i] = 0; }
      if (it.k === 'shield') this.shield[i] = true;
      if (it.k === 'magnet') this.magnet[i] = true;
      this.event('get', i, it.k);
      this.item = null;
      this.nextItem = now + 7000 + Math.random() * 5000;
    }
  }

  // Power-ups that haven't come for a while are more likely: each one's weight is (items since it last came + 1)²,
  // so the one that just came drops to the bottom and the others climb. Starts even every match.
  pickItem() {
    const w = this.since.map(s => (s + 1) ** 2);
    let r = Math.random() * w.reduce((a, b) => a + b, 0), i = 0;
    while (i < w.length - 1 && r >= w[i]) r -= w[i++];
    this.since = this.since.map((s, j) => j === i ? 0 : s + 1);
    return ITEMS[i];
  }

  // A player sees the ball a little late but their own paddle at once, so a miss on the server can be a hit
  // on their screen. Before a miss counts, wait about one round trip for paddle reports from that moment.
  grace(i) { return Math.min(260, this.rtt[i] + 70); }

  bounce(i, paddleY, x, y, speed) {
    const s = i === 0 ? -1 : 1, rel = Math.max(-1, Math.min(1, (y - paddleY) / (this.ph[i] / 2))), a = rel * Math.PI / 3;
    // +5 % per return up to VFAST, then only +2 %, so the top speeds take a long rally.
    const v = Math.min(VMAX, speed * (speed < VFAST ? 1.05 : 1.02));
    Object.assign(this.b, {x, y, vx: -s * v * Math.cos(a), vy: v * Math.sin(a)});
    this.last = i;
    this.turbo = false;
    this.spin = 0;
    if (this.god[i] > Date.now()) { this.b.vx = -s * TURBO * Math.cos(a); this.b.vy = TURBO * Math.sin(a); this.turbo = true; }
    else if (this.curve[i] > Date.now()) this.spin = this.curveSpin(i, Math.abs(this.b.vx));
  }

  // The bend follows the paddle: moving up at the hit curves the ball up, moving down curves it down; from a
  // still paddle it bends against the ball's flight, so it flies a clear arc. It is scaled to the ball's speed
  // so a curve shifts the ball by about the same distance across the table (170–260 px), however fast it is.
  curveSpin(i, vx) {
    const h = this.hist[i], last = h[h.length - 1];
    let vel = 0;
    if (last) { const old = h.find(([t]) => t >= last[0] - 80) || last; vel = (last[1] - old[1]) / Math.max(0.03, (last[0] - old[0]) / 1000); }
    const dir = Math.abs(vel) > 120 ? Math.sign(vel) : Math.abs(this.b.vy) > 40 ? -Math.sign(this.b.vy) : Math.random() < 0.5 ? -1 : 1;
    const shift = 170 + 90 * Math.min(1, Math.abs(vel) / 800);
    return dir * 2 * shift * vx * vx / (FACE_GAP * FACE_GAP);
  }

  // The owner's page asks for the divine item, signed with the admin key; it appears at once.
  async adminGod(ts, sig) {
    if (Math.abs(Date.now() - ts) > 60000 || ts <= this.adminT || this.st !== 'play') return;
    try {
      const key = await crypto.subtle.importKey('jwk', ADMIN_PUB, {name: 'ECDSA', namedCurve: 'P-256'}, false, ['verify']);
      const raw = Uint8Array.from(atob(sig.replace(/-/g, '+').replace(/_/g, '/')), c => c.charCodeAt(0));
      if (!await crypto.subtle.verify({name: 'ECDSA', hash: 'SHA-256'}, key, raw, new TextEncoder().encode('god|' + this.key + '|' + ts))) return;
    } catch (e) { return; }
    this.adminT = ts;
    this.item = {k: 'god', x: W / 2 + (Math.random() * 2 - 1) * 80, y: 70 + Math.random() * (H - 140), t: Date.now()};
  }

  stick(i, now) {
    this.stuck = i;
    this.spin = 0;
    this.stuckUntil = now + 3000;
    this.magnet[i] = false;
    this.pad[i].ty = this.pad[i].y;
    this.aim[i] = H / 2;
    this.pend[i] = null;
    this.event('catch', i);
  }

  // The magnet shot leaves at full speed toward the aimed point on the other side.
  release() {
    const i = this.stuck, s = i === 0 ? -1 : 1, b = this.b;
    const dx = -s * (W - 2 * (PX + PW / 2 + BR)), dy = this.aim[i] - b.y;
    let a = Math.atan2(dy, Math.abs(dx));
    a = Math.max(-Math.PI / 3, Math.min(Math.PI / 3, a));
    b.vx = -s * TURBO * Math.cos(a);
    b.vy = TURBO * Math.sin(a);
    this.stuck = -1;
    this.turbo = true;
    this.spin = 0;
    this.last = i;
    this.event('fire', i);
  }

  judge(now) {
    for (let i = 0; i < 2; i++) {
      const p = this.pend[i];
      if (!p) continue;
      const g = this.grace(i), reach = this.ph[i] / 2 + BR + 3;
      const hit = this.hist[i].find(([t, y]) => t >= p.t - 60 && t <= p.t + g && Math.abs(p.y - y) <= reach);
      if (hit) {
        this.pend[i] = null;
        this.bounce(i, hit[1], p.x, p.y, p.v);
        const b = this.b, el = (now - p.t) / 1000;
        b.x += b.vx * el;
        b.y += b.vy * el;
        if (b.y < BR) b.y = 2 * BR - b.y;
        if (b.y > H - BR) b.y = 2 * (H - BR) - b.y;
        b.y = Math.max(BR, Math.min(H - BR, b.y));
      } else if (now > p.t + g) {
        p.done = true;
      }
    }
  }

  physics(dt, now) {
    const steps = Math.max(1, Math.ceil(dt / 0.0042)), h = dt / steps, b = this.b, maxStep = PADDLE_V * h;
    for (let k = 0; k < steps; k++) {
      for (let i = 0; i < 2; i++) {
        const p = this.pad[i], half = this.ph[i] / 2;
        p.ty = Math.max(half, Math.min(H - half, p.ty));
        if (this.stuck !== i) p.y += Math.max(-maxStep, Math.min(maxStep, p.ty - p.y));
        p.y = Math.max(half, Math.min(H - half, p.y));
      }
      if (this.stuck >= 0) {
        const i = this.stuck;
        b.x = i === 0 ? PX + PW / 2 + BR : W - PX - PW / 2 - BR;
        b.y = this.pad[i].y;
        if (now >= this.stuckUntil) this.release();
        return;
      }
      // An armed magnet pulls the ball toward its paddle while the ball is on its way there.
      for (let i = 0; i < 2; i++) {
        if (!this.magnet[i] || (i === 0 ? b.vx >= 0 : b.vx <= 0)) continue;
        b.vy += (this.pad[i].y - b.y) * 9 * h;
        const cap = Math.max(250, Math.abs(b.vx) * 1.4);
        b.vy = Math.max(-cap, Math.min(cap, b.vy));
      }
      const px = b.x, f = this.slowAt(b.vx, now);
      b.x += b.vx * h * f;
      b.y += b.vy * h * f;
      if (this.spin) {
        // The curve turns the ball's direction without changing its speed.
        const sp = Math.hypot(b.vx, b.vy);
        b.vy += this.spin * h * f;
        const k = sp / Math.hypot(b.vx, b.vy);
        b.vx *= k; b.vy *= k;
      }
      // A curving ball that hits a wall comes off it with the curve turned around.
      if (b.y < BR) { b.y = BR; b.vy = Math.abs(b.vy); if (this.spin < 0) this.spin = -this.spin; }
      if (b.y > H - BR) { b.y = H - BR; b.vy = -Math.abs(b.vy); if (this.spin > 0) this.spin = -this.spin; }
      for (let i = 0; i < 2; i++) {
        const face = i === 0 ? PX + PW / 2 : W - PX - PW / 2, s = i === 0 ? -1 : 1;
        const crossed = s < 0 ? b.vx < 0 && px - BR >= face - 1 && b.x - BR < face : b.vx > 0 && px + BR <= face + 1 && b.x + BR > face;
        if (!crossed) continue;
        if (this.magnet[i]) { this.stick(i, now); return; }
        // Where the ball meets the paddle sets the angle; every return is a bit faster.
        if (Math.abs(b.y - this.pad[i].y) <= this.ph[i] / 2 + BR) this.bounce(i, this.pad[i].y, face - s * BR, b.y, Math.hypot(b.vx, b.vy));
        else if (!this.pend[i] && !(this.god[i] > now)) this.pend[i] = {t: now, x: face - s * BR, y: b.y, v: Math.hypot(b.vx, b.vy)};
      }
      for (let i = 0; i < 2; i++) {
        const out = i === 0 ? b.x < -BR : b.x > W + BR;
        if (!out) continue;
        if (this.pend[i] && !this.pend[i].done) { b.x = i === 0 ? -BR : W + BR; continue; }
        if (this.god[i] > now) {
          // The golden wall sends everything back, as a fireball.
          b.x = i === 0 ? BR : W - BR;
          const sp = Math.hypot(b.vx, b.vy) || 1;
          b.vx = (i === 0 ? 1 : -1) * Math.abs(b.vx) / sp * TURBO;
          b.vy = b.vy / sp * TURBO;
          this.turbo = true;
          this.spin = 0;
          this.last = i;
          this.event('godwall', i);
          continue;
        }
        if (this.shield[i]) {
          this.shield[i] = false;
          this.pend[i] = null;
          this.spin = 0;
          b.x = i === 0 ? BR : W - BR;
          b.vx = i === 0 ? Math.abs(b.vx) : -Math.abs(b.vx);
          this.turbo = false;
          // A fireball bounced off a shield goes back as a normal ball: overall speed capped, direction kept.
          const sp = Math.hypot(b.vx, b.vy);
          if (sp > VMAX) { b.vx *= VMAX / sp; b.vy *= VMAX / sp; }
          this.event('shield', i);
          continue;
        }
        return this.point(1 - i);
      }
    }
  }

  // Slow motion only holds the ball back while it comes toward the taker; once it bounces off, it's full speed.
  slowAt(vx, now) { return now < this.slow[vx < 0 ? 0 : 1] ? SLOW_F : 1; }

  // First to 7, but from 6:6 on a set needs a two-point lead, up to 10: at 9:9 the next point wins.
  point(scorer) {
    this.sc[scorer]++;
    const a = this.sc[scorer], o = this.sc[1 - scorer];
    if (a >= CAP || (a >= WIN && a - o >= 2)) return this.finish(scorer + 1);
    this.dir = scorer === 0 ? 1 : -1;
    this.serve('goal', 1200);
  }

  broadcast(now) {
    const r = v => Math.round(v), f = this.st === 'play' && this.stuck < 0 ? this.slowAt(this.b.vx, now) : 1;
    const s = JSON.stringify({t: 's', ts: now, st: this.st, c: Math.max(0, this.until - now), w: this.w, sc: this.sc, names: this.names,
      on: [!!this.seats[0], !!this.seats[1]], p: [r(this.b.x), r(this.b.y), r(this.b.vx * f), r(this.b.vy * f)],
      sl: [Math.max(0, this.slow[0] - now), Math.max(0, this.slow[1] - now)],
      m: [[PX, r(this.pad[0].y)], [W - PX, r(this.pad[1].y)]], ph: this.ph,
      votes: this.votes, pu: this.pu, coin: this.coin, item: this.item && {k: this.item.k, x: r(this.item.x), y: r(this.item.y), l: Math.max(0, ITEM_LIFE - (now - this.item.t))},
      sh: this.shield, mg: this.magnet, gd: [Math.max(0, this.god[0] - now), Math.max(0, this.god[1] - now)],
      aw: this.away, cv: [Math.max(0, this.curve[0] - now), Math.max(0, this.curve[1] - now)], sp: r(this.spin * f * f), stuck: this.stuck, su: this.stuck >= 0 ? Math.max(0, this.stuckUntil - now) : 0,
      aim: this.stuck >= 0 ? r(this.aim[this.stuck]) : 0, turbo: this.turbo, ev: this.ev});
    for (const ws of this.seats) if (ws) try { ws.send(s); } catch (e) {}
  }
}

// Bolzplatz and Neon Rush: game rooms for two to four players, run on the server like Ping Pong. Players join
// with the room's key and their chat id; the first one in hosts. From a direct invitation (auto=2) the match
// starts as soon as both are there; at a table in the group chat the host starts it. Whoever drops out has
// 15 s to come back. When every player has the game out of sight, a running match pauses.
class Room {
  constructor(ctx, max, tickMs) {
    this.ctx = ctx;
    this.key = '';
    this.players = [];
    this.max = max;
    this.tickMs = tickMs;
    this.st = 'lobby';
    this.until = 0;
    this.auto = 0;
    this.timer = 0;
    this.ev = {n: 0};
    this.w = null;
    this.pausedAt = 0;
  }

  async fetch(req) {
    const u = new URL(req.url), key = u.searchParams.get('key') || '', pid = u.searchParams.get('pid') || '';
    if (!/^[a-z0-9]{12,40}$/.test(key) || !/^[A-Za-z0-9_-]{4,40}$/.test(pid)) return new Response('bad request', {status: 400});
    if (this.key && this.key !== key) return new Response('forbidden', {status: 403});
    let p = this.players.find(x => x.pid === pid);
    if (!p && (this.st !== 'lobby' || this.players.length >= this.max)) return new Response('full', {status: 409});
    this.key = key;
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    if (!p) { p = {pid, ws: null, left: 0, away: false}; this.players.push(p); this.joined(p); }
    if (p.ws) try { p.ws.close(1000, 'replaced'); } catch (e) {}
    p.ws = server;
    p.name = (u.searchParams.get('name') || '').slice(0, 24) || 'Spieler';
    p.left = 0;
    p.away = false;
    if (Number(u.searchParams.get('auto')) === 2) this.auto = 2;
    server.addEventListener('message', e => this.onMessage(p, e.data));
    const gone = () => {
      if (p.ws !== server) return;
      p.ws = null;
      p.left = Date.now();
      if (this.st === 'lobby' && !this.auto) this.remove(p);
    };
    server.addEventListener('close', gone);
    server.addEventListener('error', gone);
    this.full(p);
    if (!this.timer) { this.last = Date.now(); this.timer = setInterval(() => this.loop(), this.tickMs); }
    return new Response(null, {status: 101, webSocket: client});
  }

  remove(p) {
    const i = this.players.indexOf(p);
    if (i < 0) return;
    this.players.splice(i, 1);
    this.removed(p, i);
  }

  onMessage(p, data) {
    let m;
    try { m = JSON.parse(data); } catch (e) { return; }
    if (m.t === 'ping' && Number.isFinite(m.c)) { try { p.ws.send(JSON.stringify({t: 'pong', c: m.c})); } catch (e) {} }
    else if (m.t === 'away' && typeof m.v === 'boolean') p.away = m.v;
    else if (m.t === 'start' && this.st === 'lobby' && this.players[0] === p && this.players.length >= 2) this.begin(Date.now());
    else if (m.t === 'quit') { if (this.st === 'lobby') { try { p.ws.close(1000, 'bye'); } catch (e) {} this.remove(p); } else this.drop(p); }
    else this.input(p, m);
  }

  event(k, extra) { this.ev = {n: this.ev.n + 1, k, ...extra}; }

  loop() {
    const now = Date.now();
    if (this.st === 'over') {
      if (now > this.until || !this.players.some(p => p.ws)) { clearInterval(this.timer); this.timer = 0; }
    } else if (this.st === 'lobby') {
      if (this.auto && this.players.length >= this.auto && this.players.every(p => p.ws)) this.begin(now);
      if (!this.players.some(p => p.ws) && now - Math.max(0, ...this.players.map(p => p.left)) > 60000) { clearInterval(this.timer); this.timer = 0; }
    } else {
      for (const p of [...this.players]) if (!p.ws && now - p.left > 15000) this.drop(p);
      const on = this.players.filter(p => p.ws);
      if (this.st === 'pause') {
        if (on.length && on.every(p => !p.away)) { this.st = 'resume'; this.until = now + 3000; this.pauseLen = now - this.pausedAt + 3000; }
      } else if (this.st === 'play' && on.length && on.every(p => p.away)) {
        this.st = 'pause';
        this.pausedAt = now;
      } else if (this.st === 'resume' && now >= this.until) {
        this.st = 'play';
        this.resumed(this.pauseLen);
      }
    }
    this.advance(now);
    this.last = now;
    this.send(now);
  }

  // Everyone's own copy of the state, plus the full picture once on joining.
  broadcast(obj) {
    const s = JSON.stringify(obj);
    for (const p of this.players) if (p.ws) try { p.ws.send(s); } catch (e) {}
  }
  roster() { return this.players.map(p => ({pid: p.pid, name: p.name, on: !!p.ws, aw: p.away, ...this.extra(p)})); }
  joined() {}
  removed() {}
  full() {}
  resumed() {}
  extra() { return {}; }
}

// ⚽ Bolzplatz: top-down football like Haxball. Round players, a ball and two goals; arrow keys or WASD to run,
// Space or X to kick. Physics run 60 times a second in pixels per tick, with the classic Haxball constants:
// players accelerate by 0.1 (0.07 while holding kick), everything slows by a fixed factor each tick, and a kick
// adds 5 to the ball's speed. First to 3 goals or the lead after 3 minutes; a draw goes to a golden goal.
const KW = 760, KH = 380, KM = 32, GOAL_H = 124, GOAL_D = 38, POST_R = 6, KPR = 15, KBR = 9, KCC = 70,
  K_ACC = 0.1, K_ACC_KICK = 0.07, K_DAMP = 0.96, B_DAMP = 0.99, K_KICK = 5, K_REACH = 4, K_LIMIT = 3, K_TIME = 180000;
export class Kick extends Room {
  constructor(ctx) {
    super(ctx, 4, 1000 / 60);
    this.sc = [0, 0];
    this.b = {x: KW / 2, y: KH / 2, vx: 0, vy: 0};
    this.tl = K_TIME;
    this.ot = false;
    this.ko = -1;
    this.touch = null;
    this.acc = 0;
    this.tick = 0;
  }
  joined(p) {
    const n = [0, 1].map(t => this.players.filter(x => x !== p && x.team === t).length);
    Object.assign(p, {team: n[0] <= n[1] ? 0 : 1, x: 0, y: 0, vx: 0, vy: 0, k: 0, q: [], ack: 0, armed: true, owed: 0, started: false});
  }
  extra(p) { return {team: p.team}; }
  input(p, m) {
    if (m.t === 'team' && this.st === 'lobby' && (m.v === 0 || m.v === 1)) {
      if (this.players.filter(x => x.team === m.v).length < 2) p.team = m.v;
    } else if (m.t === 'face' && ['baby', 'gleg'].includes(m.f) && this.st === 'play') {
      // The owner's face-ball prank, passed on by its victim: the ball wears the face until the next goal.
      this.face = m.f;
    } else if (m.t === 'in' && Number.isInteger(m.k) && Number.isInteger(m.i)) {
      p.q.push([m.k & 31, m.i]);
      if (p.q.length > 12) p.q.splice(0, p.q.length - 12);
    }
  }
  begin(now) {
    // Teams that ended up uneven in the lobby get evened out.
    for (;;) {
      const n = [0, 1].map(t => this.players.filter(x => x.team === t).length);
      if (Math.abs(n[0] - n[1]) < 2) break;
      const from = n[0] > n[1] ? 0 : 1;
      this.players.filter(x => x.team === from).pop().team = 1 - from;
    }
    if (!this.players.some(p => p.team === 0)) this.players[0].team = 0;
    if (!this.players.some(p => p.team === 1)) this.players[this.players.length - 1].team = 1;
    this.kickoff(Math.random() < 0.5 ? 0 : 1);
    this.st = 'count';
    this.until = now + 3000;
  }
  // Positions for a kickoff: each team in its own half, the ball on the spot.
  kickoff(team) {
    Object.assign(this.b, {x: KW / 2, y: KH / 2, vx: 0, vy: 0});
    for (const t of [0, 1]) {
      const mates = this.players.filter(p => p.team === t);
      mates.forEach((p, i) => Object.assign(p, {owed: 0,
        x: t === 0 ? KW / 2 - 150 : KW / 2 + 150,
        y: mates.length === 1 ? KH / 2 : KH / 2 + (i ? 70 : -70), vx: 0, vy: 0}));
    }
    this.ko = team;
    this.touch = null;
  }
  drop(p) {
    this.remove(p);
    if (this.st === 'over' || this.st === 'lobby') return;
    for (const t of [0, 1]) if (!this.players.some(x => x.team === t)) return this.finish(1 - t);
  }
  finish(team) {
    this.st = 'over';
    this.w = team;
    this.until = Date.now() + 10000;
    this.event('over', {team});
  }
  advance(now) {
    if (this.st === 'count' && now >= this.until) this.st = 'play';
    if (this.st === 'goal' && now >= this.until) {
      if (this.sc[0] >= K_LIMIT || this.sc[1] >= K_LIMIT || this.ot) return this.finish(this.sc[0] > this.sc[1] ? 0 : 1);
      this.kickoff(this.lastGoal === 0 ? 1 : 0);
      this.st = 'play';
    }
    if (this.st !== 'play' && this.st !== 'goal') { this.acc = 0; return; }
    // Fixed 60 Hz steps, however the timer fires.
    this.acc += Math.min(100, now - this.last);
    while (this.acc >= 1000 / 60) {
      this.acc -= 1000 / 60;
      this.step();
      if (this.st === 'play' && !this.ot) {
        this.tl -= 1000 / 60;
        if (this.tl <= 0) {
          this.tl = 0;
          if (this.sc[0] !== this.sc[1]) return this.finish(this.sc[0] > this.sc[1] ? 0 : 1);
          this.ot = true;
          this.event('ot');
        }
      }
    }
  }
  step() {
    this.tick++;
    const b = this.b, cy = KH / 2;
    // Every input from a page is one movement step, exactly as that page already showed it, used one per tick
    // (two while catching up) so others see the player move evenly. When inputs come late, the player runs on
    // with the last one for up to 8 ticks, and that many late inputs are then skipped, so each input still
    // counts exactly once.
    for (const p of this.players) {
      if (!p.q.length) {
        if (p.started && p.owed < 8) { p.owed++; kickMove(p, p.k); kickBounds(p, this.ko); }
        continue;
      }
      p.started = true;
      while (p.owed > 0 && p.q.length > 1) { p.ack = p.q.shift()[1]; p.owed--; }
      const steps = p.q.length > 4 ? 2 : 1;
      for (let n = 0; n < steps && p.q.length; n++) {
        const [k, i] = p.q.shift();
        p.k = k;
        p.ack = i;
        kickMove(p, k);
        kickBounds(p, this.ko);
        if (!(k & 16)) p.armed = true;
        else if (p.armed && Math.hypot(b.x - p.x, b.y - p.y) < KPR + KBR + K_REACH && this.st === 'play') {
          const d = Math.hypot(b.x - p.x, b.y - p.y) || 1;
          b.vx += (b.x - p.x) / d * K_KICK;
          b.vy += (b.y - p.y) / d * K_KICK;
          p.armed = false;
          this.touch = p.pid;
          this.event('kick', {pid: p.pid});
        }
      }
    }
    b.x += b.vx; b.y += b.vy;
    b.vx *= B_DAMP; b.vy *= B_DAMP;
    // Collisions: players with each other, players with the ball (players weigh twice as much).
    const ps = this.players;
    for (let i = 0; i < ps.length; i++) {
      for (let j = i + 1; j < ps.length; j++) discHit(ps[i], ps[j], KPR, KPR, 0.5, 0.5, 0.25);
      if (discHit(ps[i], b, KPR, KBR, 0.5, 1, 0.25)) this.touch = ps[i].pid;
    }
    for (const p of ps) {
      kickBounds(p, this.ko);
      for (const s of [-1, 1]) for (const gx of [0, KW]) postHit(p, gx, cy + s * GOAL_H / 2, KPR);
    }
    for (const s of [-1, 1]) for (const gx of [0, KW]) postHit(b, gx, cy + s * GOAL_H / 2, KBR);
    ballBounds(b);
    if (this.ko >= 0 && Math.hypot(b.vx, b.vy) > 0.01) this.ko = -1;
    if (this.st === 'play' && (b.x < -KBR || b.x > KW + KBR)) {
      const team = b.x < 0 ? 1 : 0, scorer = ps.find(p => p.pid === this.touch);
      this.sc[team]++;
      this.lastGoal = team;
      this.face = null;
      this.st = 'goal';
      this.until = Date.now() + 2500;
      this.event('goal', {team, by: scorer ? scorer.name : '', own: !!scorer && scorer.team !== team});
    }
  }
  full(p) { p.ws.send(JSON.stringify(this.state(Date.now()))); }
  state(now) {
    const r = v => Math.round(v * 10) / 10, r2 = v => Math.round(v * 100) / 100;
    return {t: 's', ts: now, st: this.st, c: Math.max(0, this.until - now), tl: Math.round(this.tl), ot: this.ot, sc: this.sc, w: this.w, ko: this.ko,
      host: this.players[0] && this.players[0].pid, tk: this.tick, ev: this.ev, lim: K_LIMIT, face: this.face || null,
      b: [r(this.b.x), r(this.b.y), r(this.b.vx), r(this.b.vy)],
      pl: this.roster().map((x, i) => { const p = this.players[i]; return {...x, x: r2(p.x), y: r2(p.y), vx: r2(p.vx), vy: r2(p.vy), k: p.k, a: p.ack}; })};
  }
  send(now) { this.broadcast(this.state(now)); }
}
// One player's movement for one tick (the page runs the same for its own player, to show it without delay).
function kickMove(p, k) {
  let dx = ((k & 8) ? 1 : 0) - ((k & 4) ? 1 : 0), dy = ((k & 2) ? 1 : 0) - ((k & 1) ? 1 : 0);
  if (dx && dy) { dx *= Math.SQRT1_2; dy *= Math.SQRT1_2; }
  const a = (k & 16) ? K_ACC_KICK : K_ACC;
  p.vx += dx * a; p.vy += dy * a;
  p.x += p.vx; p.y += p.vy;
  p.vx *= K_DAMP; p.vy *= K_DAMP;
}
function kickBounds(p, ko) {
  p.x = Math.max(-KM + KPR, Math.min(KW + KM - KPR, p.x));
  p.y = Math.max(-KM + KPR, Math.min(KH + KM - KPR, p.y));
  if (ko < 0) return;
  // Kickoff: everyone stays in their own half, and the team without the kickoff out of the centre circle.
  if (p.team === 0 && p.x > KW / 2 - KPR) { p.x = KW / 2 - KPR; p.vx = Math.min(0, p.vx); }
  if (p.team === 1 && p.x < KW / 2 + KPR) { p.x = KW / 2 + KPR; p.vx = Math.max(0, p.vx); }
  if (p.team !== ko) {
    const dx = p.x - KW / 2, dy = p.y - KH / 2, d = Math.hypot(dx, dy) || 1;
    if (d < KCC + KPR) { p.x = KW / 2 + dx / d * (KCC + KPR); p.y = KH / 2 + dy / d * (KCC + KPR); }
  }
}
// The ball stays on the pitch except through the goal mouths; inside a goal the net stops it softly.
function ballBounds(b) {
  const cy = KH / 2, top = cy - GOAL_H / 2, bot = cy + GOAL_H / 2;
  if (b.y < KBR) { b.y = KBR; b.vy = -b.vy * 0.5; }
  if (b.y > KH - KBR) { b.y = KH - KBR; b.vy = -b.vy * 0.5; }
  for (const side of [0, 1]) {
    const inGoal = side === 0 ? b.x < 0 : b.x > KW, line = side === 0 ? 0 : KW, s = side === 0 ? 1 : -1;
    if (inGoal) {
      if (b.y < top + KBR) { b.y = top + KBR; b.vy = Math.abs(b.vy) * 0.1; }
      if (b.y > bot - KBR) { b.y = bot - KBR; b.vy = -Math.abs(b.vy) * 0.1; }
      const back = line - s * (GOAL_D - KBR);
      if (side === 0 ? b.x < back : b.x > back) { b.x = back; b.vx = s * Math.abs(b.vx) * 0.1; }
    } else if ((b.y < top || b.y > bot) && (side === 0 ? b.x < KBR : b.x > KW - KBR)) {
      b.x = side === 0 ? KBR : KW - KBR;
      b.vx = s * Math.abs(b.vx) * 0.5;
    }
  }
}
// Two discs that overlap are pushed apart by their weight, and bounce (bc = how much speed the bounce keeps).
function discHit(a, b, ra, rb, ia, ib, bc) {
  const dx = b.x - a.x, dy = b.y - a.y, d = Math.hypot(dx, dy);
  if (d >= ra + rb || d === 0) return false;
  const nx = dx / d, ny = dy / d, over = ra + rb - d, im = ia + ib;
  a.x -= nx * over * ia / im; a.y -= ny * over * ia / im;
  b.x += nx * over * ib / im; b.y += ny * over * ib / im;
  const rv = (b.vx - a.vx) * nx + (b.vy - a.vy) * ny;
  if (rv < 0) {
    const j = -(1 + bc) * rv / im;
    a.vx -= nx * j * ia; a.vy -= ny * j * ia;
    b.vx += nx * j * ib; b.vy += ny * j * ib;
  }
  return true;
}
function postHit(o, px, py, r) {
  const dx = o.x - px, dy = o.y - py, d = Math.hypot(dx, dy);
  if (d >= r + POST_R || d === 0) return;
  const nx = dx / d, ny = dy / d;
  o.x = px + nx * (r + POST_R); o.y = py + ny * (r + POST_R);
  const rv = o.vx * nx + o.vy * ny;
  if (rv < 0) { o.vx -= 1.5 * rv * nx; o.vy -= 1.5 * rv * ny; }
}

// 🏍️ Neon Rush: light bikes on a grid. Every bike leaves a wall behind it; whoever drives into a wall, a trail
// or another bike is out, the last one riding wins the round. First to 3 rounds. Arrow keys or WASD turn
// (turns are queued, so a quick double turn works), Space boosts to double speed while the boost bar lasts.
const TW = 80, TH = 50, T_HZ = 24, T_SPEED = 12, T_FUEL = 2000, T_WIN = 3;
// Start lanes are offset, so nobody rides head-on into anybody else.
const T_SPAWN = [[6, 15, 1], [73, 34, 3], [60, 4, 2], [19, 45, 0]], T_DIR = [[0, -1], [1, 0], [0, 1], [-1, 0]];
export class Tron extends Room {
  constructor(ctx) {
    super(ctx, 4, 1000 / T_HZ);
    this.grid = new Uint8Array(TW * TH);
    this.rd = 0;
    this.tk = 0;
    this.mv = [];
    this.ghosts = [];
  }
  joined(p) { Object.assign(p, {sc: 0, alive: false, x: 0, y: 0, d: 1, q: [], fuel: T_FUEL, boost: false, acc: 0, trail: []}); }
  extra(p) { return {sc: p.sc, al: p.alive, x: p.x, y: p.y, d: p.d, f: Math.round(p.fuel), bo: p.boost, fc: p.face || null}; }
  input(p, m) {
    // A turn can name the move it is meant for (the page has already shown it there); it waits for that move.
    if (m.t === 'turn' && Number.isInteger(m.d) && m.d >= 0 && m.d < 4 && p.q.length < 3) p.q.push({d: m.d, n: Number.isInteger(m.n) ? m.n : 0});
    else if (m.t === 'boost' && typeof m.v === 'boolean') p.boost = m.v;
    else if (m.t === 'face' && ['baby', 'gleg'].includes(m.f) && this.st === 'play') p.face = m.f;
    else if (m.t === 'full') this.full(p);
  }
  begin(now) { this.round(now); }
  round(now) {
    this.rd++;
    this.grid.fill(0);
    this.ghosts = [];
    this.players.forEach((p, i) => {
      const [x, y, d] = T_SPAWN[i];
      Object.assign(p, {alive: true, x, y, d, q: [], fuel: T_FUEL, acc: 0, trail: [[x, y]], face: null, mc: 0});
      this.grid[y * TW + x] = i + 1;
    });
    this.st = 'count';
    this.until = now + 3000;
    for (const p of this.players) this.full(p);
  }
  drop(p) {
    const i = this.players.indexOf(p);
    this.remove(p);
    if (i >= 0 && p.trail.length) this.ghosts.push(p.trail);
    if (i >= 0) {
      // The grid marks trails by seat; seats after the one that left move down by one.
      for (let c = 0; c < this.grid.length; c++) {
        const v = this.grid[c];
        if (v === i + 1) this.grid[c] = 255;
        else if (v > i + 1 && v !== 255) this.grid[c] = v - 1;
      }
    }
    if (this.st === 'lobby' || this.st === 'over') return;
    if (this.players.length < 2) return this.finish(this.players[0] || null);
    this.checkRound(Date.now());
    for (const q of this.players) this.full(q);
  }
  finish(p) {
    this.st = 'over';
    this.w = p ? p.pid : null;
    this.until = Date.now() + 10000;
    this.event('over', {pid: this.w});
  }
  checkRound(now) {
    if (this.st !== 'play') return;
    const alive = this.players.filter(p => p.alive);
    if (alive.length > 1) return;
    const win = alive[0] || null;
    if (win) win.sc++;
    this.event('round', {pid: win ? win.pid : null});
    if (win && win.sc >= T_WIN) return this.finish(win);
    this.st = 'roundover';
    this.until = now + 2500;
  }
  advance(now) {
    this.mv = [];
    if (this.st === 'count' && now >= this.until) this.st = 'play';
    if (this.st === 'roundover' && now >= this.until) this.round(now);
    if (this.st !== 'play') return;
    this.tk++;
    // Each bike moves 0, 1 or 2 cells this tick; moves happen in two rounds so bikes move at the same time.
    const steps = this.players.map(p => {
      if (!p.alive) return 0;
      const fast = p.boost && p.fuel > 0;
      if (fast) p.fuel = Math.max(0, p.fuel - 1000 / T_HZ);
      p.acc += (fast ? 2 : 1) * T_SPEED / T_HZ;
      const n = Math.floor(p.acc);
      p.acc -= n;
      return n;
    });
    for (let r = 0; r < 2; r++) {
      const target = new Map();
      this.players.forEach((p, i) => {
        if (!p.alive || steps[i] <= r) return;
        while (p.q.length && !(p.q[0].n > p.mc + 1)) {
          const d = p.q.shift().d;
          if (d !== p.d && d !== (p.d + 2) % 4) { p.d = d; break; }
        }
        const nx = p.x + T_DIR[p.d][0], ny = p.y + T_DIR[p.d][1];
        if (nx < 0 || ny < 0 || nx >= TW || ny >= TH || this.grid[ny * TW + nx]) { this.crash(p, i); return; }
        const c = ny * TW + nx;
        if (target.has(c)) { this.crash(p, i); const o = target.get(c); if (o.p.alive) this.crash(o.p, o.i); return; }
        target.set(c, {p, i, nx, ny});
      });
      for (const [c, {p, i, nx, ny}] of target) {
        if (!p.alive) continue;
        p.x = nx; p.y = ny;
        p.mc++;
        p.trail.push([nx, ny]);
        this.grid[c] = i + 1;
        this.mv.push([i, nx, ny]);
      }
    }
    this.checkRound(now);
  }
  crash(p, i) {
    p.alive = false;
    this.event('crash', {pid: p.pid, x: p.x, y: p.y});
  }
  full(p) { if (p.ws) try { p.ws.send(JSON.stringify({t: 'full', rd: this.rd, trails: this.players.map(q => q.trail), ghosts: this.ghosts})); } catch (e) {} }
  send(now) {
    this.broadcast({t: 's', ts: now, st: this.st, c: Math.max(0, this.until - now), tk: this.tk, rd: this.rd, win: T_WIN, w: this.w,
      host: this.players[0] && this.players[0].pid, ev: this.ev, pl: this.roster(), mv: this.mv});
  }
}
