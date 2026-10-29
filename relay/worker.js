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
      cv: [Math.max(0, this.curve[0] - now), Math.max(0, this.curve[1] - now)], sp: r(this.spin * f * f), stuck: this.stuck, su: this.stuck >= 0 ? Math.max(0, this.stuckUntil - now) : 0,
      aim: this.stuck >= 0 ? r(this.aim[this.stuck]) : 0, turbo: this.turbo, ev: this.ev});
    for (const ws of this.seats) if (ws) try { ws.send(s); } catch (e) {}
  }
}
