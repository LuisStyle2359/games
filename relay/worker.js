// A small MQTT 3.1.1 broker (over WebSocket) for the chat, as a second server next to the public ones.
// It only implements what the page uses: QoS 0, retained messages, last will, '+'/'#' wildcards.
// Messages are end-to-end encrypted by the page, so this server only ever sees ciphertext.

const PREFIX = 'dv-2563318804-chat/';
const MAX_PAYLOAD = 256 * 1024;
const MAX_RETAINED = 16 * 1024;
const MAX_SUBS = 32;
// Limits against flooding: messages per socket and second, kept messages per mailbox and in total.
const MAX_RATE = 40, MAX_MAIL = 100, MAX_KEPT = 5000;
const enc = new TextEncoder(), dec = new TextDecoder();

export default {
  async fetch(req, env) {
    if (req.headers.get('Upgrade') !== 'websocket') return new Response('ok\n', {headers: {'content-type': 'text/plain'}});
    const game = new URL(req.url).pathname.match(/^\/game\/([A-Za-z0-9]{8,48})$/);
    // Pinned to western Europe, where everyone plays; otherwise an object lives wherever it was first used.
    if (game) return env.ARENA.get(env.ARENA.idFromName(game[1]), {locationHint: 'weur'}).fetch(req);
    const room = new URL(req.url).pathname.match(/^\/(kick|tron|breach)\/([A-Za-z0-9]{8,48})$/);
    if (room) { const ns = {kick: env.KICK, tron: env.TRON, breach: env.BREACH}[room[1]]; return ns.get(ns.idFromName(room[2]), {locationHint: 'weur'}).fetch(req); }
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
  constructor(ctx) { this.ctx = ctx; this.rate = new WeakMap(); this.kept = -1; }

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
      // At most MAX_RATE messages a second from one connection; more are dropped.
      const now = Date.now(), rt = this.rate.get(ws) || {t: now, n: 0};
      if (now - rt.t > 1000) { rt.t = now; rt.n = 0; }
      this.rate.set(ws, rt);
      if (++rt.n > MAX_RATE) return;
      if (topic.startsWith(PREFIX) && !/[+#]/.test(topic) && payload.length <= MAX_PAYLOAD) await this.publish(topic, payload, !!(head & 1));
    } else if (type === 8) {
      const id = r.u16(), granted = [];
      const added = [];
      while (r.i < body.length) {
        const f = r.str();
        r.u8();
        // Only '+' wildcards (what the page uses): nobody can listen to everything with '#'.
        const ok = f.startsWith(PREFIX) && !f.includes('#') && att.subs.length < MAX_SUBS;
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
      const st = this.ctx.storage;
      if (this.kept < 0) this.kept = (await st.list({limit: MAX_KEPT + 1})).size;
      if (!payload.length) { if (await st.delete(topic)) this.kept--; }
      else if (payload.length <= MAX_RETAINED) {
        // A full mailbox, or too much kept in total, takes nothing new (replacing what is there is fine).
        const box = topic.match(/^(.*\/mail\/[^/]+\/)/), isNew = (await st.get(topic)) === undefined;
        const full = isNew && (this.kept >= MAX_KEPT || (box && (await st.list({prefix: box[1], limit: MAX_MAIL})).size >= MAX_MAIL));
        if (!full) { await st.put(topic, dec.decode(payload)); if (isNew) this.kept++; }
      }
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
  TURBO = 2000, PADDLE_V = 1600, TICK = 1000 / 120, ITEM_R = 16, ITEMS = ['big', 'shield', 'magnet', 'slow', 'mini', 'curve', 'ghost'], SLOW_MS = 4000, SLOW_F = 0.5, ITEM_LIFE = 12000,
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
    this.tok = ['', ''];
    this.emoAt = [0, 0];
    // Rally: returns in the current point. Rescue: the one-time wall for whoever falls 3 behind. Ghost: the ball
    // fades in the middle of the table after the taker's returns.
    this.rally = 0;
    // The match in numbers, per seat, for the end screen; log: who won each point, in order.
    this.stats = [0, 1].map(() => ({ret: 0, top: 0, items: 0, fire: 0, saves: 0, rally: 0}));
    this.log = [];
    this.rescued = [false, false];
    this.ghost = [0, 0];
    this.hide = false;
    this.spAt = [0, 0];
    this.rttN = [0, 0];
    this.evq = [];
  }

  async fetch(req) {
    const u = new URL(req.url), seat = Number(u.searchParams.get('seat')), key = u.searchParams.get('key') || '';
    if ((seat !== 1 && seat !== 2) || !/^[a-z0-9]{12,40}$/.test(key)) return new Response('bad request', {status: 400});
    if (this.key && this.key !== key) return new Response('forbidden', {status: 403});
    // A seat belongs to the page that took it first (its token): the other player can't take it over.
    const i0 = seat - 1, tok = (u.searchParams.get('tok') || '').slice(0, 64);
    if (this.tok[i0] && this.tok[i0] !== tok) return new Response('taken', {status: 409});
    this.tok[i0] = tok;
    this.key = key;
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    const i = seat - 1, old = this.seats[i];
    // The page it replaces (another tab) is told not to reconnect.
    if (old) { this.seats[i] = null; try { old.close(4001, 'replaced'); } catch (e) {} }
    this.seats[i] = server;
    this.away[i] = false;
    this.names[i] = (u.searchParams.get('name') || '').slice(0, 24);
    server.addEventListener('message', e => { if (this.seats[i] === server) this.onMessage(i, e.data); });
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
      try { this.seats[i].send(JSON.stringify({t: 'pong', c: msg.c})); } catch (e) {}
    } else if (msg.t === 'spr' && Number.isFinite(msg.c) && msg.c === this.spAt[i]) {
      // The round trip as the server measures it (a page could claim any ping), smoothed a little.
      const r = Math.max(0, Math.min(400, Date.now() - msg.c));
      this.rtt[i] = this.rttN[i]++ ? this.rtt[i] * 0.7 + r * 0.3 : r;
    } else if (msg.t === 'vote' && this.st === 'vote' && typeof msg.v === 'boolean') {
      this.votes[i] = msg.v;
      if (this.votes[0] !== null && this.votes[1] !== null) this.decide();
    } else if (msg.t === 'fire' && this.stuck === i && this.st === 'play') {
      this.release(now);
    } else if (msg.t === 'god' && Number.isFinite(msg.ts) && typeof msg.sig === 'string') {
      this.adminGod(msg.ts, msg.sig);
    } else if (msg.t === 'emo' && Number.isInteger(msg.v) && msg.v >= 0 && msg.v < 5 && now - (this.emoAt[i] || 0) > 700) {
      // A quick reaction for the other one to see (no sound, so no trash talk otherwise); at most one every 0.7 s.
      this.emoAt[i] = now;
      this.event('emo', i, msg.v);
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
    this.rally = 0;
    this.hide = false;
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

  event(k, i, kind) { this.ev = {n: this.ev.n + 1, k, i, kind}; this.evq.push(this.ev); if (this.evq.length > 8) this.evq.shift(); }

  tick() {
    const now = Date.now(), prevTick = this.tickAt;
    this.tickAt = now;
    if (this.st === 'over') {
      if (now > this.until || (!this.seats[0] && !this.seats[1])) { clearInterval(this.timer); this.timer = 0; }
    } else if (this.st === 'vote') {
      if (now > this.until) this.decide();
    } else if (this.st !== 'wait') {
      for (let i = 0; i < 2 && this.st !== 'over'; i++) if (!this.seats[i] && now - this.left[i] > 10000) this.finish(2 - i);
      // A dropped connection stops the game like both looking away: no power-up runs out meanwhile, and it goes
      // on with a countdown once both are back.
      const gone = !this.seats[0] || !this.seats[1];
      if (this.st !== 'over' && gone && this.st !== 'pause' && this.st !== 'vote') this.pause(now);
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
    // Every 2 s the server asks each page for an echo, to know its ping itself.
    for (let i = 0; i < 2; i++) if (this.seats[i] && now - this.spAt[i] > 2000) { this.spAt[i] = now; try { this.seats[i].send(JSON.stringify({t: 'sp', c: now})); } catch (e) {} }
    this.broadcast(now);
  }

  pause(now) {
    this.pausedSt = this.st === 'resume' ? 'play' : this.st;
    // Paused again during the countdown: the rest of that countdown isn't counted twice.
    this.pausedAt = this.st === 'resume' ? this.until : now;
    this.st = 'pause';
  }

  // Back from a pause: every running timer moves on by the paused time, so power-ups, items and a held magnet
  // shot have as long left as before. A rally goes on from where it stopped after a 3 s countdown; a pause
  // between points ends in the usual countdown before the serve.
  resume(now) {
    const d = now - this.pausedAt + (this.pausedSt === 'play' ? 3000 : 0), sh = t => t ? t + d : t;
    for (const k of ['bigUntil', 'miniUntil', 'god', 'curve', 'slow', 'ghost']) this[k] = this[k].map(sh);
    if (this.item) this.item.t += d;
    this.nextItem = sh(this.nextItem);
    this.stuckUntil = sh(this.stuckUntil);
    for (const h of this.hist) for (const e of h) if (e[0] <= this.pausedAt) e[0] += d;
    for (const p of this.pend) if (p) p.t += d;
    this.st = this.pausedSt === 'play' ? 'resume' : this.pausedSt;
    this.until = now + 3000;
  }

  powers(now) {
    for (let i = 0; i < 2; i++) {
      if (this.bigUntil[i] && now > this.bigUntil[i]) { this.bigUntil[i] = 0; this.ph[i] = PH; }
      if (this.miniUntil[i] && now > this.miniUntil[i]) { this.miniUntil[i] = 0; this.ph[i] = PH; }
      if (this.god[i] && now > this.god[i]) { this.god[i] = 0; this.event('godend', i); }
      // Overlapping power-ups: the divine paddle beats mini, mini beats big.
      this.ph[i] = this.god[i] ? GOD_PH : this.miniUntil[i] ? MINI : this.bigUntil[i] ? BIG : PH;
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
      if (it.k === 'ghost') this.ghost[i] = now + 10000;
      if (it.k === 'god') { this.god[i] = now + GOD_MS; this.ph[i] = GOD_PH; this.miniUntil[i] = 0; }
      if (it.k === 'shield') this.shield[i] = true;
      if (it.k === 'magnet') this.magnet[i] = true;
      this.stats[i].items++;
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
    this.rally++;
    const stt = this.stats[i];
    stt.ret++;
    stt.top = Math.max(stt.top, Math.round(Math.hypot(this.b.vx, this.b.vy)));
    stt.rally = Math.max(stt.rally, this.rally);
    this.hide = this.ghost[i] > Date.now();
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
    this.stats[i].fire++;
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
          this.stats[i].saves++;
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
          this.stats[i].saves++;
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
    // How long the rally was (for records on the pages).
    this.event('point', scorer, this.rally);
    this.log.push(scorer);
    for (const st of this.stats) st.rally = Math.max(st.rally, this.rally);
    if (a >= CAP || (a >= WIN && a - o >= 2)) return this.finish(scorer + 1);
    // Aufholjagd: the first time someone is 3 behind, they get a free shield wall (once per match).
    const j = 1 - scorer;
    if (a - o >= 3 && !this.rescued[j]) { this.rescued[j] = true; this.shield[j] = true; this.event('rescue', j); }
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
      aim: this.stuck >= 0 ? r(this.aim[this.stuck]) : 0, turbo: this.turbo, ev: this.ev, evs: this.evq, rl: this.rally, gb: this.hide && this.stuck < 0,
      fin: this.st === 'over' ? {st: this.stats, log: this.log} : undefined});
    for (const ws of this.seats) if (ws) try { ws.send(s); } catch (e) {}
  }
}


// Fußball and Neon Rush: game rooms for two to four players, run on the server like Ping Pong. Players join
// with the room's key and their chat id; the first one in hosts. From a direct invitation (auto=2) the match
// starts as soon as both are there; at a table in the group chat the host starts it. First everyone votes for
// or against power-ups (the majority wins, a tie is a coin toss). Whoever drops out has 15 s to come back.
// A match pauses when a player presses pause (twice per player, at most a minute each) or when every player
// has the game out of sight; it goes on after a 3 s countdown. All game timers count game ticks, so a pause
// takes nothing off them.
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
    this.evq = [];
    this.w = null;
    this.votes = {};
    this.pu = false;
    this.coin = false;
    this.pausedBy = null;
    this.pauseEnd = 0;
    // Solo: the page runs this room itself against bots (no pause limit; only the human counts as away).
    this.solo = false;
    this.think = null;
  }

  async fetch(req) {
    const u = new URL(req.url), key = u.searchParams.get('key') || '', pid = u.searchParams.get('pid') || '';
    if (!/^[a-z0-9]{12,40}$/.test(key) || !/^[A-Za-z0-9_-]{4,40}$/.test(pid)) return new Response('bad request', {status: 400});
    if (this.key && this.key !== key) return new Response('forbidden', {status: 403});
    let p = this.players.find(x => x.pid === pid);
    if (!p && (this.st !== 'lobby' || this.players.length >= this.max)) return new Response('full', {status: 409});
    // A seat belongs to the page that took it: a reconnect has to bring the same secret, so nobody who knows
    // the room's key and someone's chat id can take over their seat.
    const sec = (u.searchParams.get('sec') || '').slice(0, 64);
    if (p && p.sec && p.sec !== sec) return new Response('taken', {status: 409});
    this.key = key;
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    const first = !this.players.length;
    if (!p) { p = {pid, sec, ws: null, left: 0, away: false, pauses: 2}; this.players.push(p); this.joined(p); }
    if (p.ws) try { p.ws.close(1000, 'replaced'); } catch (e) {}
    p.ws = server;
    p.name = (u.searchParams.get('name') || '').slice(0, 24) || 'Spieler';
    p.left = 0;
    p.away = false;
    // Only whoever opens the room can make it a direct match that starts by itself.
    if (first && Number(u.searchParams.get('auto')) === 2) this.auto = 2;
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
    // Someone who left (or quit) has no say any more.
    if (!this.players.includes(p)) return;
    let m;
    try { m = JSON.parse(data); } catch (e) { return; }
    const now = Date.now();
    if (m.t === 'ping' && Number.isFinite(m.c)) { try { p.ws.send(JSON.stringify({t: 'pong', c: m.c})); } catch (e) {} }
    else if (m.t === 'away' && typeof m.v === 'boolean') p.away = m.v;
    else if (m.t === 'start' && this.st === 'lobby' && this.players[0] === p && this.players.length >= 2) this.vote(now);
    else if (m.t === 'vote' && this.st === 'vote' && typeof m.v === 'boolean') {
      this.votes[p.pid] = m.v;
      if (this.players.filter(q => q.ws).every(q => q.pid in this.votes)) this.decide(now);
    } else if (m.t === 'pause' && this.st === 'play' && (p.pauses > 0 || this.solo)) {
      if (!this.solo) p.pauses--;
      this.st = 'pause';
      this.pausedBy = p.pid;
      this.pauseEnd = now + (this.solo ? 864e5 : 60000);
      this.event('pause', {pid: p.pid});
    } else if (m.t === 'resume' && this.st === 'pause' && this.pausedBy) this.resume(now);
    else if (m.t === 'quit') { const ws = p.ws; if (this.st === 'lobby') this.remove(p); else this.drop(p); try { ws.close(1000, 'bye'); } catch (e) {} }
    else this.input(p, m);
  }

  vote(now) {
    this.st = 'vote';
    this.until = now + 15000;
    this.votes = {};
  }
  decide(now) {
    const v = Object.values(this.votes), yes = v.filter(x => x).length, no = v.length - yes;
    this.coin = yes === no && yes > 0;
    this.pu = yes > no || (this.coin && Math.random() < 0.5);
    this.event('vote', {pu: this.pu, coin: this.coin});
    this.begin(now);
  }
  resume(now) {
    this.st = 'resume';
    this.until = now + 3000;
    this.pausedBy = null;
  }

  // Several things can happen in one tick (a hit, then the kill, then the end of the round): the last few events
  // go out with every update, so the page sees each one even if updates are lost or come together.
  event(k, extra) { this.ev = {n: this.ev.n + 1, k, ...extra}; this.evq.push(this.ev); if (this.evq.length > 8) this.evq.shift(); }

  loop() {
    const now = Date.now();
    if (this.st === 'over') {
      if (now > this.until || !this.players.some(p => p.ws)) { clearInterval(this.timer); this.timer = 0; }
    } else if (this.st === 'lobby') {
      if (this.auto && this.players.length >= this.auto && this.players.every(p => p.ws)) this.vote(now);
      if (!this.players.some(p => p.ws) && now - Math.max(0, ...this.players.map(p => p.left)) > 60000) { clearInterval(this.timer); this.timer = 0; }
    } else {
      for (const p of [...this.players]) if (!p.ws && now - p.left > 15000) this.drop(p);
      const on = this.players.filter(p => p.ws && !p.bot);
      if (this.st === 'vote') { if (now >= this.until) this.decide(now); }
      else if (this.st === 'pause') {
        // A pause someone pressed ends with "Weiter" or after a minute; one because nobody looks, when all are back.
        if (this.pausedBy ? now >= this.pauseEnd : on.length && on.every(p => !p.away)) this.resume(now);
      } else if (this.st === 'play' && on.length && on.every(p => p.away)) {
        this.st = 'pause';
        this.pausedBy = null;
      } else if (this.st === 'resume' && now >= this.until) this.st = 'play';
    }
    this.advance(now);
    this.last = now;
    this.send(now);
  }

  // What every update carries besides the game itself.
  base(now) {
    return {t: 's', ts: now, st: this.st, c: Math.max(0, this.until - now), w: this.w, host: this.players[0] && this.players[0].pid, ev: this.ev, evs: this.evq,
      vt: this.st === 'vote' ? this.votes : null, pu: this.pu, coin: this.coin, pb: this.st === 'pause' ? this.pausedBy : null,
      pz: this.st === 'pause' && this.pausedBy ? Math.max(0, this.pauseEnd - now) : 0};
  }
  broadcast(obj) {
    const s = JSON.stringify(obj);
    for (const p of this.players) if (p.ws) try { p.ws.send(s); } catch (e) {}
  }
  roster() { return this.players.map(p => ({pid: p.pid, name: p.name, on: !!p.ws, aw: p.away, ps: p.pauses, ...this.extra(p)})); }
  // Power-ups that haven't come for a while are more likely, as in Ping Pong: weight (items since + 1)².
  pickItem(kinds) {
    const since = this.since || (this.since = kinds.map(() => 0)), w = since.map(s => (s + 1) ** 2);
    let r = Math.random() * w.reduce((a, b) => a + b, 0), i = 0;
    while (i < w.length - 1 && r >= w[i]) r -= w[i++];
    this.since = since.map((s, j) => j === i ? 0 : s + 1);
    return kinds[i];
  }
  joined() {}
  removed() {}
  full() {}
  extra() { return {}; }
}

// ⚽ Fußball: top-down football like Haxball. Round players, a ball and two goals; arrow keys or WASD to run,
// Space or X to kick. Physics run 60 times a second in pixels per tick, with the classic Haxball constants:
// players accelerate by 0.1 (0.07 while holding kick), everything slows by a fixed factor each tick, and a kick
// adds 5 to the ball's speed. First to 3 goals or the lead after 3 minutes; a draw goes to a golden goal.
const KW = 760, KH = 380, KM = 32, GOAL_H = 124, GOAL_D = 38, POST_R = 6, KPR = 15, KBR = 9, KCC = 70,
  K_ACC = 0.1, K_ACC_KICK = 0.07, K_DAMP = 0.96, B_DAMP = 0.99, K_KICK = 5, K_REACH = 4, K_LIMIT = 3, K_TIME = 180000,
  K_ITEM_R = 14, K_ITEMS = ['turbo', 'hammer', 'sticky', 'wall', 'giant', 'dwarf', 'ice', 'curve'], K_GOD = 0.025, K_WALL_X = 22;
// Power-up durations in ticks (60 a second).
const K_FX = {turbo: 480, giant: 480, dwarf: 480, ice: 360, curve: 600, god: 480, wall: 360, sticky: 900};
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
    this.item = null;
    this.nextItem = 540;
    this.wall = [0, 0];
    this.spin = 0;
    this.hold = null;
  }
  joined(p) {
    const n = [0, 1].map(t => this.players.filter(x => x !== p && x.team === t).length);
    Object.assign(p, {team: n[0] <= n[1] ? 0 : 1, x: 0, y: 0, vx: 0, vy: 0, k: 0, q: [], ack: 0, armed: true, owed: 0, started: false, fx: kickFx(), fdx: 1, fdy: 0});
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
  // Positions for a kickoff: each team in its own half, the ball on the spot. Power-ups end with a goal.
  kickoff(team) {
    Object.assign(this.b, {x: KW / 2, y: KH / 2, vx: 0, vy: 0});
    for (const t of [0, 1]) {
      const mates = this.players.filter(p => p.team === t);
      mates.forEach((p, i) => Object.assign(p, {owed: 0, fx: kickFx(),
        x: t === 0 ? KW / 2 - 150 : KW / 2 + 150,
        y: mates.length === 1 ? KH / 2 : KH / 2 + (i ? 70 : -70), vx: 0, vy: 0}));
    }
    this.ko = team;
    this.koT = 0;
    this.touch = null;
    this.wall = [0, 0];
    this.spin = 0;
    this.hold = null;
    this.item = null;
    this.nextItem = Math.max(this.nextItem, 120);
  }
  drop(p) {
    this.remove(p);
    if (this.hold && this.hold.pid === p.pid) this.hold = null;
    if (this.st === 'vote') { if (this.players.length < 2) this.st = 'lobby'; return; }
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
  // A kick from player p, with the power-ups that change it.
  kick(p, dx, dy) {
    const b = this.b, f = (p.fx.god || p.fx.hammer > 0 ? 2 : 1);
    if (!p.fx.god && p.fx.hammer > 0) p.fx.hammer--;
    b.vx += dx * K_KICK * f;
    b.vy += dy * K_KICK * f;
    // Curveball: the shot bends to the side the kicker is moving, or to a random side from standing.
    this.spin = 0;
    if (p.fx.curve) {
      const side = p.vx * -dy + p.vy * dx;
      this.spin = (Math.abs(side) > 0.3 ? Math.sign(side) : Math.random() < 0.5 ? -1 : 1) * 0.016;
    }
    p.armed = false;
    this.touch = p.pid;
    this.shotV = Math.hypot(b.vx, b.vy);
    this.event('kick', {pid: p.pid, hard: f > 1, curve: !!this.spin});
  }
  step() {
    this.tick++;
    if (this.think) this.think();
    const b = this.b, cy = KH / 2, ps = this.players;
    // The last 1.5 s (every second tick) for the goal replay.
    if (this.st === 'play' && this.tick % 2 === 0) {
      const rp = this.rp || (this.rp = []);
      rp.push([Math.round(b.x), Math.round(b.y), ...ps.flatMap(p => [Math.round(p.x), Math.round(p.y)])]);
      if (rp.length > 45) rp.shift();
    }
    // Every input from a page is one movement step, exactly as that page already showed it, used one per tick
    // (two while catching up) so others see the player move evenly. When inputs come late, the player runs on
    // with the last one for up to 8 ticks, and that many late inputs are then skipped, so each input still
    // counts exactly once.
    for (const p of ps) {
      for (const k in p.fx) if (k !== 'hammer' && p.fx[k] > 0) p.fx[k]--;
      // One step per tick on average, whatever a page sends (a short burst to catch up is fine).
      p.cr = Math.min(3, (p.cr || 0) + 1);
      if (!p.q.length) {
        if (p.started && p.owed < 8) { p.owed++; p.cr--; kickMove(p, p.k); kickBounds(p, this.ko); }
        continue;
      }
      p.started = true;
      while (p.owed > 0 && p.q.length > 1) { p.ack = p.q.shift()[1]; p.owed--; }
      // Inputs piled up while the server stood still: drop the oldest, so the player doesn't lag behind for good.
      if (p.q.length > 4 && p.cr < 2) { p.ack = p.q[p.q.length - 5][1]; p.q.splice(0, p.q.length - 4); }
      const steps = Math.min(p.q.length > 4 ? 2 : 1, Math.floor(p.cr));
      for (let n = 0; n < steps && p.q.length; n++) {
        p.cr--;
        const [k, i] = p.q.shift();
        p.k = k;
        p.ack = i;
        kickMove(p, k);
        kickBounds(p, this.ko);
        const dx = ((k & 8) ? 1 : 0) - ((k & 4) ? 1 : 0), dy = ((k & 2) ? 1 : 0) - ((k & 1) ? 1 : 0);
        if (dx || dy) { const l = Math.hypot(dx, dy); p.fdx = dx / l; p.fdy = dy / l; }
        if (!(k & 16)) p.armed = true;
        else if (p.armed && this.st === 'play') {
          if (this.hold && this.hold.pid === p.pid) {
            // A held ball leaves in the direction the player is running, a little harder than a normal kick.
            this.hold = null;
            p.fx.sticky = 0;
            b.vx = p.vx + p.fdx * 1.6;
            b.vy = p.vy + p.fdy * 1.6;
            this.kick(p, p.fdx, p.fdy);
          } else if (Math.hypot(b.x - p.x, b.y - p.y) < kickR(p) + KBR + K_REACH) {
            const d = Math.hypot(b.x - p.x, b.y - p.y) || 1;
            this.kick(p, (b.x - p.x) / d, (b.y - p.y) / d);
          }
        }
      }
    }
    for (const t of [0, 1]) if (this.wall[t] > 0) this.wall[t]--;
    if (this.hold) {
      const h = ps.find(p => p.pid === this.hold.pid);
      if (!h || --this.hold.t <= 0) { if (h) h.fx.sticky = 0; this.hold = null; }
      else {
        const r = kickR(h) + KBR + 0.5;
        Object.assign(b, {x: h.x + h.fdx * r, y: h.y + h.fdy * r, vx: h.vx, vy: h.vy});
      }
    }
    if (!this.hold) {
      b.x += b.vx; b.y += b.vy;
      b.vx *= B_DAMP; b.vy *= B_DAMP;
      if (this.spin) {
        const a = this.spin, c = Math.cos(a), s = Math.sin(a), vx = b.vx;
        b.vx = vx * c - b.vy * s;
        b.vy = vx * s + b.vy * c;
        this.spin *= 0.985;
        if (Math.abs(this.spin) < 0.002) this.spin = 0;
      }
    }
    // Collisions: players with each other, players with the ball (players weigh twice as much, a giant more).
    for (let i = 0; i < ps.length; i++) {
      for (let j = i + 1; j < ps.length; j++) discHit(ps[i], ps[j], kickR(ps[i]), kickR(ps[j]), kickW(ps[i]), kickW(ps[j]), 0.25);
      if (this.hold && this.hold.pid === ps[i].pid) continue;
      const threat = goalThreat(b), fast = Math.hypot(b.vx, b.vy) > 3;
      if (discHit(ps[i], b, kickR(ps[i]), KBR, kickW(ps[i]), 1, 0.25)) {
        // A save: the ball was on its way into this player's own goal (team 0 defends the left one) and isn't now.
        if (fast && threat === ps[i].team && goalThreat(b) !== threat && this.tick - (this.saveT || 0) > 30) { this.saveT = this.tick; this.event('save', {pid: ps[i].pid, name: ps[i].name}); }
        this.touch = ps[i].pid;
        this.spin = 0;
        if (this.hold) { const h = ps.find(p => p.pid === this.hold.pid); if (h) h.fx.sticky = 0; this.hold = null; this.event('steal', {pid: ps[i].pid}); }
        // Sticky foot: the ball stays at the foot for up to 2 s.
        else if (ps[i].fx.sticky && this.st === 'play') { this.hold = {pid: ps[i].pid, t: 120}; this.event('stick', {pid: ps[i].pid}); }
      }
    }
    for (const p of ps) {
      kickBounds(p, this.ko);
      for (const s of [-1, 1]) for (const gx of [0, KW]) postHit(p, gx, cy + s * GOAL_H / 2, kickR(p));
    }
    // A hard shot off the post: everyone should see that one.
    const spd = Math.hypot(b.vx, b.vy);
    for (const s of [-1, 1]) for (const gx of [0, KW]) if (postHit(b, gx, cy + s * GOAL_H / 2, KBR) && spd > 2.5 && this.tick - (this.postT || 0) > 30) {
      this.postT = this.tick;
      this.event('post', {x: gx, y: cy + s * GOAL_H / 2});
    }
    // The wall power-up: a barrier in front of the goal that only the ball bounces off.
    for (const t of [0, 1]) if (this.wall[t] > 0 && wallHit(b, t === 0 ? K_WALL_X : KW - K_WALL_X)) { this.spin = 0; this.event('wallhit', {team: t}); }
    ballBounds(b);
    // Kickoff: once the ball moves, or after 5 s if nobody takes it, everyone may go for the ball.
    if (this.ko >= 0 && (Math.hypot(b.vx, b.vy) > 0.01 || ++this.koT > 300)) this.ko = -1;
    this.items();
    if (this.st === 'play' && (b.x < -KBR || b.x > KW + KBR)) {
      const team = b.x < 0 ? 1 : 0, scorer = ps.find(p => p.pid === this.touch);
      this.sc[team]++;
      this.lastGoal = team;
      this.face = null;
      this.hold = null;
      this.spin = 0;
      this.st = 'goal';
      // Long enough for the cheering and then the replay in slow motion.
      this.until = Date.now() + 5000;
      this.broadcast({t: 'replay', pids: ps.map(p => p.pid), fr: this.rp || []});
      this.rp = [];
      // About 19 px are a metre; the speed of the last shot, in km/h.
      this.event('goal', {team, by: scorer ? scorer.name : '', own: !!scorer && scorer.team !== team, kmh: Math.round((this.shotV || 0) * 60 / 19 * 3.6)});
    }
  }
  // Items appear one at a time on the pitch; a player runs over one, or the ball carries it to whoever last
  // touched the ball. One left alone goes away after 12 s.
  items() {
    if (!this.pu || this.st !== 'play') return;
    const b = this.b;
    if (!this.item) {
      if (--this.nextItem > 0 || this.ko >= 0) return;
      let x, y, tries = 0;
      do { x = 110 + Math.random() * (KW - 220); y = 40 + Math.random() * (KH - 80); }
      while (++tries < 20 && (Math.hypot(x - b.x, y - b.y) < 80 || Math.hypot(x - KW / 2, y - KH / 2) < KCC));
      this.item = {k: Math.random() < K_GOD ? 'god' : this.pickItem(K_ITEMS), x, y, life: 720};
      this.event('spawn', {kind: this.item.k});
      return;
    }
    const it = this.item;
    let taker = this.players.find(p => Math.hypot(p.x - it.x, p.y - it.y) < kickR(p) + K_ITEM_R);
    if (!taker && this.touch && Math.hypot(b.x - it.x, b.y - it.y) < KBR + K_ITEM_R) taker = this.players.find(p => p.pid === this.touch);
    if (taker) {
      this.give(taker, it.k);
      this.item = null;
      this.nextItem = 420 + Math.floor(Math.random() * 300);
    } else if (--it.life <= 0) {
      this.event('gone', {kind: it.k});
      this.item = null;
      this.nextItem = 120 + Math.floor(Math.random() * 120);
    }
  }
  give(p, k) {
    const foes = this.players.filter(q => q.team !== p.team);
    if (k === 'turbo') p.fx.turbo = K_FX.turbo;
    else if (k === 'hammer') p.fx.hammer = 3;
    else if (k === 'sticky') p.fx.sticky = K_FX.sticky;
    else if (k === 'wall') this.wall[p.team] = K_FX.wall;
    else if (k === 'giant') { p.fx.giant = K_FX.giant; p.fx.dwarf = 0; }
    else if (k === 'dwarf') for (const q of foes) { q.fx.dwarf = K_FX.dwarf; q.fx.giant = 0; }
    else if (k === 'ice') for (const q of foes) q.fx.ice = K_FX.ice;
    else if (k === 'curve') p.fx.curve = K_FX.curve;
    else if (k === 'god') { p.fx.god = K_FX.god; this.wall[p.team] = Math.max(this.wall[p.team], K_FX.god); }
    this.event('get', {pid: p.pid, kind: k, team: p.team});
  }
  full(p) { p.ws.send(JSON.stringify(this.state(Date.now()))); }
  state(now) {
    const r = v => Math.round(v * 10) / 10, r2 = v => Math.round(v * 100) / 100;
    return {...this.base(now), tl: Math.round(this.tl), ot: this.ot, sc: this.sc, ko: this.ko, tk: this.tick, lim: K_LIMIT, face: this.face || null,
      b: [r(this.b.x), r(this.b.y), r(this.b.vx), r(this.b.vy)], it: this.item && {k: this.item.k, x: r(this.item.x), y: r(this.item.y), l: this.item.life},
      wall: this.wall, spin: r2(this.spin * 100), hold: this.hold && this.hold.pid,
      pl: this.roster().map((x, i) => { const p = this.players[i]; return {...x, x: r2(p.x), y: r2(p.y), vx: r2(p.vx), vy: r2(p.vy), k: p.k, a: p.ack, fx: p.fx, r: kickR(p)}; })};
  }
  send(now) { this.broadcast(this.state(now)); }
}
function kickFx() { return {turbo: 0, hammer: 0, sticky: 0, giant: 0, dwarf: 0, ice: 0, curve: 0, god: 0}; }
function kickR(p) { return p.fx.giant ? 30 : p.fx.dwarf ? 8 : KPR; }
function kickW(p) { return p.fx.giant ? 0.2 : p.fx.dwarf ? 0.9 : 0.5; }
// One player's movement for one tick (the page runs the same for its own player, to show it without delay).
// Turbo makes a player 1.5 times as fast; on ice they get going slowly and slide on.
function kickMove(p, k) {
  let dx = ((k & 8) ? 1 : 0) - ((k & 4) ? 1 : 0), dy = ((k & 2) ? 1 : 0) - ((k & 1) ? 1 : 0);
  if (dx && dy) { dx *= Math.SQRT1_2; dy *= Math.SQRT1_2; }
  const fx = p.fx, a = ((k & 16) ? K_ACC_KICK : K_ACC) * (fx.turbo || fx.god ? 1.5 : 1) * (fx.ice ? 0.5 : 1), damp = fx.ice ? 0.985 : K_DAMP;
  p.vx += dx * a; p.vy += dy * a;
  p.x += p.vx; p.y += p.vy;
  p.vx *= damp; p.vy *= damp;
}
function kickBounds(p, ko) {
  const r = kickR(p);
  p.x = Math.max(-KM + r, Math.min(KW + KM - r, p.x));
  p.y = Math.max(-KM + r, Math.min(KH + KM - r, p.y));
  if (ko < 0) return;
  // Kickoff: everyone stays in their own half, and the team without the kickoff out of the centre circle.
  if (p.team === 0 && p.x > KW / 2 - r) { p.x = KW / 2 - r; p.vx = Math.min(0, p.vx); }
  if (p.team === 1 && p.x < KW / 2 + r) { p.x = KW / 2 + r; p.vx = Math.max(0, p.vx); }
  if (p.team !== ko) {
    const dx = p.x - KW / 2, dy = p.y - KH / 2, d = Math.hypot(dx, dy) || 1;
    if (d < KCC + r) { p.x = KW / 2 + dx / d * (KCC + r); p.y = KH / 2 + dy / d * (KCC + r); }
  }
}
// The wall: a vertical bar at x, a little longer than the goal is high.
function wallHit(b, x) {
  const top = KH / 2 - GOAL_H / 2 - 26, bot = KH / 2 + GOAL_H / 2 + 26;
  if (b.y < top - KBR || b.y > bot + KBR || Math.abs(b.x - x) >= KBR + 3) return false;
  const s = b.x < x ? -1 : 1;
  b.x = x + s * (KBR + 3);
  if (b.vx * s < 0) b.vx = -b.vx * 0.8;
  return true;
}
// The ball stays on the pitch except through the goal mouths; inside a goal the net stops it softly.
function ballBounds(b) {
  const cy = KH / 2, top = cy - GOAL_H / 2, bot = cy + GOAL_H / 2;
  if (b.y < KBR) { b.y = KBR; b.vy = -b.vy * 0.5; }
  if (b.y > KH - KBR) { b.y = KH - KBR; b.vy = -b.vy * 0.5; }
  for (const side of [0, 1]) {
    const inGoal = side === 0 ? b.x < 0 : b.x > KW, line = side === 0 ? 0 : KW, s = side === 0 ? 1 : -1;
    // A fast ball that went past the end line beside the goal in one step bounces off the wall, it doesn't score.
    if (inGoal && (b.y < top || b.y > bot) && (side === 0 ? b.x - b.vx >= 0 : b.x - b.vx <= KW)) {
      b.x = side === 0 ? KBR : KW - KBR;
      b.vx = s * Math.abs(b.vx) * 0.5;
      continue;
    }
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
  if (d >= r + POST_R || d === 0) return false;
  const nx = dx / d, ny = dy / d;
  o.x = px + nx * (r + POST_R); o.y = py + ny * (r + POST_R);
  const rv = o.vx * nx + o.vy * ny;
  if (rv < 0) { o.vx -= 1.5 * rv * nx; o.vy -= 1.5 * rv * ny; }
  return true;
}
// Where a ball moving like this crosses an end line within 40 ticks: 0 (left goal), 1 (right goal) or -1.
function goalThreat(b) {
  if (Math.abs(b.vx) < 0.5) return -1;
  const line = b.vx < 0 ? 0 : KW, t = (line - b.x) / b.vx, y = b.y + b.vy * t, cy = KH / 2;
  return t > 0 && t < 40 && Math.abs(y - cy) < GOAL_H / 2 ? (b.vx < 0 ? 0 : 1) : -1;
}

// 🏍️ Neon Rush: light bikes on a grid. Every bike leaves a wall behind it; whoever drives into a wall, a trail
// or another bike is out, the last one riding wins the round. First to 3 rounds. Arrow keys or WASD turn
// (turns are queued, so a quick double turn works), Space boosts to double speed while the boost bar lasts.
const TW = 80, TH = 50, T_HZ = 24, T_SPEED = 12, T_FUEL = 2000, T_WIN = 3;
// Start lanes are offset, so nobody rides head-on into anybody else.
const T_SPAWN = [[6, 15, 1], [73, 34, 3], [60, 4, 2], [19, 45, 0]], T_DIR = [[0, -1], [1, 0], [0, 1], [-1, 0]];
const T_ITEMS = ['nitro', 'ghost', 'bomb', 'gap', 'brake', 'confuse', 'shield'], T_GOD = 0.025;
// Power-up durations in ticks (24 a second).
const T_FX = {ghost: 72, gap: 72, brake: 96, confuse: 96, god: 120};
export class Tron extends Room {
  constructor(ctx) {
    super(ctx, 4, 1000 / T_HZ);
    this.grid = new Uint8Array(TW * TH);
    this.rd = 0;
    this.tk = 0;
    this.mv = [];
    this.er = [];
    this.ghosts = [];
    this.items = [];
    this.nextItem = 0;
  }
  joined(p) { Object.assign(p, {sc: 0, alive: false, x: 0, y: 0, d: 1, q: [], fuel: T_FUEL, boost: false, acc: 0, trail: [], fx: tronFx()}); }
  extra(p) { return {sc: p.sc, al: p.alive, x: p.x, y: p.y, d: p.d, f: Math.round(p.fuel), bo: p.boost, fc: p.face || null, fx: p.fx, sp: tronSpeed(p), gr: this.tk - p.gr < 4}; }
  input(p, m) {
    // A turn can name the move it is meant for (the page has already shown it there); it waits for that move.
    // A turn can't wait for a move far ahead (a stale one from the last round would block all others).
    if (m.t === 'turn' && Number.isInteger(m.d) && m.d >= 0 && m.d < 4 && p.q.length < 3) p.q.push({d: m.d, n: Number.isInteger(m.n) ? Math.min(m.n, p.mc + 8) : 0});
    else if (m.t === 'boost' && typeof m.v === 'boolean') p.boost = m.v;
    else if (m.t === 'face' && ['baby', 'gleg'].includes(m.f) && this.st === 'play') p.face = m.f;
    else if (m.t === 'full') this.full(p);
  }
  begin(now) { this.round(now); }
  round(now) {
    this.rd++;
    this.grid.fill(0);
    this.ghosts = [];
    this.items = [];
    this.nextItem = 96 + Math.floor(Math.random() * 72);
    this.players.forEach((p, i) => {
      const [x, y, d] = T_SPAWN[i];
      Object.assign(p, {alive: true, x, y, d, q: [], fuel: T_FUEL, acc: 0, trail: [[x, y, 1]], face: null, mc: 0, fx: tronFx(), od: 1, gr: -99});
      this.grid[y * TW + x] = i + 1;
    });
    this.t0 = this.tk;
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
    if (this.st === 'vote') { if (this.players.length < 2) this.st = 'lobby'; return; }
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
    // Long enough for the time-lapse of the round.
    this.until = now + 3500;
  }
  free(x, y) { return x >= 0 && y >= 0 && x < TW && y < TH && !this.grid[y * TW + x]; }
  // Whether a bike can move into cell (x, y): a ghost or a divine bike rides through trails, never off the grid.
  open(p, x, y) { return x >= 0 && y >= 0 && x < TW && y < TH && (!this.grid[y * TW + x] || p.fx.ghost > 0 || p.fx.god > 0); }
  advance(now) {
    this.mv = [];
    this.er = [];
    if (this.st === 'count' && now >= this.until) this.st = 'play';
    if (this.st === 'roundover' && now >= this.until) this.round(now);
    if (this.st !== 'play') return;
    this.tk++;
    // Overdrive: after 30 s of a round every bike gets faster, up to +50 % over the next minute.
    const od = 1 + Math.min(0.5, Math.max(0, (this.tk - this.t0 - 30 * T_HZ) / (60 * T_HZ)));
    if (this.tk - this.t0 === 30 * T_HZ) this.event('overdrive', {});
    for (const p of this.players) p.od = od;
    for (const p of this.players) for (const k in p.fx) if (k !== 'shield' && p.fx[k] > 0) p.fx[k]--;
    if (this.think) this.think();
    // Each bike moves 0, 1 or 2 cells this tick; moves happen in two rounds so bikes move at the same time.
    const steps = this.players.map(p => {
      if (!p.alive) return 0;
      if (p.boost && p.fuel > 0) p.fuel = Math.max(0, p.fuel - 1000 / T_HZ);
      p.acc += tronSpeed(p) / T_HZ;
      const n = Math.min(3, Math.floor(p.acc));
      p.acc -= n;
      return n;
    });
    for (let r = 0; r < 3; r++) {
      const target = new Map();
      this.players.forEach((p, i) => {
        if (!p.alive || steps[i] <= r) return;
        while (p.q.length && !(p.q[0].n > p.mc + 1)) {
          // Confused: every turn goes the other way (for pages and bots alike).
          let d = p.q.shift().d;
          if (p.fx.confuse > 0) d = (d + 2) % 4;
          if (d !== p.d && d !== (p.d + 2) % 4) { p.d = d; break; }
        }
        let nx = p.x + T_DIR[p.d][0], ny = p.y + T_DIR[p.d][1];
        if (!this.open(p, nx, ny) || target.has(ny * TW + nx)) {
          // A shield (or the divine power) steers away from the crash once: to the side with more room.
          if (!((p.fx.shield || p.fx.god) && this.dodge(p, target))) {
            // What it was: the edge, the own trail, someone's trail (they cut you off), or head-on.
            const out = nx < 0 || ny < 0 || nx >= TW || ny >= TH, v = out ? 0 : this.grid[ny * TW + nx], o = target.get(ny * TW + nx);
            const by = !out && v && v !== 255 && v !== i + 1 && this.players[v - 1] ? this.players[v - 1].pid : null;
            this.crash(p, i, out ? 'wall' : o && !v ? 'head' : v === i + 1 ? 'self' : by ? 'cut' : 'trail', by || (o && o.p.pid) || null);
            if (o && o.p.alive && this.open(p, nx, ny) && !o.p.fx.god) {
              if (o.p.fx.shield) { o.p.fx.shield = 0; this.event('shield', {pid: o.p.pid}); } else this.crash(o.p, o.i, 'head', p.pid);
            }
            return;
          }
          if (!p.fx.god) { p.fx.shield = 0; this.event('shield', {pid: p.pid}); }
          nx = p.x + T_DIR[p.d][0]; ny = p.y + T_DIR[p.d][1];
        }
        target.set(ny * TW + nx, {p, i, nx, ny});
      });
      for (const [c, {p, i, nx, ny}] of target) {
        if (!p.alive) continue;
        p.x = nx; p.y = ny;
        p.mc++;
        // A gap leaves no wall; a ghost going through someone's trail leaves that trail as it is.
        const wall = !(p.fx.gap > 0) && !this.grid[c];
        if (wall) this.grid[c] = i + 1;
        p.trail.push([nx, ny, wall ? 1 : 0]);
        this.mv.push([i, nx, ny, wall ? 1 : 0]);
        // Grinding: riding right next to someone else's trail fills the boost back up.
        const [sx, sy] = T_DIR[(p.d + 1) % 4];
        for (const s of [1, -1]) {
          const gx = nx + sx * s, gy = ny + sy * s, v = gx >= 0 && gy >= 0 && gx < TW && gy < TH ? this.grid[gy * TW + gx] : 0;
          if (v && v !== i + 1) { p.fuel = Math.min(T_FUEL, p.fuel + 25); p.gr = this.tk; break; }
        }
        this.pickup(p, i, nx, ny);
      }
    }
    this.spawn();
    this.checkRound(now);
  }
  dodge(p, target) {
    const opts = [(p.d + 1) % 4, (p.d + 3) % 4].map(d => {
      let n = 0, x = p.x, y = p.y;
      while (n < 12) { x += T_DIR[d][0]; y += T_DIR[d][1]; if (!this.free(x, y) || target.has(y * TW + x)) break; n++; }
      return {d, n};
    }).sort((a, b) => b.n - a.n);
    if (!opts[0].n) return false;
    p.d = opts[0].d;
    return true;
  }
  // Items: up to two on the grid at a time, gone after 12 s; a bike takes one by riding over it.
  spawn() {
    this.items = this.items.filter(it => --it.life > 0 || (this.event('gone', {kind: it.k}), false));
    if (!this.pu || this.items.length >= 2 || --this.nextItem > 0) return;
    for (let tries = 0; tries < 40; tries++) {
      const x = 4 + Math.floor(Math.random() * (TW - 8)), y = 4 + Math.floor(Math.random() * (TH - 8));
      if (!this.free(x, y) || this.players.some(p => p.alive && Math.abs(p.x - x) + Math.abs(p.y - y) < 8) || this.items.some(it => it.x === x && it.y === y)) continue;
      this.items.push({k: Math.random() < T_GOD ? 'god' : this.pickItem(T_ITEMS), x, y, life: 288});
      this.event('spawn', {kind: this.items[this.items.length - 1].k});
      break;
    }
    this.nextItem = 120 + Math.floor(Math.random() * 96);
  }
  pickup(p, i, x, y) {
    const k = this.items.findIndex(it => it.x === x && it.y === y);
    if (k < 0) return;
    const kind = this.items[k].k, foes = this.players.filter(q => q !== p && q.alive);
    this.items.splice(k, 1);
    if (kind === 'nitro') p.fuel = T_FUEL;
    else if (kind === 'ghost') p.fx.ghost = T_FX.ghost;
    else if (kind === 'gap') p.fx.gap = T_FX.gap;
    else if (kind === 'shield') p.fx.shield = 1;
    else if (kind === 'god') p.fx.god = T_FX.god;
    else if (kind === 'brake') for (const q of foes) q.fx.brake = T_FX.brake;
    else if (kind === 'confuse') for (const q of foes) q.fx.confuse = T_FX.confuse;
    else if (kind === 'bomb') {
      // Clears every trail cell within 6 cells, and the walls in them are gone for everyone.
      for (let yy = y - 6; yy <= y + 6; yy++) for (let xx = x - 6; xx <= x + 6; xx++) {
        if (xx < 0 || yy < 0 || xx >= TW || yy >= TH || (xx - x) ** 2 + (yy - y) ** 2 > 36 || (xx === x && yy === y) || !this.grid[yy * TW + xx]) continue;
        this.grid[yy * TW + xx] = 0;
        this.er.push([xx, yy]);
      }
      if (this.er.length) {
        const gone = new Set(this.er.map(([a, b]) => a + ',' + b));
        for (const t of [...this.players.map(q => q.trail), ...this.ghosts]) for (const c of t) if (c[2] && gone.has(c[0] + ',' + c[1])) c[2] = 0;
      }
    }
    this.event('get', {pid: p.pid, kind, x, y});
  }
  crash(p, i, why = '', by = null) {
    p.alive = false;
    this.event('crash', {pid: p.pid, x: p.x, y: p.y, why, by});
  }
  full(p) { if (p.ws) try { p.ws.send(JSON.stringify({t: 'full', rd: this.rd, trails: this.players.map(q => q.trail), ghosts: this.ghosts})); } catch (e) {} }
  send(now) {
    this.broadcast({...this.base(now), tk: this.tk, rd: this.rd, win: T_WIN, pl: this.roster(), mv: this.mv, er: this.er,
      its: this.items.map(it => [it.k, it.x, it.y, it.life])});
  }
}
function tronFx() { return {ghost: 0, gap: 0, brake: 0, confuse: 0, shield: 0, god: 0}; }
// Cells per second: boost and the divine power double it, the brake halves it.
function tronSpeed(p) { return T_SPEED * ((p.boost && p.fuel > 0) || p.fx.god > 0 ? 2 : 1) * (p.fx.brake > 0 ? 0.5 : 1) * (p.od || 1); }

// ⚔️ Breach: a 1 v 1 tactical shooter in first person (drawn by the page with ray casting, like the old 3D
// shooters). An attacker and a defender, one life each per round; the attacker wins the round by taking the
// defender out or defusing the bomb (holding E for 7 s), the defender by taking the attacker out or holding on
// for 2 minutes. Sides swap every round; first to 4 rounds. Brick walls break (bullets wear them down, a breach
// charge blows a hole), concrete doesn't. Movement runs 60 times a second from each player's inputs, like
// Fußball, so the page can show its own player at once; shots are judged against where the shooter saw the
// other player (the server rewinds to that moment), as long as no wall is in between now.
const BR_MAP = [
  '################################',
  '#aoooooooooooooooooooooooooooaa#',
  '#oooooooooooooooooooooooooooooo#',
  '#ooo###=====###..###=====###ooo#',
  '#ooo#........=.....=.......#ooo#',
  '#ooo#........=.....=.......#ooo#',
  '#ooo=...........B..........=ooo#',
  '#ooo#........=.....=.......#ooo#',
  '#ooo#........====.==.......#ooo#',
  '#ooo###.######.....######=##ooo#',
  '#oooo#.....#...d......#....#ooo#',
  '#oooo=.....#..........=....=ooo#',
  '#oooo#.....=....d.....#....#ooo#',
  '#oooo#.....#..........#....#ooo#',
  '#oooo##=####===..===#####.##ooo#',
  '#ooooooooooooooooooooooooooooaa#',
  '#aooooooooooooooooooooooooooooo#',
  '################################'];
// The warehouse: a big hall with shelves, the bomb in an office in the north-east corner.
const BR_MAP2 = [
  '################################',
  '#aooooooooooooooooooooooooooooa#',
  '#oooooooooooooooooooooooooooooo#',
  '#oo####====##..#===##########oo#',
  '#oo#...........#....#.......#oo#',
  '#oo#...........#....=....B..#oo#',
  '#oo=..#####....#....=.d.....#oo#',
  '#oo=................#.......#oo#',
  '#oo#................###==#.##oo#',
  '#oo#........................=oo#',
  '#oo...#####..#####..........=oo#',
  '#oo#.............d......#...#oo#',
  '#oo#....................#....oo#',
  '#oo###===#####..###===#######oo#',
  '#oooooooooooooooooooooooooooooo#',
  '#oooooooooooooooooooooooooooooo#',
  '#aooooooooooooooooooooooooooooa#',
  '################################'];
// Both maps with what a room needs to know about them: the rows, the bomb, where each side starts.
const BR_MAPS = [{name: 'Haus', rows: BR_MAP}, {name: 'Lagerhalle', rows: BR_MAP2}].map(m => {
  const spots = ch => m.rows.flatMap((row, y) => [...row].map((c, x) => c === ch ? [x + 0.5, y + 0.5] : null)).filter(Boolean);
  return {...m, bomb: spots('B')[0], a: spots('a'), d: spots('d')};
});
const BR_W = 32, BR_H = 18, BR_R = 0.25, BR_RUN = 3, BR_WALK = 1.4, BR_WIN = 4, BR_ROUND = 120000, BR_DEFUSE = 7000, BR_PREP = 15000, BR_REINF = 2;
// Weapons: damage close up and far away (falls off over 4..16 units), shots per second, magazine, reload ms, spread.
const BR_GUN = {atk: {name: 'Sturmgewehr', dmg: 34, far: 22, rate: 9, mag: 30, reload: 2200, spread: 0.012},
  def: {name: 'MP', dmg: 26, far: 14, rate: 13, mag: 32, reload: 1900, spread: 0.02}};
// Map cells: 0 floor, 1 concrete, 2 brick (with hit points), 3 reinforced brick (steel behind it: bullets and charges do nothing).
function brGrid(rows = BR_MAP) {
  const g = new Uint8Array(BR_W * BR_H), hp = new Uint8Array(BR_W * BR_H);
  rows.forEach((row, y) => [...row].forEach((c, x) => { g[y * BR_W + x] = c === '#' ? 1 : c === '=' ? 2 : 0; hp[y * BR_W + x] = c === '=' ? 100 : 0; }));
  return {g, hp};
}
const BR_BOMB = BR_MAPS[0].bomb;
function brSolid(g, x, y) { const cx = Math.floor(x), cy = Math.floor(y); return cx < 0 || cy < 0 || cx >= BR_W || cy >= BR_H || g[cy * BR_W + cx] > 0; }
// One movement step (1/60 s) from keys k (1 W, 2 S, 4 A, 8 D, 16 walk) and view angle a; slides along walls.
function brMove(p, k, a, g) {
  let f = ((k & 1) ? 1 : 0) - ((k & 2) ? 1 : 0), s = ((k & 8) ? 1 : 0) - ((k & 4) ? 1 : 0);
  if (f && s) { f *= Math.SQRT1_2; s *= Math.SQRT1_2; }
  const v = ((k & 16) ? BR_WALK : BR_RUN) / 60, c = Math.cos(a), sn = Math.sin(a);
  const dx = (c * f - sn * s) * v, dy = (sn * f + c * s) * v;
  const r = BR_R, x0 = p.x, y0 = p.y;
  if (dx && !brSolid(g, p.x + dx + Math.sign(dx) * r, p.y - r + 0.01) && !brSolid(g, p.x + dx + Math.sign(dx) * r, p.y + r - 0.01)) p.x += dx;
  if (dy && !brSolid(g, p.x - r + 0.01, p.y + dy + Math.sign(dy) * r) && !brSolid(g, p.x + r - 0.01, p.y + dy + Math.sign(dy) * r)) p.y += dy;
  // Speed as really moved: running against a wall makes no noise.
  p.vx = (p.x - x0) * 60; p.vy = (p.y - y0) * 60;
}
// Distance along a ray to the first wall, and which cell it is (DDA through the grid).
function brRay(g, x, y, a, max = 40) {
  const dx = Math.cos(a), dy = Math.sin(a);
  let cx = Math.floor(x), cy = Math.floor(y);
  const sx = dx > 0 ? 1 : -1, sy = dy > 0 ? 1 : -1, tdx = Math.abs(1 / dx), tdy = Math.abs(1 / dy);
  let tx = dx > 0 ? (cx + 1 - x) * tdx : (x - cx) * tdx, ty = dy > 0 ? (cy + 1 - y) * tdy : (y - cy) * tdy, d = 0;
  while (d < max) {
    if (tx < ty) { d = tx; tx += tdx; cx += sx; } else { d = ty; ty += tdy; cy += sy; }
    if (cx < 0 || cy < 0 || cx >= BR_W || cy >= BR_H) return {d, cell: -1};
    const v = g[cy * BR_W + cx];
    if (v > 0) return {d, cell: cy * BR_W + cx};
  }
  return {d: max, cell: -1};
}
export class Breach extends Room {
  constructor(ctx) {
    super(ctx, 2, 1000 / 60);
    this.rd = 0;
    // The map is drawn by lot for each match (offline the page may pick one before the start).
    this.mapIx = Math.floor(Math.random() * BR_MAPS.length);
    this.map = brGrid(BR_MAPS[this.mapIx].rows);
    this.changed = [];
    this.charges = [];
    this.acc = 0;
    this.tick = 0;
    this.defuse = 0;
    this.rl = BR_ROUND;
  }
  joined(p) { Object.assign(p, {st: {shots: 0, hits: 0, dmg: 0, defuses: 0, walls: 0, clutch: 0, fast: 0}, sc: 0, side: 'atk', x: 1.5, y: 1.5, a: 0, vx: 0, vy: 0, hp: 100, alive: false, q: [], k: 0, ack: 0, owed: 0, started: false,
    mag: 30, reload: 0, cd: 0, nade: 2, rf: 0, hist: [], kills: 0}); }
  // No power-ups in Breach, so nothing to vote on: the match starts right away.
  vote(now) { this.begin(now); }
  // After a pause nobody can fire for a moment, so lining up on a frozen enemy during the pause gives nothing.
  resume(now) { super.resume(now); for (const p of this.players) p.cd = Math.max(p.cd, 350); }
  extra(p) { return {sc: p.sc, side: p.side, hp: p.hp, al: p.alive, mag: p.mag, rl: p.reload > 0, ch: p.nade, rf: p.rf, kills: p.kills}; }
  input(p, m) {
    if (m.t === 'in' && Number.isInteger(m.k) && Number.isInteger(m.i) && Number.isFinite(m.a)) {
      p.q.push([m.k & 255, m.i, m.a, Number.isFinite(m.rt) ? m.rt : 0]);
      if (p.q.length > 12) p.q.splice(0, p.q.length - 12);
    } else if (m.t === 'charge' && this.st === 'play') this.place(p);
    else if (m.t === 'charge' && this.st === 'prep') this.reinforce(p);
    else if (m.t === 'side' && this.st === 'roundover' && p.pid === this.picker && (m.v === 'atk' || m.v === 'def')) { this.pick = m.v; this.event('picked', {pid: p.pid, side: m.v}); }
    else if (m.t === 'face' && ['baby', 'gleg'].includes(m.f)) p.face = m.f;
    else if (m.t === 'full') this.full(p);
  }
  begin(now) {
    // The first attacker is drawn by lot.
    if (Math.random() < 0.5) this.players.reverse();
    this.players[0].side = 'atk';
    if (this.players[1]) this.players[1].side = 'def';
    this.round(now);
  }
  round(now) {
    this.rd++;
    const def = this.def = BR_MAPS[this.mapIx] || BR_MAPS[0];
    this.map = brGrid(def.rows);
    this.changed = [];
    this.charges = [];
    this.defuse = 0;
    this.dfOn = false;
    this.rl = BR_ROUND;
    for (const p of this.players) p.rp = [];
    for (const p of this.players) {
      const spots = p.side === 'atk' ? def.a : def.d, [x, y] = spots[Math.floor(Math.random() * spots.length)];
      const gun = BR_GUN[p.side];
      Object.assign(p, {x, y, a: Math.atan2(def.bomb[1] - y, def.bomb[0] - x), vx: 0, vy: 0, hp: 100, alive: true, q: [], owed: 0, started: false,
        mag: gun.mag, reload: 0, cd: 0, nade: p.side === 'atk' ? 2 + (p.lost || 0) : 0, rf: p.side === 'def' ? BR_REINF + (p.lost || 0) : 0, hist: [], face: null});
      // While preparing, the attacker scouts with a small drone instead of walking.
      p.drone = p.side === 'atk' ? {x, y, a: p.a, vx: 0, vy: 0} : null;
    }
    this.st = 'count';
    this.until = now + 4000;
    for (const p of this.players) this.full(p);
  }
  drop(p) {
    this.remove(p);
    if (this.st === 'vote') { if (this.players.length < 2) this.st = 'lobby'; return; }
    if (this.st === 'lobby' || this.st === 'over') return;
    this.finish(this.players[0] || null);
  }
  finish(p) {
    this.st = 'over';
    this.w = p ? p.pid : null;
    this.until = Date.now() + 12000;
    // The match in numbers, for the end screen.
    this.event('over', {pid: this.w, stats: this.players.map(q => ({pid: q.pid, name: q.name, sc: q.sc, kills: q.kills, ...q.st}))});
  }
  endRound(winner, why) {
    if (this.st !== 'play') return;
    winner.sc++;
    if (why === 'defuse') winner.st.defuses++;
    if (why === 'kill' && winner.alive && winner.hp <= 30) winner.st.clutch++;
    const took = BR_ROUND - this.rl;
    if (!winner.st.fast || took < winner.st.fast) winner.st.fast = Math.round(took);
    // Whoever lost gets a little more next round: one more charge (attack) or steel wall (defence).
    for (const q of this.players) q.lost = q !== winner ? 1 : 0;
    this.event('round', {pid: winner.pid, why, side: winner.side, hp: winner.alive ? winner.hp : 0});
    if (winner.sc >= BR_WIN) return this.finish(winner);
    this.st = 'roundover';
    // A killcam needs a little longer before the next round.
    this.until = Date.now() + (this.killcam ? 5200 : 3500);
    this.killcam = false;
    // 3:3: the deciding round. Whoever lost this one picks the side (a few seconds more to choose).
    if (this.players.length === 2 && this.players.every(q => q.sc === BR_WIN - 1)) {
      const loser = this.players.find(q => q !== winner);
      this.picker = loser.pid;
      this.pick = null;
      this.until += 4000;
      this.event('decider', {pid: loser.pid});
    }
  }
  advance(now) {
    // After the countdown, 15 s to prepare: the defender moves and reinforces walls, the attacker waits.
    if (this.st === 'count' && now >= this.until) { this.st = 'prep'; this.until = now + BR_PREP; this.event('prep', {}); }
    if (this.st === 'prep' && now >= this.until) { this.st = 'play'; for (const p of this.players) p.drone = null; this.event('go', {}); }
    if (this.st === 'roundover' && now >= this.until) {
      const picker = this.picker && this.players.find(q => q.pid === this.picker);
      if (picker) {
        // The pick (or, without one, a coin toss) decides the sides of the last round.
        const side = this.pick || (Math.random() < 0.5 ? 'atk' : 'def');
        for (const p of this.players) p.side = p === picker ? side : side === 'atk' ? 'def' : 'atk';
        this.picker = null;
      } else for (const p of this.players) p.side = p.side === 'atk' ? 'def' : 'atk';
      this.round(now);
    }
    // While counting down players may turn but not move.
    if (this.st !== 'play' && this.st !== 'prep') {
      this.acc = 0;
      for (const p of this.players) while (p.q.length) { const [, i, a] = p.q.shift(); p.ack = i; p.a = a; }
      return;
    }
    this.acc += Math.min(100, now - this.last);
    while (this.acc >= 1000 / 60 && (this.st === 'play' || this.st === 'prep')) {
      this.acc -= 1000 / 60;
      this.step(now);
    }
  }
  step(now) {
    this.tick++;
    const prep = this.st === 'prep';
    if (this.think) this.think();
    const [atk, def] = ['atk', 'def'].map(s => this.players.find(p => p.side === s));
    for (const p of this.players) {
      if (p.reload > 0 && (p.reload -= 1000 / 60) <= 0) { p.reload = 0; p.mag = BR_GUN[p.side].mag; }
      if (p.cd > 0) p.cd -= 1000 / 60;
      // Inputs as in Fußball: one per tick, two to catch up, and a late player runs on with the last input.
      const run = (k, a, rt) => {
        // Preparing: nobody shoots; the attacker stays at the spawn and steers the drone.
        if (prep && p.side === 'atk') { p.k = 0; if (p.drone && p.alive) { p.drone.a = a; brMove(p.drone, k & 31, a, this.map.g); } return; }
        // The defender may shoot while preparing, but only drones are hit then.
        if (prep) k &= ~128;
        p.k = k; p.a = a;
        if (!p.alive) return;
        brMove(p, k, a, this.map.g);
        if ((k & 64) && !p.reload && p.mag < BR_GUN[p.side].mag) p.reload = BR_GUN[p.side].reload;
        if ((k & 32) && p.cd <= 0 && !p.reload) {
          if (p.mag <= 0) p.reload = BR_GUN[p.side].reload;
          else this.fire(p, a, rt);
        }
      };
      // One input per tick on average, whatever a page sends (a short burst to catch up is fine).
      p.cr = Math.min(3, (p.cr || 0) + 1);
      if (!p.q.length) {
        if (p.started && p.owed < 8) { p.owed++; p.cr--; run(p.k & ~32, p.a, 0); }
      } else {
        p.started = true;
        // Inputs skipped to catch up still pass on a reload press (a tap may be in just one input).
        while (p.owed > 0 && p.q.length > 1) { const d = p.q.shift(); p.ack = d[1]; p.owed--; p.q[0][0] |= d[0] & 64; }
        // Inputs piled up while the server stood still: drop the oldest (a reload press goes on).
        if (p.q.length > 4 && p.cr < 2) { while (p.q.length > 4) { const d = p.q.shift(); p.ack = d[1]; p.q[0][0] |= d[0] & 64; } }
        for (let n = Math.min(p.q.length > 4 ? 2 : 1, Math.floor(p.cr)); n > 0 && p.q.length; n--) { p.cr--; const [k, i, a, rt] = p.q.shift(); p.ack = i; run(k, a, rt); }
      }
      p.hist.push([this.tick, now, p.x, p.y]);
      if (p.hist.length > 40) p.hist.shift();
      // The last 2.5 s of everyone, for the killcam.
      if (!prep) { const rp = p.rp || (p.rp = []); rp.push([Math.round(p.x * 100) / 100, Math.round(p.y * 100) / 100, Math.round(p.a * 100) / 100, (p.k & 32) && p.cd > 1000 / BR_GUN[p.side].rate - 20 ? 1 : 0]); if (rp.length > 150) rp.shift(); }
    }
    if (prep) return;
    // Breach charges go off 2 s after they are placed.
    for (const c of this.charges) if ((c.t -= 1000 / 60) <= 0) this.blow(c);
    this.charges = this.charges.filter(c => c.t > 0);
    // Defusing: the attacker stands at the bomb holding E and doesn't move.
    if (atk && atk.alive && (atk.k & 128) && Math.hypot(atk.x - this.def.bomb[0], atk.y - this.def.bomb[1]) < 1.3 && !(atk.k & (15 | 32))) {
      if (!this.dfOn) this.event('defusing', {pid: atk.pid, from: Math.round(this.defuse)});
      this.dfOn = true;
      this.defuse += 1000 / 60;
      if (this.defuse >= BR_DEFUSE) return this.endRound(atk, 'defuse');
    } else {
      // Interrupted: past halfway, half of it stays done.
      if (this.dfOn && this.defuse < BR_DEFUSE / 2) this.defuse = 0;
      if (this.dfOn && this.defuse >= BR_DEFUSE / 2) this.defuse = BR_DEFUSE / 2;
      this.dfOn = false;
    }
    this.rl -= 1000 / 60;
    if (this.rl <= 0 && def) return this.endRound(def, 'time');
  }
  // A shot: the other player is put back to where the shooter saw them (the page says which moment it drew),
  // then the bullet goes until it meets that player or a wall. Brick takes some damage and breaks after a while.
  fire(p, a, rt) {
    const gun = BR_GUN[p.side];
    p.mag--;
    p.cd = 1000 / gun.rate;
    const moving = Math.hypot(p.vx, p.vy) > 0.5, ang = a + (Math.random() * 2 - 1) * (gun.spread + (moving ? gun.spread * 2 : 0));
    const wall = brRay(this.map.g, p.x, p.y, ang);
    let hitP = null, hd = wall.d;
    if (this.st === 'play') p.st.shots++;
    if (this.st === 'prep') {
      // Preparing: a shot can only take out the attacker's drone (a small target close to the floor).
      for (const o of this.players) {
        const dr = o.drone;
        if (o === p || !dr) continue;
        const fx = dr.x - p.x, fy = dr.y - p.y, along = fx * Math.cos(ang) + fy * Math.sin(ang), off = Math.abs(-fx * Math.sin(ang) + fy * Math.cos(ang));
        if (along > 0 && off < 0.2 && along < hd) {
          o.drone = null;
          this.event('drone', {pid: p.pid, to: o.pid, x: dr.x, y: dr.y, sx: p.x, sy: p.y});
          return;
        }
      }
      if (wall.cell >= 0 && this.map.g[wall.cell] === 2) this.wear(wall.cell, 9);
      this.event('shot', {pid: p.pid, x: p.x + Math.cos(ang) * hd, y: p.y + Math.sin(ang) * hd, sx: p.x, sy: p.y});
      return;
    }
    for (const o of this.players) {
      if (o === p || !o.alive) continue;
      let ox = o.x, oy = o.y;
      if (rt) {
        // A page draws the other player at most about a quarter second in the past; older claims don't count.
        rt = Math.min(Date.now(), Math.max(rt, Date.now() - 250));
        const h = o.hist, i = h.findIndex(e => e[1] >= rt);
        if (i > 0) { const [, t0, x0, y0] = h[i - 1], [, t1, x1, y1] = h[i], k = (rt - t0) / Math.max(1, t1 - t0); ox = x0 + (x1 - x0) * k; oy = y0 + (y1 - y0) * k; }
        else if (i === 0) { ox = h[0][2]; oy = h[0][3]; }
      }
      // Distance along the ray to the circle around the other player.
      const fx = ox - p.x, fy = oy - p.y, along = fx * Math.cos(ang) + fy * Math.sin(ang), off = Math.abs(-fx * Math.sin(ang) + fy * Math.cos(ang));
      if (along > 0 && off < BR_R + 0.08 && along < hd) { hd = along; hitP = o; }
    }
    const ex = p.x + Math.cos(ang) * hd, ey = p.y + Math.sin(ang) * hd;
    if (hitP) {
      const dmg = Math.round(hd < 4 ? gun.dmg : hd > 16 ? gun.far : gun.dmg + (gun.far - gun.dmg) * (hd - 4) / 12);
      p.st.dmg += Math.min(dmg, hitP.hp);
      hitP.hp = Math.max(0, hitP.hp - dmg);
      p.st.hits++;
      this.event('hit', {pid: p.pid, to: hitP.pid, dmg, x: ex, y: ey, sx: p.x, sy: p.y});
      if (!hitP.hp) this.kill(hitP, p);
    } else {
      if (wall.cell >= 0 && this.map.g[wall.cell] === 2) this.wear(wall.cell, 9);
      this.event('shot', {pid: p.pid, x: ex, y: ey, sx: p.x, sy: p.y});
    }
  }
  wear(cell, dmg) {
    const hp = this.map.hp;
    hp[cell] = Math.max(0, hp[cell] - dmg);
    if (!hp[cell]) { this.map.g[cell] = 0; this.changed.push([cell % BR_W, Math.floor(cell / BR_W), 0]); }
  }
  kill(o, by) {
    o.alive = false;
    by.kills++;
    this.event('kill', {pid: by.pid, to: o.pid, x: o.x, y: o.y});
    // Killcam: the last 2.5 s of both, sent once (every second frame), shown while the round ends.
    const msg = JSON.stringify({t: 'replay', rd: this.rd, by: by.pid, to: o.pid, fr: this.players.map(q => ({pid: q.pid, side: q.side, f: (q.rp || []).filter((_, i) => i % 2 === 0)}))});
    for (const q of this.players) if (q.ws) try { q.ws.send(msg); } catch (e) {}
    this.killcam = true;
    this.endRound(by, 'kill');
  }
  // F: a breach charge on the brick wall right in front of the attacker; it goes off 2 s later.
  place(p) {
    if (!p.alive || p.side !== 'atk' || p.nade <= 0) return;
    const w = brRay(this.map.g, p.x, p.y, p.a, 1.2);
    if (w.cell < 0 || this.map.g[w.cell] !== 2 || this.charges.some(c => c.cell === w.cell)) return;
    p.nade--;
    this.charges.push({cell: w.cell, t: 2000, x: p.x + Math.cos(p.a) * (w.d - 0.05), y: p.y + Math.sin(p.a) * (w.d - 0.05), pid: p.pid});
    this.event('charge', {pid: p.pid, cell: w.cell, x: this.charges[this.charges.length - 1].x, y: this.charges[this.charges.length - 1].y});
  }
  // F while preparing: the defender puts steel behind the brick wall in front of them (twice per round).
  reinforce(p) {
    if (!p.alive || p.side !== 'def' || p.rf <= 0) return;
    const w = brRay(this.map.g, p.x, p.y, p.a, 1.3);
    if (w.cell < 0 || this.map.g[w.cell] !== 2) return;
    p.rf--;
    this.map.g[w.cell] = 3;
    this.changed.push([w.cell % BR_W, Math.floor(w.cell / BR_W), 3]);
    this.event('reinforce', {pid: p.pid, x: p.x + Math.cos(p.a) * (w.d - 0.05), y: p.y + Math.sin(p.a) * (w.d - 0.05)});
  }
  blow(c) {
    const cx = c.cell % BR_W, cy = Math.floor(c.cell / BR_W);
    // The brick cell and the brick right next to it along the wall go; anyone close gets hurt.
    for (const [dx, dy] of [[0, 0], [1, 0], [-1, 0], [0, 1], [0, -1]]) {
      const x = cx + dx, y = cy + dy, i = y * BR_W + x;
      if (x >= 0 && y >= 0 && x < BR_W && y < BR_H && this.map.g[i] === 2 && (dx === 0 && dy === 0 || Math.random() < 0.5)) {
        this.wear(i, 255);
        const by = this.players.find(q => q.pid === c.pid);
        if (by) by.st.walls++;
      }
    }
    this.event('boom', {x: c.x, y: c.y});
    // Hurts anyone close; a wall in between (after the blast) takes half of it.
    const dead = [];
    for (const o of this.players) {
      if (!o.alive) continue;
      const d = Math.hypot(o.x - c.x, o.y - c.y);
      if (d >= 2.5) continue;
      const a = Math.atan2(o.y - c.y, o.x - c.x), behind = brRay(this.map.g, c.x - Math.cos(a) * 0.1, c.y - Math.sin(a) * 0.1, a, d + 0.1).d < d;
      const dmg = Math.round(70 * (1 - d / 2.5) * (behind ? 0.5 : 1));
      o.hp = Math.max(0, o.hp - dmg);
      if (dmg) this.event('hit', {pid: c.pid, to: o.pid, dmg, x: o.x, y: o.y, sx: c.x, sy: c.y, boom: true});
      if (!o.hp) { o.alive = false; dead.push(o); }
    }
    if (dead.length) {
      // Both caught in the blast: the defender held on. Otherwise whoever is still standing wins.
      const def = this.players.find(q => q.side === 'def'), alive = this.players.find(q => q.alive);
      const winner = dead.length > 1 ? def : alive;
      for (const o of dead) if (winner && o !== winner) { winner.kills += o.pid !== c.pid ? 1 : 0; this.event('kill', {pid: winner.pid, to: o.pid, x: o.x, y: o.y}); }
      if (winner) this.endRound(winner, 'kill');
    }
  }
  full(p) {
    if (!p.ws) return;
    const g = [...this.map.g].map(v => v === 2 ? 2 : v);
    try { p.ws.send(JSON.stringify({t: 'full', rd: this.rd, mp: this.mapIx, g: g.join('')})); } catch (e) {}
  }
  // Can p see o right now: a clear line between them (a little slack for the body's width).
  sees(p, o) { return this.clear(p, o) || (this.st === 'prep' && !!p.drone && this.clear(p.drone, o)); }
  clear(a, o) {
    const d = Math.hypot(o.x - a.x, o.y - a.y);
    return d < 0.6 || brRay(this.map.g, a.x, a.y, Math.atan2(o.y - a.y, o.x - a.x), d + 0.5).d >= d - BR_R - 0.15;
  }
  // Each page gets the other player only while it could know where they are: in sight (and a moment after), or
  // roughly when running close by (footsteps). Otherwise just "somewhere", so a page can't show them through walls.
  send(now) {
    const r = v => Math.round(v * 1000) / 1000, open = this.st !== 'play' && this.st !== 'prep';
    const all = this.roster().map((x, i) => { const p = this.players[i]; return {...x, x: r(p.x), y: r(p.y), a: r(p.a), vx: r(p.vx), vy: r(p.vy), k: p.k, ack: p.ack, fc: p.face || null, dr: p.drone ? [r(p.drone.x), r(p.drone.y), r(p.drone.a)] : null}; });
    const msg = {...this.base(now), tk: this.tick, rd: this.rd, mp: this.mapIx, win: BR_WIN, rl: Math.max(0, Math.round(this.rl)), df: Math.round(this.defuse), dfa: !!this.dfOn,
      ch: this.charges.map(c => [r(c.x), r(c.y), Math.round(c.t)]), cells: this.changed.splice(0)};
    for (const p of this.players) {
      if (!p.ws) continue;
      const seen = p.seen || (p.seen = {});
      const pl = all.map((x, i) => {
        const o = this.players[i];
        if (o === p || open || !o.alive || !p.alive) return x;
        if (this.sees(p, o)) seen[o.pid] = now;
        if (now - (seen[o.pid] || 0) < 300) return x;
        const hid = {pid: x.pid, name: x.name, on: x.on, aw: x.aw, ps: x.ps, sc: x.sc, side: x.side, al: x.al, ch: x.ch, kills: x.kills, dr: x.dr, hid: true};
        // Running within 12 units: where the steps are, give or take a little.
        const d = Math.hypot(o.x - p.x, o.y - p.y);
        if (Math.hypot(o.vx, o.vy) > BR_WALK + 0.3 && d < 12) {
          const b = o.blur || (o.blur = {at: 0});
          if (now - b.at > 350) Object.assign(b, {at: now, x: o.x + (Math.random() - 0.5) * 1.2, y: o.y + (Math.random() - 0.5) * 1.2});
          return {...hid, hid: 'step', x: r(b.x), y: r(b.y)};
        }
        return hid;
      });
      try { p.ws.send(JSON.stringify({...msg, pl})); } catch (e) {}
    }
  }
}
