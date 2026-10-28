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
const W = 640, H = 360, PH = 70, PW = 10, PX = 24, BR = 7, WIN = 7, V0 = 380, VMAX = 1150, PADDLE_V = 1600, TICK = 1000 / 60;

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
    this.n = 0;
    this.dir = Math.random() < 0.5 ? -1 : 1;
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
    if (this.st === 'wait' && this.seats[0] && this.seats[1]) this.serve('count', 3000);
    if (!this.timer) this.timer = setInterval(() => this.tick(), TICK);
    return new Response(null, {status: 101, webSocket: client});
  }

  onMessage(i, data) {
    let msg;
    try { msg = JSON.parse(data); } catch (e) { return; }
    if (msg.t === 'm' && Number.isFinite(msg.y)) this.pad[i].ty = Math.max(PH / 2, Math.min(H - PH / 2, msg.y));
    else if (msg.t === 'ping' && Number.isFinite(msg.c)) { try { this.seats[i].send(JSON.stringify({t: 'pong', c: msg.c})); } catch (e) {} }
    else if (msg.t === 'quit' && this.st !== 'over') this.finish(2 - i);
  }

  serve(st, ms) {
    this.st = st;
    this.until = Date.now() + ms;
    Object.assign(this.b, {x: W / 2, y: H / 2, vx: 0, vy: 0});
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

  tick() {
    const now = Date.now();
    if (this.st === 'over') {
      if (now > this.until || (!this.seats[0] && !this.seats[1])) { clearInterval(this.timer); this.timer = 0; }
    } else if (this.st !== 'wait') {
      for (let i = 0; i < 2; i++) if (!this.seats[i] && now - this.left[i] > 10000) this.finish(2 - i);
      if (this.st !== 'over' && this.seats[0] && this.seats[1]) {
        if ((this.st === 'count' || this.st === 'goal') && now >= this.until) { this.st = 'play'; this.launch(); }
        if (this.st === 'play') this.physics(TICK / 1000);
      }
    } else if (!this.seats[0] && !this.seats[1] && now - Math.max(...this.left) > 60000) {
      clearInterval(this.timer);
      this.timer = 0;
    }
    this.broadcast(now);
  }

  physics(dt) {
    const steps = 4, h = dt / steps, b = this.b, maxStep = PADDLE_V * h;
    for (let k = 0; k < steps; k++) {
      for (const p of this.pad) p.y += Math.max(-maxStep, Math.min(maxStep, p.ty - p.y));
      const px = b.x;
      b.x += b.vx * h;
      b.y += b.vy * h;
      if (b.y < BR) { b.y = BR; b.vy = Math.abs(b.vy); }
      if (b.y > H - BR) { b.y = H - BR; b.vy = -Math.abs(b.vy); }
      for (let i = 0; i < 2; i++) {
        const face = i === 0 ? PX + PW / 2 : W - PX - PW / 2, s = i === 0 ? -1 : 1;
        const crossed = s < 0 ? b.vx < 0 && px - BR >= face - 1 && b.x - BR < face : b.vx > 0 && px + BR <= face + 1 && b.x + BR > face;
        if (!crossed || Math.abs(b.y - this.pad[i].y) > PH / 2 + BR) continue;
        // Where the ball meets the paddle sets the angle; every return is a bit faster.
        const rel = Math.max(-1, Math.min(1, (b.y - this.pad[i].y) / (PH / 2))), a = rel * Math.PI / 3;
        const v = Math.min(VMAX, Math.hypot(b.vx, b.vy) * 1.06);
        b.vx = -s * v * Math.cos(a);
        b.vy = v * Math.sin(a);
        b.x = face - s * BR;
      }
      if (b.x < -BR) return this.point(1);
      if (b.x > W + BR) return this.point(0);
    }
  }

  point(scorer) {
    this.sc[scorer]++;
    if (this.sc[scorer] >= WIN) return this.finish(scorer + 1);
    this.dir = scorer === 0 ? 1 : -1;
    this.serve('goal', 1200);
  }

  broadcast(now) {
    const r = v => Math.round(v);
    const s = JSON.stringify({t: 's', ts: now, st: this.st, c: Math.max(0, this.until - now), w: this.w, sc: this.sc, names: this.names,
      on: [!!this.seats[0], !!this.seats[1]], p: [r(this.b.x), r(this.b.y), r(this.b.vx), r(this.b.vy)],
      m: [[PX, r(this.pad[0].y)], [W - PX, r(this.pad[1].y)]]});
    for (const ws of this.seats) if (ws) try { ws.send(s); } catch (e) {}
  }
}
