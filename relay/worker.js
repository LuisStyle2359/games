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
    return env.HUB.get(env.HUB.idFromName('main')).fetch(req);
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
