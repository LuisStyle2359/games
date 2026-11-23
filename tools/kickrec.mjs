// Fetches the recorded Fußball games (see bzRecSend in index.html) from our relay and writes one JSON file per
// game into the given folder: {head, fr} with fr the frames as recorded (columns in head.cols).
// Usage: node tools/kickrec.mjs <out-dir>   (CLEAR=<slot> deletes that slot's recording instead)   (needs `npm i ws https-proxy-agent` in a scratch folder behind a proxy)
import fs from 'fs';
import zlib from 'zlib';
import {createRequire} from 'module';
const require = createRequire(process.env.NODE_MODS ? process.env.NODE_MODS + '/' : import.meta.url);
const URL = 'wss://games.luisumbescheid1234.workers.dev/', ROOT = 'dv-2563318804-chat/', out = process.argv[2] || 'recs';
let WS = globalThis.WebSocket, opts = undefined;
try {
  const ws = require('ws');
  WS = ws;
  const proxy = process.env.HTTPS_PROXY || process.env.https_proxy;
  if (proxy) { const {HttpsProxyAgent} = require('https-proxy-agent'); opts = {agent: new HttpsProxyAgent(proxy)}; }
} catch (e) {}
const enc = new TextEncoder(), dec = new TextDecoder();
const str = s => { const b = enc.encode(s); return [b.length >> 8, b.length & 255, ...b]; };
const varLen = n => { const o = []; do { let b = n % 128; n = Math.floor(n / 128); if (n) b |= 128; o.push(b); } while (n); return o; };
const pkt = (h, body) => Buffer.from([h, ...varLen(body.length), ...body]);
const ws = opts ? new WS(URL, 'mqtt', opts) : new WS(URL, 'mqtt');
ws.binaryType = 'arraybuffer';
const parts = {};
let buf = Buffer.alloc(0);
ws.onopen = () => ws.send(pkt(0x10, [...str('MQTT'), 4, 0x02, 0, 30, ...str('fetch' + Date.now().toString(36))]));
ws.onmessage = e => {
  buf = Buffer.concat([buf, Buffer.from(e.data)]);
  for (;;) {
    let n = 0, mul = 1, i = 1, b;
    do { if (i >= buf.length) return; b = buf[i++]; n += (b & 127) * mul; mul *= 128; } while (b & 128);
    if (buf.length < i + n) return;
    const head = buf[0], body = buf.subarray(i, i + n);
    buf = buf.subarray(i + n);
    if (head >> 4 === 2 && process.env.CLEAR) {
      for (let j = 0; j < 16; j++) ws.send(pkt(0x31, [...str(ROOT + 'rec/kick/' + process.env.CLEAR + '/' + j)]));
      console.log('cleared slot', process.env.CLEAR);
    } else if (head >> 4 === 2) ws.send(pkt(0x82, [0, 1, ...str(ROOT + 'rec/kick/+/+'), 0]));
    if (head >> 4 === 3) {
      const tl = (body[0] << 8) | body[1], topic = dec.decode(body.subarray(2, 2 + tl)), p = dec.decode(body.subarray(2 + tl));
      const m = topic.match(/rec\/kick\/(\d+)\/(\d+)$/);
      if (m && p) try { (parts[m[1]] = parts[m[1]] || []).push(JSON.parse(p)); } catch (err) {}
    }
  }
};
ws.onerror = e => { console.error('error', e.message || e); };
setTimeout(() => {
  ws.close();
  fs.mkdirSync(out, {recursive: true});
  let n = 0;
  for (const [slot, ps] of Object.entries(parts)) {
    const first = ps.find(p => p.i === 0);
    if (!first) continue;
    const mine = ps.filter(p => p.id === first.id).sort((a, b) => a.i - b.i);
    if (mine.length !== first.n) { console.error('slot', slot, 'incomplete'); continue; }
    try {
      const {head, d} = JSON.parse(zlib.inflateRawSync(Buffer.from(mine.map(p => p.d).join(''), 'base64')).toString());
      const prev = [], fr = d.map(f => f.map((v, c) => (prev[c] = (prev[c] || 0) + v)));
      fs.writeFileSync(`${out}/kick-${new Date(head.at).toISOString().replace(/[:.]/g, '-')}.json`, JSON.stringify({head, fr}));
      console.log(slot, new Date(head.at).toISOString(), 'v' + head.ver, 'score', head.sc.join(':'), 'me team', head.teams[head.me], 'frames', fr.length, 'bot ticks', head.bot, 'rtt', head.rtt);
      n++;
    } catch (err) { console.error('slot', slot, err.message); }
  }
  console.log(n, 'games');
  process.exit(0);
}, +(process.env.WAIT || 6000));
