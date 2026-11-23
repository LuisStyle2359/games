// Fußball owner-assist vs a bot, with simulated network delay; reads the live code from index.html. LVL=human with OPP=<json> plays a custom bot.
// env: FILE (default /home/user/games/index.html), LIN/LOUT (ticks one way), JIT (extra random ticks), TICKS, SIDE (0/1), RUNS, LVL
import fs from 'fs';
const src = fs.readFileSync(process.env.FILE || '/home/user/games/index.html', 'utf8').split('\n');
const at = (re, from = 0) => { for (let i = from; i < src.length; i++) if (re.test(src[i])) return i; throw new Error('no ' + re); };
const seg = (a, b) => src.slice(a, b).join('\n');
const s0 = at(/^const SG_LOCAL = \(\(\) => \{/), s1 = at(/^\}\)\(\);/, s0);
const c0 = at(/^const BZ_W = 760/), c1 = at(/^function bzTicks/);
const a0 = at(/^\/\/ The owner's trick in Fußball/), a1 = at(/^function bzSnapshot/), a2 = at(/^\/\* Fußball bots/), a3 = at(/^\/\* Neon Rush bots/);
const code = [seg(c0, c1), seg(a0, a1), seg(a1, at(/^}/, a1) + 1), seg(a2, a3), seg(s0, s1 + 1),
  'return {BZ_BOT, SG_LOCAL, bzAhead, bzAssistKeys, bzSnapshot, bzMove, bzBounds, bzBrain, BZ_ASSIST};'].join('\n');
const LIN = +(process.env.LIN ?? 3), LOUT = +(process.env.LOUT ?? 3), JIT = +(process.env.JIT ?? 0), TICKS = +(process.env.TICKS ?? 10800);
const RUNS = +(process.env.RUNS ?? 6), LVL = process.env.LVL || 'profi';
let simMs = 0;
const me = {id: 'A'};
const env = {me, performance: {now: () => simMs}, pgMs: () => simMs, pgAssistOn: true, pgAssistHeld: true};
const mod = new Function(...Object.keys(env), code)(...Object.values(env));
const {SG_LOCAL, bzAssistKeys, bzSnapshot, bzMove, bzBounds, bzBrain} = mod;
if (process.env.OPP) mod.BZ_BOT.human = JSON.parse(process.env.OPP);
if (process.env.AV) Object.assign(mod.BZ_ASSIST, JSON.parse(process.env.AV));

function run(side) {
  const R = new SG_LOCAL.Kick({});
  R.solo = true;
  const mk = (pid, bot) => ({pid, name: pid, ws: {send() {}, close() {}}, left: 0, away: false, pauses: 0, bot});
  const A = mk('A', false), B = mk('B', true);
  if (side) { R.players.push(B); R.joined(B); R.players.push(A); R.joined(A); }
  else { R.players.push(A); R.joined(A); R.players.push(B); R.joined(B); }
  A.team = side; B.team = 1 - side;
  R.pu = !!process.env.PU;
  R.think = bzBrain(R, LVL);
  R.kickoff(Math.random() < 0.5 ? 0 : 1);
  R.st = 'play';
  const g = {game: 'kick', sg: {s: null, me: null, hist: [], vis: {x: 0, y: 0}, keys: 0, seq: 0, rtt: (LIN + LOUT) * 1000 / 60, off: LOUT * 1000 / 60, offs: []}};
  const toS = [], toC = [];
  let goalT = 0, t0 = 0;
  for (let t = 0; t < TICKS; t++) {
    simMs = t * 1000 / 60;
    // client
    const sg = g.sg;
    while (toC.length && toC[0][0] <= t) { const m = toC.shift()[1]; sg.s = m; bzSnapshot(g, m); }
    if (sg.s && sg.s.st === 'play' && sg.me) {
      const k = bzAssistKeys(g);
      const i = ++sg.seq;
      toS.push([t + LIN + (JIT ? Math.floor(Math.random() * (JIT + 1)) : 0), {t: 'in', k, i}]);
      toS.sort((a, b) => a[0] - b[0]);
      sg.hist.push([i, k]);
      bzMove(sg.me, k); bzBounds(sg.me, sg.s.ko);
      if (sg.hist.length > 240) sg.hist.splice(0, sg.hist.length - 240);
    } else sg.assist = null;
    // server
    while (toS.length && toS[0][0] <= t) R.input(A, toS.shift()[1]);
    if (R.st === 'goal') {
      if (++goalT > 60) { goalT = 0; R.kickoff(R.lastGoal === 0 ? 1 : 0); R.st = 'play'; }
    }
    if (R.st === 'play' || R.st === 'goal') R.step();
    const m = JSON.parse(JSON.stringify(R.state(simMs)));
    toC.push([t + LOUT + (JIT ? Math.floor(Math.random() * (JIT + 1)) : 0), m]);
    toC.sort((a, b) => a[0] - b[0]);
  }
  return [R.sc[side], R.sc[1 - side]];
}
let f = 0, a = 0;
for (let r = 0; r < RUNS; r++) {
  const side = process.env.SIDE !== undefined ? +process.env.SIDE : r % 2;
  const [x, y] = run(side);
  f += x; a += y; if (process.env.PER) process.stdout.write(x + ":" + y + " ");
}
console.log(JSON.stringify({lin: LIN, lout: LOUT, jit: JIT, for: f, against: a}));
