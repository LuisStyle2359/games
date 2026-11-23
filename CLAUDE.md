# Notes for Claude

The site is the single file `index.html` (edit it directly). `relay/worker.js` is the Cloudflare relay.

## "prognose": improve the Fußball owner-bot from real games

The owner's page records every online Fußball game (only when the owner trick is on) and uploads it to the
relay at the end (`bzRecTick` / `bzRecSend` in index.html, 30 slots used in turn). When the user says
"prognose":

1. Fetch the recordings (in a scratch folder: `npm i ws https-proxy-agent`, then
   `NODE_MODS=<that>/node_modules node tools/kickrec.mjs <out-dir>`). Columns are in `head.cols`;
   `bot` = 1 while the owner held 1 (the bot played), `lag` = unacknowledged inputs.
2. Look at how goals against happened while the bot played, and how the human opponents move and shoot.
3. Improve `bzAssistKeys` (and its helpers), check it with `tools/kicksim.mjs` (LIN/LOUT = ping in ticks,
   LVL=human with OPP fitted to what the recordings show), compare against the previous version.
4. Release: bump `APP_VERSION`, commit, push. Old recordings need no clearing, new games replace them.
