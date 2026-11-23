#!/bin/bash
# Usage: tools/kickeval.sh <assist overrides json>  (R=runs). Goal difference of the owner-bot against three opponents:
# a quick person-like bot at 30 ms ping, a slower one at 100 ms with jitter, and the Profi bot at 100 ms.
cfg="$1"; H6='{"react":6,"err":0.1,"look":18,"kick":0.95,"speed":1,"items":0.6,"lineUp":0.65}'; H14='{"react":14,"err":0.15,"look":18,"kick":0.9,"speed":0.9,"items":0.5,"lineUp":0.6}'
a=$(LIN=1 LOUT=1 LVL=human OPP=$H6 AV="$cfg" RUNS=${R:-100} node "$(dirname "$0")/kicksim.mjs")
b=$(LIN=3 LOUT=3 JIT=2 LVL=human OPP=$H14 AV="$cfg" RUNS=${R:-100} node "$(dirname "$0")/kicksim.mjs")
c=$(LIN=3 LOUT=3 AV="$cfg" RUNS=${R:-100} node "$(dirname "$0")/kicksim.mjs")
node -e "const r=[$a,$b,$c];const d=r.map(x=>x.for-x.against);console.log(d.reduce((s,x)=>s+x,0), JSON.stringify(d), process.argv[1])" "$cfg"
