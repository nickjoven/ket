#!/usr/bin/env bash
# Domain demo: parallel multi-agent review.
#
# One handoff fans out to three reviewers that run CONCURRENTLY against the
# same store, then fans back in through a merge node. Nothing is locked and
# nothing conflicts, because nothing is ever overwritten: every write is a
# new blob named by its own hash. The graph is a diamond; the log shows the
# interleaved writes landing intact.
#
# Needs `ket` and `catbus` on PATH. Dolt is optional (repair/verify need it).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${1:-$HERE/../../ket-cas/src/lib.rs}"
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
export KET_HOME="$WORK/.ket"; mkdir -p "$WORK/src"; cp "$SRC" "$WORK/src/cas.rs"; cd "$WORK"
jget() { sed -n "s/.*\"$1\": *\"\([0-9a-f]*\)\".*/\1/p" | head -1; }
say()  { printf '\n\033[1m%s\033[0m\n' "$*"; }
run()  { printf '\033[2m$ %s\033[0m\n' "$*"; "$@"; }

ket init >/dev/null

say "1. One handoff. Three reviewers will start from it — at the same time."
H="$(catbus --json pack --title "review cas.rs" \
      --summary "Review src/cas.rs for security, performance, and style. Report findings as reasoning nodes." \
      --agent orchestrator --file src/cas.rs --cdom | jget node_cid)"
run catbus stats "$H"

say "2. Fan out. Each reviewer is a separate process writing to the same store. No locks."
review() { # <agent> <finding>
  sleep "0.0$((RANDOM % 9))"                       # jitter, so the writes really interleave
  ket --json dag create "$2" --kind reasoning --agent "$1" --parent "$H" | jget node_cid
}
review security "no path traversal: blob path is root.join(hex); hex is validated by hash length" > r1 &
review perf     "put() hashes then writes; dedup check happens before the write — no wasted IO"   > r2 &
review style    "no path traversal: blob path is root.join(hex); hex is validated by hash length" > r3 &
wait
R1="$(cat r1)"; R2="$(cat r2)"; R3="$(cat r3)"
echo "security -> $R1"; echo "perf     -> $R2"; echo "style    -> $R3"

say "3. Two reviewers wrote the identical finding. Two nodes, ONE blob — the content deduplicated itself."
O1="$(ket --json dag show "$R1" | jget output_cid)"; O3="$(ket --json dag show "$R3" | jget output_cid)"
echo "security output_cid: $O1"; echo "style    output_cid: $O3"
[ "$O1" = "$O3" ] && echo "same bytes, same CID: stored once."

say "4. Fan in. The merge node names all three parents. Then the orchestrator hands off again."
S="$(ket --json merge "3 reviews in; 2 agree on traversal safety; perf: dedup-before-write confirmed. Ship." \
      --parents "$R1" "$R2" "$R3" --agent orchestrator | jget node_cid)"
H2="$(catbus --json pack --title "review complete" \
      --summary "Reviews merged (see parent). Next: open PR." \
      --agent orchestrator --parent "$S" | jget node_cid)"
run ket dag lineage "$H2"

say "5. The interleaved log. Concurrent appends, one line each, nothing lost."
run ket log

if command -v dolt >/dev/null 2>&1; then
  say "6. Project it into SQL and audit. Concurrency cost nothing here either."
  run ket repair
  run ket verify-projection
fi

say "7. The graph: a diamond. Handoff at the bottom, three reviewers, one merge, next handoff."
run ket graph --format mermaid
