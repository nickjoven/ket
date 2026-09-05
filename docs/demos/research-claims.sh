#!/usr/bin/env bash
# Domain demo: research claims.
#
# A measurement, a derivation that is *grounded* in it, a hypothesis that is
# merely *proposed* from it, a synthesis, and then a correction — recorded as
# a new node that supersedes the old one, never as an overwrite. The graph
# at the end shows the epistemic edge kinds; the log shows every event.
#
# Needs `ket` on PATH. Dolt is optional.
set -euo pipefail

WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
export KET_HOME="$WORK/.ket"; cd "$WORK"
jget() { sed -n "s/.*\"$1\": *\"\([0-9a-f]*\)\".*/\1/p" | head -1; }
say()  { printf '\n\033[1m%s\033[0m\n' "$*"; }
run()  { printf '\033[2m$ %s\033[0m\n' "$*"; "$@"; }
node() { ket --json dag create "$@" | jget node_cid; }

ket init >/dev/null

say "1. A measurement is an irreducible input. Nothing derives it; it grounds things."
M="$(node "pendulum: T = 2.006 s at L = 1.000 m (lab B, 2026-09-04)" --kind memory --agent human)"
run ket dag show "$M" | head -3

say "2. Two agents read it. One derives; one proposes."
D="$(node "derived: g = 4π²L/T² = 9.81 m/s²" \
        --kind reasoning --agent codex --parent "$M" --edge-kind grounds)"
H="$(node "hypothesis: the 0.3% shortfall vs. 9.84 is air drag on the bob" \
        --kind reasoning --agent claude --parent "$M" --edge-kind proposes)"
run ket dag lineage "$H"

say "3. A synthesis merges both. It is a node with two parents, not a rewrite of either."
S="$(ket --json merge "g = 9.81 ± 0.02; drag hypothesis untested" \
        --parents "$D" "$H" --agent human | jget node_cid)"
run ket dag show "$S" | grep -A2 Parents

say "4. The hypothesis turns out wrong. The correction is a NEW node whose edge to the old one says so."
C="$(node "retracted: shortfall was a timing offset (stopwatch latency), not drag" \
        --kind reasoning --agent claude --parent "$H:supersedes" --parent "$M:grounds")"
run ket dag show "$C" | grep -A2 Parents
OLD="$(ket --json dag show "$H" | jget output_cid)"
run ket get "$OLD"; echo
echo "   ^ the superseded claim is still there, byte for byte. Resolution is an event, not a deletion."

say "5. The graph. Bold = grounds, dashed = proposes, plain = derives, circle-head = supersedes."
run ket graph --format mermaid

say "6. The log. Every event above, in order, append-only."
run ket log
