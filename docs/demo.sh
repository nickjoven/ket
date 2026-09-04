#!/usr/bin/env bash
# ket + catbus in one sitting: context, tokens, retrieval, generation.
#
# Runs against a throwaway store. Needs `ket` and `catbus` on PATH; Dolt is
# optional (the drift gate is skipped without it). Pass a source file to use
# as the "context" — default is ket-dag's lib.rs from this checkout.
#
#   ./docs/demo.sh                 # uses ket-dag/src/lib.rs
#   ./docs/demo.sh path/to/file.rs
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${1:-$HERE/../ket-dag/src/lib.rs}"
[ -f "$SRC" ] || { echo "no such file: $SRC" >&2; exit 2; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
export KET_HOME="$WORK/.ket"
mkdir -p "$WORK/src" && cp "$SRC" "$WORK/src/dag.rs"
cd "$WORK"

# Pull one hex field out of `--json` output (one key per line, pretty-printed).
jget() { sed -n "s/.*\"$1\": *\"\([0-9a-f]*\)\".*/\1/p" | head -1; }
say()  { printf '\n\033[1m%s\033[0m\n' "$*"; }
run()  { printf '\033[2m$ %s\033[0m\n' "$*"; "$@"; }

say "0. A store. Nothing in it yet."
run ket init

say "1. CONTEXT is content-addressed: same bytes, same ID, every time."
run ket put src/dag.rs
run ket put src/dag.rs
CID="$(ket --json put src/dag.rs | jget cid)"
run ket verify "$CID"

say "2. GENERATION leaves a trail: what was produced, by whom, from what."
N1="$(ket --json dag create "reviewed dag.rs: lineage() walks parents depth-first; no cycle guard" \
        --kind reasoning --agent claude | jget node_cid)"
N2="$(ket --json dag create "added visited-set to lineage(); cycle now terminates" \
        --kind code --agent codex --parent "$N1" | jget node_cid)"
run ket dag lineage "$N2"
run ket log

say "3. RETRIEVAL is by hash, and it knows when it is stale."
run sh -c "ket get $CID | head -3"
if command -v dolt >/dev/null 2>&1; then
  run ket track add src/dag.rs --agent claude
  run ket drift
  echo "// a later edit nobody told the model about" >> src/dag.rs
  set +e; run ket drift; RC=$?; set -e
  echo "exit=$RC   <- non-zero, so 'ket drift && agent' refuses to reason on stale context"
else
  echo "(track/drift need Dolt; skipping the drift gate)"
fi

say "4. TOKENS: hand the next model a receipt, not the transcript."
P1="$(catbus --json pack --title "dag.rs review" \
        --summary "lineage() cycle fix landed (see parent). Next: test lineage_bounded with a cycle." \
        --agent claude --file src/dag.rs --cdom --parent "$N2" | jget node_cid)"
run catbus handoff "$P1"
run catbus stats "$P1"

say "5. The second model picks up exactly there — and leaves its own receipt."
run catbus guard --cid "$P1" -- true
run catbus unpack "$P1" --out-dir ./handoff
run sh -c "cmp src/dag.rs handoff/dag.rs && echo 'unpacked bytes == packed bytes'"
echo "// test: lineage_bounded terminates on a cycle" >> src/dag.rs
P2="$(catbus --json pack --title "dag.rs tests" \
        --summary "lineage_bounded cycle test added; all green." \
        --agent codex --file src/dag.rs --cdom --parent "$P1" | jget node_cid)"
run catbus diff "$P1" "$P2"
run ket dag lineage "$P2"

say "6. SQL is a projection. catbus wrote blobs only; replay them into Dolt and audit."
if command -v dolt >/dev/null 2>&1; then
  run ket repair
  run ket verify-projection
fi
run ket status

say "7. The whole thing as a graph. Paste this into a \`\`\`mermaid fence and GitHub draws it."
run ket graph --format mermaid

say "Done. Everything above lives in $KET_HOME: blobs by hash, one append-only log."
