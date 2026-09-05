#!/usr/bin/env bash
# Domain demo: a repository audit as a DAG (sieve).
#
# Reviewers propose findings against the audited tip; verifiers confirm or
# refute them with evidence; a partial verdict writes a corrected finding
# that supersedes the original. The ledger is rendered from the graph.
#
# Runs with sieve's deterministic fake agent, so it needs no API and costs
# nothing. Point SIEVE_AGENT at a real one to audit for real:
#   SIEVE_AGENT="claude -p --output-format json" ./docs/demos/repo-audit.sh
#
# Needs `ket` on PATH and a sieve checkout (SIEVE_DIR, default ../sieve).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SIEVE_DIR="${SIEVE_DIR:-$HERE/../../../sieve}"
SIEVE="python3 $SIEVE_DIR/sieve.py"
AGENT="${SIEVE_AGENT:-python3 $SIEVE_DIR/tests/fake_agent.py}"
[ -f "$SIEVE_DIR/sieve.py" ] || { echo "sieve not found at $SIEVE_DIR (set SIEVE_DIR)" >&2; exit 2; }

WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
export KET_HOME="$WORK/.ket"; cd "$WORK"
say()  { printf '\n\033[1m%s\033[0m\n' "$*"; }
run()  { printf '\033[2m$ %s\033[0m\n' "$*"; "$@"; }

# A tiny repo whose README overstates itself.
mkdir repo && cd repo && git init -q
printf '# demo\n\nA tiny repo.\nCI on every push.\n' > README.md
printf 'region=us-east-1\nkey=AKIA_PLACEHOLDER\n' > config.example
git -c user.name=demo -c user.email=demo@x add . && git -c user.name=demo -c user.email=demo@x commit -qm init
cd ..
ket init >/dev/null

say "1. Audit. Three reviewers in parallel, then adversarial verification of every material finding."
$SIEVE --json run repo --dims "$SIEVE_DIR/tests/dims.json" --agent "$AGENT" --jobs 3 > run.json
cat run.json
ROOT="$(sed -n 's/.*"root": *"\([0-9a-f]*\)".*/\1/p' run.json)"

say "2. The ledger is not written. It is rendered from the graph."
$SIEVE ledger "$ROOT" | sed '/^## Graph/,$d'

say "3. The graph. proposes = dashed, confirms = green, refutes = cross-head, supersedes = circle-head, grounds = bold."
$SIEVE ledger "$ROOT" --format mermaid

say "4. The repo changes; audit again, chained to the first; diff by finding content."
cd repo && mkdir -p .github/workflows && echo 'on: push' > .github/workflows/ci.yml
git -c user.name=demo -c user.email=demo@x add . && git -c user.name=demo -c user.email=demo@x commit -qm "add ci"; cd ..
printf '{"context":"","dimensions":[{"key":"tests","prompt":"x"}]}' > dims2.json
ROOT2="$($SIEVE --json run repo --dims dims2.json --agent "$AGENT" --parent "$ROOT" 2>/dev/null | sed -n 's/.*"root": *"\([0-9a-f]*\)".*/\1/p')"
run $SIEVE diff "$ROOT" "$ROOT2"
run ket dag lineage "$ROOT2"

say "5. Audit the audit."
if command -v dolt >/dev/null 2>&1; then run ket repair >/dev/null; run ket verify-projection; fi
run ket log
