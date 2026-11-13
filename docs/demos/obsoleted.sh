#!/usr/bin/env bash
# Benchmark demo: naive chunk-RAG vs a ket substrate, on the HTTP RFC series.
#
# Four scripted walkers read overlapping slices of 13 HTTP RFCs (1945 ..
# 9112). A confident wrong summary is written and built on, the 2022 RFCs
# arrive after the store was built, and someone edits a shared copy. Then 44
# questions are answered from a naive chunk index and from the ket store,
# and scored against RFC 9110. No LLM and no API key: every score is a
# deterministic function of the pinned corpus. See obsoleted/README.md.
#
# Needs `ket`, `curl` and `python3` on PATH. Dolt is not used.
#
#   ./docs/demos/obsoleted.sh          mechanics tier: scripted walkers, no API calls
#   ./docs/demos/obsoleted.sh --live   live tier: Claude API walkers and reader; needs
#                                      `pip install anthropic` and an API key, and
#                                      prints a cost estimate and stops unless
#                                      OBSOLETED_LIVE_YES=1 (see obsoleted/live.py)
# The RFC texts are downloaded once from www.rfc-editor.org into
# ${KET_DEMO_CACHE:-~/.cache/ket-demo}/rfc and checked against the BLAKE3
# CIDs pinned in obsoleted/corpus.tsv before anything reads them.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CACHE="${KET_DEMO_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/ket-demo}/rfc"
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
mkdir -p "$CACHE"

for tool in ket curl python3; do
  command -v "$tool" >/dev/null || { echo "obsoleted: '$tool' is not on PATH" >&2; exit 2; }
done

# Pin check uses a throwaway store, so the corpus arrives by CID.
export KET_HOME="$WORK/.pin"; ket init >/dev/null
printf '\033[1m%s\033[0m\n' "0. Corpus: 13 RFCs, each verified against its pinned BLAKE3 CID"
grep -v '^#' "$HERE/obsoleted/corpus.tsv" | while IFS=$'\t' read -r n cid _bytes title _obs; do
  f="$CACHE/rfc$n.txt"
  if [ ! -s "$f" ]; then
    curl -fsS "https://www.rfc-editor.org/rfc/rfc$n.txt" -o "$f.part" && mv "$f.part" "$f"
  fi
  got="$(ket put "$f")"
  if [ "$got" != "$cid" ]; then
    echo "obsoleted: rfc$n.txt hashes to $got, pinned $cid; refusing to run" >&2
    exit 1
  fi
  printf '\033[2m   RFC %-5s %s  %s\033[0m\n' "$n" "${cid:0:12}" "$title"
done

mkdir -p "$WORK/run"
if [ "${1:-}" = "--live" ]; then
  # The live tier imports the Anthropic SDK (pip install anthropic), so it
  # runs without -I, which would hide a user-site install.
  python3 "$HERE/obsoleted/obsoleted.py" "$CACHE" "$WORK/run" --live
else
  python3 -I "$HERE/obsoleted/obsoleted.py" "$CACHE" "$WORK/run"
fi
if [ -n "${KEEP:-}" ]; then
  mkdir -p "$KEEP" && cp -r "$WORK/run/." "$KEEP/"
  printf '\033[2m   store and results copied to %s\033[0m\n' "$KEEP"
fi
