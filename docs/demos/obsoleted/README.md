# obsoleted: naive chunk-RAG vs a ket substrate, on the HTTP RFCs

```sh
./docs/demos/obsoleted.sh          # needs ket, curl, python3; ~10 s after the first download
KEEP=/tmp/obsoleted ./docs/demos/obsoleted.sh   # keep the store and results.json
```

Four agents walk a real document series and build shared knowledge two ways:
a naive chunk index, and a ket store. Then the same things go wrong in both,
and 44 questions are answered from each and scored against an answer key.

This is the **mechanics tier**: the walkers are scripts, so there is no LLM
and no API key. Every score is a deterministic function of the pinned corpus,
and two runs print the same numbers. Only node CIDs differ between runs,
because nodes carry timestamps.

## The corpus

Thirteen HTTP RFCs, 1996 to 2022: 1945 (HTTP/1.0), 2068, 2616, 7230–7235,
7538, 9110, 9111, 9112. They are downloaded once from www.rfc-editor.org and
**checked against the BLAKE3 CIDs in [`corpus.tsv`](corpus.tsv)** before
anything reads them. A mismatch stops the run. The `obsoletes` relation
comes from the RFC Editor's own metadata (`rfcN.json`), so the supersession
graph is the RFC series' own, not one written for the demo.

The answer key ([`answers.tsv`](answers.tsv)) is the set of status-code
section headings in RFC 9110, with section and line numbers, so every
expected answer can be checked by hand.

## What happens

| Step | Event | Naive arm | ket arm |
|---|---|---|---|
| 1 | 4 walkers read 10 pre-2022 RFCs, each RFC by two walkers | chunks (≈800 chars) in a shared index, upserted by document and chunk number | each RFC stored by CID; claims as nodes that are *grounded* in their RFC; extraction memoised by (document CID, extractor CID) |
| 2 | walker-3 writes a confident summary that is wrong today (413/414/416 under their RFC 2616 names); walker-4 builds on it | both become chunks like any other | 3 claims with edge `proposes`, plus a summary that `derives` from them |
| 3 | RFC 9110/9111/9112 arrive after the store was built | indexed alongside the old chunks | doc nodes with `supersedes` edges from the RFC Editor's `obsoletes` lists; a checker *refutes* each proposal that disagrees with current, grounded claims |
| 4 | someone annotates the shared working copy of `rfc9110.txt` | no signal | `ket dag drift` flags it; the claims grounded in it are flagged and re-extracted |

Then each arm answers "What is the reason phrase for HTTP status code N?"
for the 44 codes in the key. There are three readers:

- **naive**: a hybrid search, which is a generous baseline. It keeps the
  chunks that contain the code (an exact-identifier filter), ranks them with
  BM25, and a reader takes the first phrase attached to the code. BM25
  stands in for embedding similarity. The failure measured here, that
  similar is not the same as current, does not depend on which similarity
  is used.
- **ket, blind**: the ket store read *without* edge kinds, which is what
  ket's MCP read tools return today
  ([#27](https://github.com/nickjoven/ket/issues/27)). Every claim for the
  code counts, and the most-asserted phrase wins.
- **ket**: edge kinds enforced. A claim counts only if it is grounded in a
  document that nothing supersedes, is not a proposal, and is not refuted.
  When current sources disagree, it reports a conflict instead of choosing.

## Results (run 2026-11-12)

| | correct | stale | conflict reported |
|---|---|---|---|
| naive | 42 / 44 | 2 (both from the planted summary) | — |
| ket, blind | 42 / 44 | 2 (old RFCs outvote new ones) | — |
| ket | 43 / 44 | 0 | 1 |

```
code answer key        naive                                       ket, blind                        ket
302  Found             Found (correct)                             Found (correct)                   Found | Moved Temporarily (conflict)
413  Content Too Large Request Entity Too Large (stale, rumour)    Request Entity Too Large (stale)  Content Too Large (correct)
414  URI Too Long      URI Too Long (correct)                      Request-URI Too Long (stale)      URI Too Long (correct)
416  Range Not Satisfiable  Requested Range Not Satisfiable (stale, rumour)  Range Not Satisfiable (correct)  Range Not Satisfiable (correct)
```

| Measure | Naive | ket |
|---|---|---|
| answers taken from the wrong summary | 2 | 0 (the proposals were refuted, and the summary built on them was flagged) |
| edit to `rfc9110.txt` | no signal | detected; 44 of 182 claims flagged, all 44 re-extracted byte-identical and cleared |
| citations still pointing at the text they quote, after the edit | 0 of 741 RFC 9110 chunks (offsets shifted) | claims cite the document's CID, so they still point at the bytes they were extracted from; re-hashing confirms them (shown for 413) |
| "where did the 413 answer come from?" | a chunk id: `note:walker-3` | claim ← extraction (extractor CID) ← RFC 9110 bytes (re-hashed OK) ← the supersession chain back to RFC 2068 |
| bytes read and processed | 4,640,142 | 2,319,913 (13 extractions reused) |

## What the results do and do not show

- **On accuracy alone, naive RAG does well here.** It scored 42/44. A
  generous hybrid baseline on a well-structured corpus answers most
  questions correctly, and the demo does not pretend otherwise.
- **The difference is where the errors come from, and whether they can be
  seen.** Both of naive's misses came from one agent's summary, which was
  retrieved because it was short and phrased like the question. Nothing in
  the index marks it as a guess. In the ket store the same summary is a
  `proposes` claim, so it is refuted, and the node built on it is flagged.
- **Recording status is not enough; reads must enforce it.** "ket, blind"
  uses the same store as "ket" and scores no better than naive. The
  difference is entirely in reading edge kinds, which is issue #27.
- **The one ket "miss" is a real disagreement.** RFC 1945 (HTTP/1.0) calls
  302 "Moved Temporarily" and RFC 9110 calls it "Found", and no RFC formally
  obsoletes 1945. Naive picked the current phrase by ranking luck. ket
  reports both current sources rather than choosing silently.
- **The work comparison is by construction.** Each RFC is assigned to two
  walkers, so the naive arm reads everything twice. The ket arm reads once
  because the extraction is keyed by content CID and extractor CID. With
  real LLM walkers, the same key saves model calls rather than bytes.

## What the demo does that ket does not do yet

The demo computes these itself. Each is a gap in ket, and the demo can serve
as an acceptance test for closing it:

- **Reading with edge kinds enforced** ([#27](https://github.com/nickjoven/ket/issues/27)).
  The demo rebuilds a read model from `ket log` and `ket --json dag show`,
  because the MCP read tools do not return edge kinds.
- **Retraction propagation** ([#24](https://github.com/nickjoven/ket/issues/24)).
  The demo walks child edges to flag the summary built on refuted claims.
- **Reverse edges.** "What supersedes this document?" requires scanning every
  node, because ket stores only parent pointers.

## Files

- [`../obsoleted.sh`](../obsoleted.sh): fetches and pins the corpus, then runs the demo
- [`obsoleted.py`](obsoleted.py): both arms, the four events, the questions and scoring (Python standard library only)
- [`extract.py`](extract.py): the deterministic claim extractor; its CID names the extractor in every extraction record
- [`corpus.tsv`](corpus.tsv): RFC number, BLAKE3 CID, size, title, `obsoletes`
- [`answers.tsv`](answers.tsv): the answer key from RFC 9110

A live tier with LLM walkers (temperature 0, prompts and outputs stored by
CID) would measure rates under real extraction noise. It is not built yet.
