# Retraction propagation: XMRV / chronic fatigue syndrome

This captured co-citation sample tests how a provenance graph responds when each retracted seed is treated as lost support. Citation edges approximate support; these flags are not findings that a paper's conclusions are false.

| Retracted seed | Downstream papers reached | Invalidated | Weakened | Unknown | Other refuted | Depth range | Complete? |
|---|---:|---:|---:|---:|---:|---|---|
| [XMRV / Science (2009)](https://doi.org/10.1126/science.1179052) | 176 | 1 | 0 | 174 | 1 | 1–1 | **no** |
| [MLV / PNAS (2010)](https://doi.org/10.1073/pnas.1006901107) | 175 | 0 | 0 | 175 | 0 | 1–1 | **no** |

Counts exclude the queried seed. `Other refuted` counts downstream papers with recorded refutation notices inside that seed's closure; intact counts, all CIDs, and complete status details remain in the JSON.

**XMRV / Science (2009):** missing/non-DAG support 5747; undiscovered support parents 0; held-back children 0; frontier 0. [Full CID report](paper-results/report-W2160675481.json).
Example: [Possible widespread low-level occurrence of murine leukemia virus-related gene sequences in humans](https://doi.org/10.1073/pnas.1012930107) — `invalidated`, depth 1, CID `b1047fd5d92776b501821b75008ff98c40b4a9e13c95eb0a9eb21bd41ec923a7`. Its full seed-to-paper CID path and lost-parent list are in the [summary](paper-results/summary.json).
This node has only 2 captured support links; all are lost under the citation model. That is a property of captured references, not a verdict on the paper's conclusions.

**MLV / PNAS (2010):** missing/non-DAG support 5747; undiscovered support parents 0; held-back children 0; frontier 0. [Full CID report](paper-results/report-W2026087575.json).
Example: [Mother's Milk and Intrinsic Immunity](https://doi.org/10.1016/j.chom.2010.11.013) — `unknown`, depth 1, CID `0065f4350eb1be95e0932d5f2d1d1b7a22d133767161221b0be1e9d403b5f3b9`. Its full seed-to-paper CID path and lost-parent list are in the [summary](paper-results/summary.json).

The rows are not simultaneous-two-seed queries. The Science-seed closure also reaches the PNAS seed and its recorded retraction; the reverse query does not automatically refute the Science ancestor. This scope difference explains why the verdict counts can differ.

## Scope and evidence

Captured **2026-11-04T07:09:28.282099+00:00**: 175 papers selected because they cite **both** seeds, plus the two seed records (177 papers total). This is a selected co-citation cohort, not a random sample or a complete subfield. The keyless OpenAlex list query fit on one page: `cites:W2160675481,cites:W2026087575`, `per_page=200`, `sort=publication_date:asc`.
The [capture manifest](../../fixtures/retraction-poc/verified/manifest.json) SHA-256 is `3b8fee0b018faadb39b7dae8565e673a27093196837c2c0a5a997f0ee36ab121`. [Summary](paper-results/summary.json) preserves each exact request URL, response byte count, and SHA-256; [graph map](paper-results/graph-map.json) pins source IDs to CIDs. Offline reproduction uses captured bytes, not a fresh live query.

Retraction verification is limited to these two seeds: the captured Crossref update records include `type=retraction`, `source=retraction-watch`, record IDs **492**, **5657**. Expression-of-concern and correction records are preserved separately and do not become refutations. OpenAlex `is_retracted` flags for the captured records are retained, but other cohort papers do not receive a comprehensive Crossref/Retraction Watch check.
The conservative graph keeps 9404 out-of-sample citation edges as unresolved raw evidence and records 3 cycle-cut edges. These are not intact, support-free paper nodes. [Graph map](paper-results/graph-map.json) preserves their source IDs, CIDs, and reasons; citation cycles need this disclosed conversion because CID-parent identity forms a DAG.
Each run explicitly refutes one seed. Recorded refutations are considered only within that seed's downstream closure; a refuted co-parent outside the closure is not automatically treated as lost. No simultaneous-two-seed analysis is implied.

## Interpretation

A citation may be background, methodological, supportive, or critical. The graph has no citation-intent classification or extracted claims. Unknown support is preserved; `complete: false` must not be read as proof that no other papers are affected. An empty invalidated or weakened count is an honest result under this model. Synthetic acceptance examples are [separate](synthetic-acceptance.md).
A witness path proves reachability, not all-support loss. The full reports list every lost/missing/unresolved support parent; the renderer checks witness edges and all-support-loss assertions against the replay CAS.

## Reproduce offline

```sh
cargo build -p ket-cli
python3 scripts/retraction-poc/replay.py \
  --capture fixtures/retraction-poc/verified --ket target/debug/ket \
  --out /tmp/ket24-paper-replay
python3 docs/retraction-poc/render_papers.py \
  --capture fixtures/retraction-poc/verified --replay /tmp/ket24-paper-replay \
  --output /tmp/ket24-paper-report
```

Choose fresh output directories. Python uses only the standard library. No network, API key, or Dolt service is required. `paper-report.md` and `paper-results/*.json` should reproduce byte-for-byte.
