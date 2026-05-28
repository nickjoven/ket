# Changelog

All notable changes to ket are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/);
ket adheres to semantic versioning.

## [0.2.0] — 2026-05-27

First versioned release. Marks the additive features accumulated on top
of the unreleased `0.1.0` baseline. No breaking changes to `ket-cas` or
`ket-dag` public APIs — downstream consumers (k-stack, canon.d, catbus)
compile unchanged.

### Added
- **Epistemic edge kinds** on DAG edges — `grounds`, `derives`, `proposes`
  — so lineage records *why* one node relates to another, not just *that*
  it does (#9).
- **MCP saturation/decay parameters** on `ket_dag_link` and
  `ket_store_reasoning`, with input validation (#8, reapplied after #7).
- MCP server now exposes a 19-tool surface (get/put/verify/search,
  DAG link/lineage/ls, drift, CDOM query, reasoning store/get, score,
  calibrate, schema-stats, status, soft-link, decay-status).

## [0.1.0]
- Initial baseline: BLAKE3 CAS, Merkle DAG lineage, Dolt SQL index,
  tree-sitter CDOM, MCP server, scoring/calibration. Never tagged.
