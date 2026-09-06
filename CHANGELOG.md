# Changelog

All notable changes to ket are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/);
ket adheres to semantic versioning.

## [Unreleased]

### Fixed
- **Concurrent writers no longer lose projection rows.** `dolt sql` cannot
  take concurrent writers; ket ran them anyway and swallowed the error, so
  16 parallel `dag create`s left 3 of 17 node rows in SQL. Every Dolt access
  now runs under a per-store advisory lock (reentrant within a thread). The
  CAS write stays lock-free; only the projection serializes.
- **The mutation log no longer tears under concurrency.** Each entry is
  written in one buffered `write_all` instead of `writeln!`'s several
  syscalls, so O_APPEND keeps concurrent entries whole (32 writers used to
  leave 20 of 42 lines malformed).
- **The projection audit and rebuild now cover `dag_nodes`, not only
  `dag_edges`.** A lost parentless node has no edges, so edge-only auditing
  called it clean. `rebuild-projection` replays both tables from the CAS and
  recreates the Dolt db if it was deleted, so the projection is
  reconstructible from the substrate. `repair` reconciles missing, extra AND
  altered rows (it used to only insert missing ones, so a hand-edited row
  survived); the Dolt-only tables (soft links, scores, tasks, agents) are
  left intact.
- **Projection sync failures surface** instead of being dropped with
  `let _ =`: `dag create`/`merge`/`import` warn and point at `ket repair`.
- `ket gc --delete` no longer deletes a schema blob a live node references.
- `ket import` preserves typed edges (it downgraded them all to `derives`),
  errors on a tampered bundle instead of panicking, and reads from stdin
  (`ket import -`).
- A parent must be a real DAG node; a content blob as a parent used to seal a
  node whose `lineage` silently dead-ended.
- `ket run` honors the registered agent's command (it hard-coded claude/codex),
  refuses an unknown task instead of minting an orphan node, and records the
  result CID on the task; `task assign` refuses an unknown task too.
- `ket calibrate` writes a mutation-log line (the one DAG write that left none).
- `ket status --json` strips Dolt's ANSI colour codes from `dolt_head`.
- `Store::put` fsyncs the blob before the rename and removes its temp file on a
  failed write; `Store::delete`/`blob_size` reject malformed CIDs.
- **Typed edges are sealed in the node.** `ket dag create --edge-kind` and
  `ket merge` wrote the kind to `dag_edges` only, so `verify-projection`
  diverged on the very next call. Both now build the node with
  `DagNode::new_typed` and project SQL rows from `parent_links()`;
  `repair` reads kinds from the node instead of defaulting to `derives`.
- `ket drift` exits 1 on drifted/missing files and 2 on environment
  error, so `ket drift && agent` actually gates.
- **Concurrent puts of identical content no longer race.** `Store::put`
  derived its temp file name from the CID alone, so two writers landing
  the same bytes at once collided and one failed with ENOENT. Each
  writer now uses its own temp name and a lost rename against an
  existing blob is a successful dedup. Found by the parallel-review
  demo; covered by an 8-thread test.
- A malformed CID (empty, non-hex, wrong length) is `NotFound` from
  `Store::get`/`exists` instead of a filesystem probe (`Cid::from("")`
  used to read the store directory).
- **Parents are checked before they are sealed.** `dag create --parent`,
  `merge --parents` and the MCP `parents` array accepted any string; a
  non-ASCII or empty parent then broke `ket graph` for the whole store,
  permanently, because nodes are immutable. Each parent must now be a
  well-formed CID that exists in the store. `ket graph` itself no longer
  byte-slices CIDs and sanitizes Mermaid node ids.
- MCP `edge_kind` is strict: a misspelled `supersedes` used to seal
  silently as `derives`. `EdgeKind::parse_or_default` is gone.
- `ket merge --parents` accepts the same `<cid>[:<kind>]` syntax as
  `dag create`; before, the suffix was sealed into the parent CID.
- DOT and Mermaid output escape agent names, labels and soft-link
  relations; a JSON `title` with a newline no longer splits a node
  statement. Soft-link relations containing commas parse correctly.
- A log append that fails after a write has already been sealed is a
  warning on stderr, not a failed command (CLI and MCP). Failing the
  command told the caller nothing was written and invited a retry that
  minted a duplicate node.
- `Store::put` fsyncs the blob before the rename (the log was already
  fsynced, so a crash could leave a logged CID with a partial blob) and
  removes its temp file when the write fails. `Store::delete` and
  `blob_size` get the same well-formedness guard as `get`.
- `ket drift` exits 2, not 1, when a tracked file is present but
  unreadable: that is "cannot check", not "missing".

### Added
- **Resolution edges.** `EdgeKind` gains `confirms`, `refutes`, `supersedes`
  (with `EdgeKind::ALL` and a strict `parse`). `ket dag create --parent`
  accepts `<cid>:<kind>` so one node can confirm a claim and be grounded
  by evidence in a single write. This closes DESIGN.md's open L2 choice:
  edge kind is in-node; a correction is a node with a `supersedes` parent,
  never an overwrite. `ket graph` styles the new kinds.
- `ket graph --format dot|mermaid|json` (alias `ket dot`). Nodes carry
  kind, agent and a one-line content preview; provenance edges are styled
  by epistemic kind; soft links render dashed. Mermaid output renders
  natively in GitHub Markdown and on GitHub Pages.
- `ket merge --edge-kind`.
- `ket dag create --content-file <path>` and `ket merge --content-file`
  (`-` for stdin), for content too large for argv or shaped like a flag.
- `ket agent register <name> --command "<cli> ..." [--model <m>]` registers a
  custom agent, not only the three presets.
- `ket_cas::log` — the append-only mutation log moved from ket-cli into
  ket-cas (`log_path_for(store)` derives `.ket/log` from a CAS root) so
  every writer shares it. The MCP server now appends `put` and
  `dag:create` events; previously its writes left no log history.
- `docs/` — the ket + catbus walkthrough (`demo.sh`, `DEMO.md`,
  `index.html` for GitHub Pages).

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
