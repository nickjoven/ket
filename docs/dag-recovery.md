# Recovering a partially completed DAG create

`dag create` is a new timestamped event on every invocation, even with identical
content. An interrupted create must be reconciled by its recorded node CID;
blindly retrying `dag create` can create a second node.

Creation records an append-only `dag:create:intent` naming the exact prospective
node CID before publishing that node in CAS. An intent is an attempted write,
not proof of publication. A `dag:create` record follows CAS publication and
precedes SQL projection. If the process dies, inspect `ket log -n 100` or the
store's `log` file to obtain the CID. Recovery verifies whether that node exists
and rehashes its bytes and direct content, parent and schema dependencies.
A failed intent append prevents node publication. A crash before publication
can leave an intent whose node does not exist; recovery reports that fact.

```sh
ket --home /path/to/.ket --json dag recover <recorded-node-cid>
ket --home /path/to/.ket verify-projection
```

Recovery appends `dag:recover` before attempting SQL and projects the retained
node without constructing a new node or changing its timestamp. Repeated
recovery preserves the node CID and node count; each attempt adds a log entry.
Missing SQL rows and edges are completed. Existing conflicting rows or extra
incoming edges cause a failure directing the caller to `ket repair`, followed
by recovery of the same CID. Recovery checks the target node's projected row
and complete set of incoming typed edges. It does not certify unrelated rows.

For JSON create and recover output, the original `node_cid`, `content_cid`, and
optional `schema_cid` fields remain. Additional fields describe partial success:

| Field | Meaning |
|---|---|
| `retained: true` | The node was stored in CAS; failure does not mean rollback. |
| `status: complete` | Logging and the configured projection completed. |
| `status: partial` | Node retained, but logging or projection failed; exit is nonzero. |
| `projection: synced` | SQL contains the target node row and exact incoming edges. |
| `projection: failed` | Configured SQL could not open, write, or agree with CAS. |
| `projection: not_attempted` | Publication/recovery log append failed before SQL. |
| `projection: not_configured` | No `ket.db` path exists; CAS-only operation completed. |
| `recovery` | Structured `command: "dag recover"` and `node_cid`, for the same store. |
| `error` | Failure detail when `status` is `partial`. |

Failures before node publication or validation do not promise a partial-success
JSON object. For example, an invalid CID, missing node, corrupt dependency, or
failed intent append returns an error without claiming a retained valid node.
If `ket.db` exists but Dolt is unavailable or the database cannot open, the
operation fails explicitly; it never silently falls back to CAS-only success.
A missing database can be rebuilt separately with `rebuild-projection`.

This contract covers process crashes and observable I/O failures. It is not an
atomic transaction across CAS, log and Dolt, an exactly-once API, or a guarantee
about machine power loss and directory fsync. It does not checkpoint Dolt
history: writes change its working set; checkpoint commits remain explicit.
`merge`, import, MCP and other writers retain their existing contracts.

The current post-write check reads the node and edge projection tables and
filters to the target. This adds work proportional to the projection size;
these bounded correctness tests do not establish performance at large scale.

## Source and verification index

- `ket-cli/src/main.rs`: `DagAction::Create`, `DagAction::Recover`,
  `validate_retained_node`, `finish_retained_node`.
- `ket-cas/src/log.rs`: append, one buffered line followed by file sync.
- `ket-sql/src/lib.rs`: `sync_dag_node`, `dag_node_rows`, `dag_edges`.
- `scripts/test_dag_recovery.py`: process-level regression tests, including
  SIGKILL during the SQL INSERT, projection failure, recovery, log failure,
  CAS-only behavior, database failure, corrupt CAS and conflicting SQL.

```sh
cargo build --offline -p ket-cli
python3 scripts/test_dag_recovery.py --ket target/debug/ket -v
cargo test --offline -p ket-cli --test integration -- --test-threads=2
```
