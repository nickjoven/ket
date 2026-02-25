# Ket $|\psi\rangle$

Content-addressable substrate for multi-agent memory systems.

Every artifact (code, reasoning, scores) is BLAKE3-hashed, deduplicated, and stored in an immutable content-addressed store with a queryable SQL mirror powered by [Dolt](https://github.com/dolthub/dolt). Built for multi-agent workflows where provenance, lineage, and scoring matter.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                     ket-cli                         │
│              21 commands, --json output              │
├──────────┬──────────┬───────────┬───────────────────┤
│ ket-mcp  │ket-agent │ ket-score │     ket-cdom      │
│ 11 tools │  tasks   │ 4 dims   │   tree-sitter     │
│ JSON-RPC │ routing  │ auto/peer │   Rust + Python   │
├──────────┴──────────┴───────────┴───────────────────┤
│                     ket-dag                          │
│        Merkle DAG · lineage · soft links             │
├─────────────────────────────────────────────────────┤
│          ket-cas            │        ket-sql         │
│   BLAKE3 flat-file blobs   │  Dolt versioned SQL    │
└─────────────────────────────┴───────────────────────┘
```

**Dual storage model** — CAS is the immutable source of truth; Dolt SQL is the queryable, versioned mirror. A `repair` command reconciles if they drift.

## Workspace Crates

| Crate | Purpose |
|-------|---------|
| **ket-cas** | BLAKE3 content-addressed blob store (`.ket/cas/<hash>`) |
| **ket-dag** | Merkle DAG for provenance — parent chains, soft links, export/import bundles |
| **ket-sql** | Dolt SQL wrapper — 8 tables, versioned commits, lineage queries |
| **ket-mcp** | MCP server (stdio JSON-RPC) exposing 11 tools for Claude and other agents |
| **ket-agent** | Multi-agent orchestration — task lifecycle, subprocess spawning, context injection |
| **ket-score** | Scoring engine — correctness, efficiency, style, completeness — with auto-scoring via `cargo build/test/clippy` |
| **ket-cdom** | Code Document Object Model — tree-sitter parsing for Rust and Python symbol extraction |
| **ket-cli** | CLI binary with 21 commands |
| **ket-py** | PyO3 Python bindings for CAS and DAG operations |

## Prerequisites

- **Rust** (stable, 2021 edition)
- **Dolt** — install from [dolthub/dolt](https://github.com/dolthub/dolt)

## Quickstart

```sh
# Build
cargo build --release

# Initialize a ket store
ket init

# Store a file and get its content ID
ket put myfile.rs

# Create a DAG node with lineage
ket dag create "initial reasoning" --kind reasoning --agent claude

# Track a file for drift detection
ket track add src/main.rs --agent claude
ket drift

# Register an agent and create a task
ket agent register claude
ket task create "Implement auth module" --by claude

# Scan code symbols
ket scan src/lib.rs
ket cdom "parse"

# Start the MCP server (for Claude integration)
ket mcp

# Auto-score an output (compile + test + clippy)
ket scores auto <cid> --agent claude --dir .
```

## CLI Commands

### Content Store
- `ket init` — Initialize `.ket` directory
- `ket put <file>` — Store file, return CID
- `ket get <cid>` — Retrieve content by CID
- `ket verify <cid>` — Check integrity
- `ket cas-stats` — Store size breakdown
- `ket gc [--delete]` — Garbage collect orphan blobs

### DAG & Lineage
- `ket dag create <content>` — Create node (`--kind`, `--parent`, `--agent`)
- `ket dag ls` / `ket dag show <cid>` — List/inspect nodes
- `ket dag lineage <cid>` — Trace ancestor chain
- `ket dag drift <path> <cid>` — Detect file drift
- `ket link create <from> <to> <rel>` — Soft links (supersedes, contradicts, etc.)
- `ket merge <content> --parents <cid>...` — Multi-parent merge node
- `ket dot [--root <cid>]` — Graphviz DOT visualization
- `ket export <cid>` / `ket import <file>` — Portable DAG bundles

### Tasks & Agents
- `ket task create <title>` / `ket task ls` / `ket task assign <id> <agent>`
- `ket agent register <preset>` / `ket agent ls`
- `ket run <task-id>` — Execute task via agent subprocess

### Code Intelligence
- `ket scan <path>` — Index symbols (Rust/Python)
- `ket cdom <query> [path]` — Search extracted symbols
- `ket search <text>` — Full-text content search

### Scoring
- `ket scores add <cid>` — Record score (`--dim`, `--value`, `--agent`)
- `ket scores show <cid>` — Scores for a node
- `ket scores profile <agent>` — Agent averages
- `ket scores route <dim>` — Best agent for a dimension
- `ket scores auto <cid>` — Auto-score (build/test/clippy)

### Operations
- `ket sql <query>` — Raw SQL against Dolt
- `ket log [-n <count>]` — Mutation log
- `ket status` — Health dashboard
- `ket history` / `ket diff` — Dolt version history
- `ket repair [--dry-run]` — Rebuild SQL from CAS
- `ket track add/ls/rm` — File drift tracking

### Global Flags
- `--home <path>` — Override `.ket` directory (env: `KET_HOME`)
- `--json` — Structured JSON output

## MCP Integration

Ket exposes 11 tools over MCP (Model Context Protocol) for agent integration:

`ket_put`, `ket_get`, `ket_verify`, `ket_dag_link`, `ket_dag_lineage`, `ket_check_drift`, `ket_query_cdom`, `ket_store_reasoning`, `ket_create_subtask`, `ket_get_reasoning`, `ket_score`

Add to your Claude MCP config:

```json
{
  "mcpServers": {
    "ket": {
      "command": "ket",
      "args": ["mcp"]
    }
  }
}
```

## Design Principles

- **Content-addressed everything** — Same content = same CID. Deterministic, deduped, immutable.
- **Provenance by default** — Every artifact links to its parents via the Merkle DAG.
- **Dual storage** — CAS for truth, SQL for queries. Either can reconstruct the other.
- **Scoring gates routing** — Historical evaluation across 4 dimensions lets the system learn which agent is best at what.
- **Drift detection** — Tracked files are re-hashed on demand to prevent stale reasoning context.
- **Portable bundles** — DAG subgraphs can be exported and imported across instances.

## License

MIT
