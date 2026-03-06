# Ket $|\psi\rangle$

Content-addressable substrate for multi-agent memory systems.

Every artifact (code, reasoning, scores) is BLAKE3-hashed, deduplicated, and stored in an immutable content-addressed store with a queryable SQL mirror powered by [Dolt](https://github.com/dolthub/dolt). Built for multi-agent workflows where provenance, lineage, and scoring matter.

Ket implements the substrate architecture described in [*A Content-Addressed Adaptive Knowledge Substrate for Distributed Epistemic Coordination*](../jfk-dsa/joven_knowledge_substrate.md) (Joven, 2026) — a systems-layer approach to LLM reasoning failures that externalizes memory persistence, provenance, and traversal control into a deterministic, content-addressed infrastructure. The paper's core primitives (Merkle DAG nodes, depth scoring, tiered operations, delta chains, and fixed-point convergence) map directly to ket's crate architecture; see the paper's §9.2 for the full mapping.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                     ket-cli                         │
│              22 commands, --json output             │
├──────────┬──────────┬───────────┬───────────────────┤
│ ket-mcp  │ket-agent │ ket-score │     ket-cdom      │
│ 16 tools │  tasks   │ 4 dims    │   tree-sitter     │
│ JSON-RPC │ routing  │ auto/peer │   Rust + Python   │
├──────────┼──────────┴───────────┴───────────────────┤
│ ket-opt  │  WQS binary search · tier allocation     │
│ calibrate│  Lagrangian relaxation · provenance      │
├──────────┴──────────────────────────────────────────┤
│                     ket-dag                         │
│        Merkle DAG · lineage · soft links            │
├─────────────────────────────────────────────────────┤
│          ket-cas            │        ket-sql        │
│   BLAKE3 flat-file blobs    │  Dolt versioned SQL   │
└─────────────────────────────┴───────────────────────┘
```

**Dual storage model** — CAS is the immutable source of truth; Dolt SQL is the queryable, versioned mirror. A `repair` command reconciles if they drift.

## Workspace Crates

| Crate | Purpose |
|-------|---------|
| **ket-cas** | BLAKE3 content-addressed blob store (`.ket/cas/<hash>`) |
| **ket-dag** | Merkle DAG for provenance — parent chains, soft links, export/import bundles |
| **ket-sql** | Dolt SQL wrapper — 9 tables, versioned commits, lineage queries |
| **ket-mcp** | MCP server (stdio JSON-RPC) exposing 16 tools for Claude and other agents. Dolt is optional — CAS-only tools work without it. |
| **ket-agent** | Multi-agent orchestration — task lifecycle, subprocess spawning, context injection |
| **ket-score** | Scoring engine — correctness, efficiency, style, completeness — with auto-scoring via `cargo build/test/clippy` |
| **ket-opt** | WQS binary search optimizer — Lagrangian relaxation for compute tier allocation across DAG nodes |
| **ket-cdom** | Code Document Object Model — tree-sitter parsing for Rust and Python symbol extraction |
| **ket-cli** | CLI binary with 22 commands |
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

# Calibrate traversal tiers for a subtree
ket calibrate run <root_cid> --max-cost 50
```

## Docker Quickstart

Run ket without installing Rust or Dolt locally.

```sh
# Build the image
docker compose build

# Initialize a ket store
docker compose run --rm ket init

# Store a file (mount your project into /data)
docker compose run --rm -v "$PWD":/data/project ket put /data/project/myfile.rs

# DAG operations
docker compose run --rm ket dag create "initial reasoning" --kind reasoning --agent claude
docker compose run --rm ket dag ls
docker compose run --rm ket status
```

The `/data` volume persists your ket store across runs.

**Optional Dolt sidecar** — for scoring, tasks, and SQL queries:

```sh
docker compose --profile full up -d dolt
docker compose run --rm ket repair
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

### Calibration
- `ket calibrate run <root_cid>` — WQS optimize tier allocation (`--max-cost`, `--max-depth`, `--max-tier3`)
- `ket calibrate inspect <cid>` — Read back a stored calibration
- `ket calibrate history <root_cid>` — All calibrations for a subtree

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

Ket exposes 16 tools over MCP (Model Context Protocol) for agent integration.

**Dolt is optional.** The MCP server starts with CAS alone — 13 of 16 tools work without Dolt. Only scoring, tasks, and calibration require it.

| Tool | What it does | Needs Dolt? |
|------|-------------|-------------|
| `ket_put` | Store content, get CID | No |
| `ket_get` | Retrieve content by CID | No |
| `ket_verify` | Check CID integrity | No |
| `ket_dag_link` | Create DAG node with provenance | No |
| `ket_dag_lineage` | Trace ancestry chain | No |
| `ket_dag_ls` | List/filter DAG nodes | No |
| `ket_check_drift` | Detect file changes | No |
| `ket_search` | Full-text content search | No |
| `ket_status` | Substrate health dashboard | No (enhanced with Dolt) |
| `ket_store_reasoning` | Persist reasoning as DAG node | No |
| `ket_get_reasoning` | Retrieve reasoning with context | No |
| `ket_query_cdom` | Search code symbols | No |
| `ket_schema_stats` | Check schema dedup effectiveness | No |
| `ket_score` | Record quality scores | **Yes** |
| `ket_create_subtask` | Delegate work to agents | **Yes** |
| `ket_calibrate` | Optimize traversal tiers | **Yes** |

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
- **Schema-linked, not schema-enforced** — See below.

## Schemas and Deduplication

Ket's CAS deduplicates by content hash: identical bytes produce identical CIDs. This is exact dedup, not semantic dedup. Two blobs that *mean* the same thing but differ by a trailing newline or key ordering get different CIDs.

Schemas address this without pulling ket above the intelligence line.

### How it works

A schema is any blob you store in CAS — JSON Schema, a struct definition, a prompt template, a plain-English description. Ket does not interpret it. When creating a DAG node, you attach the schema's CID:

```sh
# Store your schema
SCHEMA_CID=$(ket put my_schema.json)

# Create a node whose output conforms to it
ket dag create "structured observation" \
  --kind memory --agent claude --schema $SCHEMA_CID
```

The `schema_cid` field on the node records what shape the output *claims* to have. That's the contract. Enforcement is your problem.

### Why this helps dedup

Content-hash dedup works when semantically equivalent data produces byte-identical output. Schemas make this achievable by constraining the surface area: sorted keys, canonical formatting, required fields only, no optional noise. If agents conform to the schema, equivalent observations hash the same.

### What ket provides

- **`schema_cid` on every node** — optional, stored in the DAG, queryable.
- **Schema stats** — given a schema CID, count total nodes vs. unique output CIDs. If they're equal, the schema isn't producing dedup. If they diverge, it is. This is a pure hash-count query — no semantic understanding needed.
- **Propagation via provenance** — when a schema evolves, the DAG makes the blast radius visible. Query for all nodes with the old schema CID, trace their lineage, migrate explicitly.

### What ket does NOT provide

- Schema validation at ingest. Ket won't reject non-conforming data.
- Schema format opinions. JSON Schema, protobuf, TOML — ket doesn't care.
- Semantic dedup. If two blobs mean the same thing but have different bytes, they get different CIDs. The schema's job is to prevent that from happening.

The substrate stays below the intelligence line. Schemas are a user-side discipline that makes the content-addressing layer work harder for you.

## License

MIT
