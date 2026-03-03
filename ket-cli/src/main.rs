#![allow(clippy::ptr_arg)]

mod log;

use clap::{Parser, Subcommand};
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "ket",
    about = "Content-addressable substrate for EverMemOS",
    long_about = "\
Content-addressable substrate for multi-agent memory systems.

Ket provides BLAKE3-hashed content storage (CAS), a Merkle DAG for lineage
tracking, Dolt SQL for queryability, tree-sitter code parsing (CDOM), an
MCP server for agent integration, and multi-agent orchestration with scoring.

Every piece of content gets a deterministic CID. Deduplication is free.
Lineage is provable. Drift is detectable. Agents coordinate through
content-addressed reasoning chains.

QUICKSTART:
  ket init                           Create a .ket directory
  ket put myfile.py                  Store a file, get its CID
  ket get <cid>                      Retrieve content by CID
  ket dag create \"note\" --agent me   Create a DAG node
  ket dag lineage <cid>              Trace a node's history
  ket scan src/                      Index code symbols
  ket status                         Show system overview

ENVIRONMENT:
  KET_HOME    Path to .ket directory (default: .ket in current dir)",
    version,
    after_help = "See 'ket <command> -h' for more information on a specific command."
)]
struct Cli {
    /// Path to .ket directory (default: .ket in current dir)
    #[arg(long, global = true, env = "KET_HOME")]
    home: Option<String>,

    /// Emit JSON output (all commands support structured JSON)
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new .ket directory
    #[command(long_about = "\
Initialize a new .ket directory with CAS store and optional Dolt database.

Creates:
  .ket/cas/          Content-addressable blob store
  .ket/config.yaml   Agent and system configuration
  .ket/log           Append-only mutation log
  .ket/manifest      DAG head references
  .ket/ket.db/       Dolt SQL database (if dolt is installed)

Example:
  ket init
  ket --home /tmp/myproject/.ket init")]
    Init,

    /// Store a file (or stdin) in CAS, return its BLAKE3 CID
    #[command(long_about = "\
Store content in the content-addressable store and return its CID.

The CID is a 64-character hex string (BLAKE3-256 hash). Identical content
always produces the same CID — deduplication is automatic.

Examples:
  ket put myfile.py              Store a file
  echo 'hello' | ket put -       Store from stdin
  ket --json put myfile.py       Get CID as JSON")]
    Put {
        /// File path to store, or '-' to read from stdin
        path: String,
    },

    /// Retrieve content by CID
    #[command(long_about = "\
Retrieve stored content by its CID.

Outputs raw bytes to stdout (or JSON with --json flag including size).

Examples:
  ket get abc123...def          Print content to stdout
  ket get abc123...def > out    Redirect to file
  ket --json get abc123...def   Get content + metadata as JSON")]
    Get {
        /// Content identifier (64-char hex BLAKE3 hash)
        cid: String,
    },

    /// Verify CID integrity (re-hash and compare)
    #[command(long_about = "\
Verify that stored content matches its CID by re-hashing.

Detects bit-rot or corruption. Exits with code 1 if corrupted.

Example:
  ket verify abc123...def")]
    Verify {
        /// Content identifier to verify
        cid: String,
    },

    /// DAG operations (create nodes, trace lineage, detect drift)
    #[command(long_about = "\
Merkle DAG operations for tracking provenance and lineage.

Nodes represent artifacts (memories, code, reasoning, tasks) linked by
parent edges. Each node points to content in CAS via output_cid.

Node kinds: memory, code, reasoning, task, cdom, score, context

Examples:
  ket dag create 'initial design' --kind reasoning --agent claude
  ket dag create 'revision' --kind code --agent codex --parent <cid>
  ket dag lineage <cid>            Walk the parent chain
  ket dag ls                       List all nodes
  ket dag show <cid>               Show node details")]
    Dag {
        #[command(subcommand)]
        action: DagAction,
    },

    /// Execute raw SQL against the Dolt database
    #[command(long_about = "\
Execute a SQL query against the Dolt database (CSV output).

The database contains tables: dag_nodes, dag_edges, soft_links,
tasks, agents, scores, context_files, cdom_symbols.

Examples:
  ket sql 'SELECT * FROM dag_nodes LIMIT 5'
  ket sql 'SELECT kind, COUNT(*) FROM dag_nodes GROUP BY kind'")]
    Sql {
        /// SQL query to execute
        query: String,
    },

    /// Task lifecycle management (create, list, assign)
    #[command(long_about = "\
Manage tasks for multi-agent orchestration.

Tasks flow: pending -> assigned -> running -> done/failed.
Requires Dolt database.

Examples:
  ket task create 'Refactor auth module' --by human
  ket task ls
  ket task assign <id> claude")]
    Task {
        #[command(subcommand)]
        action: TaskAction,
    },

    /// Agent registry (register, list agent configs)
    #[command(long_about = "\
Manage the agent registry. Built-in presets: claude, codex, copilot.

Examples:
  ket agent register claude
  ket agent ls")]
    Agent {
        #[command(subcommand)]
        action: AgentAction,
    },

    /// Run a task by spawning an agent subprocess
    #[command(long_about = "\
Execute a task by spawning the assigned agent as a subprocess.

The agent's output is stored as a Reasoning DAG node.
Requires the agent CLI to be installed (e.g., 'claude' for Claude).

Examples:
  ket run <task-id> --agent claude --prompt 'Review this code'
  ket run <task-id> --agent codex")]
    Run {
        /// Task ID to run
        task_id: String,
        /// Agent to use (claude, codex)
        #[arg(long, default_value = "claude")]
        agent: String,
        /// Prompt for the agent (default: 'Complete the assigned task')
        #[arg(long)]
        prompt: Option<String>,
    },

    /// Parse source files with tree-sitter and index symbols
    #[command(long_about = "\
Scan source files using tree-sitter to extract symbols (functions, classes,
structs, methods, etc.) and index them in SQL for fast querying.

Supports Rust (.rs) and Python (.py) files. Directories are scanned
recursively, skipping .git, target, node_modules, __pycache__.

Scanned files are automatically tracked for drift detection.

Examples:
  ket scan src/main.rs          Scan a single file
  ket scan src/                 Scan a directory recursively
  ket --json scan .             JSON output with full symbol details")]
    Scan {
        /// File or directory to scan
        path: String,
    },

    /// Query code symbols by name (searches SQL index or a specific file)
    #[command(long_about = "\
Search for code symbols by name across the indexed codebase.

Without a file argument, searches the SQL symbol index (run 'ket scan' first).
With a file argument, parses the file directly with tree-sitter.

Examples:
  ket cdom UserProfile                    Search all indexed files
  ket cdom process_data src/main.py       Search specific file
  ket --json cdom 'Handler'               JSON output")]
    Cdom {
        /// Symbol name to search for (substring match)
        query: String,
        /// Specific file to search (optional — omit to search SQL index)
        path: Option<String>,
    },

    /// Show the append-only mutation log
    #[command(long_about = "\
Display recent entries from the append-only mutation log.

Every CAS put, DAG create, repair, and other mutations are logged
with timestamps for auditability.

Examples:
  ket log                Show last 20 entries
  ket log -n 50          Show last 50 entries
  ket --json log         JSON output")]
    Log {
        /// Number of entries to show
        #[arg(short, long, default_value = "20")]
        n: usize,
    },

    /// Run the MCP (Model Context Protocol) server on stdio
    #[command(long_about = "\
Start the MCP server on stdin/stdout using JSON-RPC over line-delimited JSON.

Exposes 11 tools: ket_put, ket_get, ket_verify, ket_dag_link,
ket_dag_lineage, ket_check_drift, ket_query_cdom, ket_store_reasoning,
ket_create_subtask, ket_get_reasoning, ket_score.

Use with Claude or other MCP-capable agents:
  claude --mcp-config ket.json -p 'store a memory'

MCP config (ket.json):
  {\"mcpServers\": {\"ket\": {\"command\": \"ket\", \"args\": [\"mcp\"]}}}")]
    Mcp,

    /// Agent scoring — record scores, view profiles, route tasks
    #[command(long_about = "\
Score agent outputs across four dimensions and use scores for routing.

Dimensions: correctness, efficiency, style, completeness.
Sources: human review, auto (compile/test/lint), peer (cross-agent).

Examples:
  ket scores add <cid> --agent claude --dim correctness --value 0.9
  ket scores show <cid>              Show all scores for a node
  ket scores profile claude          Agent's average scores
  ket scores route correctness       Best agent for a dimension
  ket scores auto <cid> --agent claude --dir .   Run build/test/clippy")]
    Scores {
        #[command(subcommand)]
        action: ScoreAction,
    },

    /// WQS calibration — optimize traversal tier allocation
    #[command(long_about = "\
Run Lagrangian relaxation (WQS/Aliens trick) to optimally allocate compute
tiers across DAG nodes given budget constraints.

Calibration results are stored as DAG nodes with provenance.

Examples:
  ket calibrate run <root_cid> --max-cost 50
  ket calibrate inspect <result_cid>
  ket calibrate history <root_cid>")]
    Calibrate {
        #[command(subcommand)]
        action: CalibrateAction,
    },

    /// Rebuild SQL index from CAS (CAS is source of truth)
    #[command(long_about = "\
Reconcile the SQL database from the CAS store.

CAS is the source of truth. If SQL is out of sync (crash, partial write),
repair scans all CAS blobs, identifies valid DAG nodes, and syncs them
to SQL using idempotent INSERT IGNORE.

Examples:
  ket repair              Sync missing nodes to SQL
  ket repair --dry-run    Show what would be synced without writing")]
    Repair {
        /// Show what would be synced without actually writing
        #[arg(long)]
        dry_run: bool,
    },

    /// Show .ket health dashboard (CAS blobs, SQL stats, Dolt HEAD)
    #[command(long_about = "\
Display a health overview of the .ket directory.

Shows: CAS blob count, Dolt HEAD commit, DAG nodes/edges, tasks,
agents, scores, soft links, tracked files, CDOM symbols.

Example:
  ket status")]
    Status,

    /// Show Dolt commit history
    #[command(long_about = "\
Display Dolt version control history.

Dolt tracks every schema and data change with git-like commits.

Examples:
  ket history              Last 10 commits
  ket history -n 50        Last 50 commits")]
    History {
        /// Number of commits to show
        #[arg(short, long, default_value = "10")]
        n: usize,
    },

    /// Show Dolt diff (working changes or between commits)
    #[command(long_about = "\
Show differences in the Dolt database.

Without arguments, shows uncommitted changes (working set vs HEAD).
With commit hashes, shows diff between two points in history.

Examples:
  ket diff                 Working changes vs HEAD
  ket diff <from> <to>     Diff between two commits")]
    Diff {
        /// Starting commit hash
        from: Option<String>,
        /// Ending commit hash
        to: Option<String>,
    },

    /// Manage soft links between DAG nodes (for cycle-safe relations)
    #[command(long_about = "\
Soft links represent relationships that would create cycles in the DAG.

Unlike parent edges (which must be acyclic), soft links can point anywhere.
Useful for: supersedes, contradicts, related_to, refines, etc.

Examples:
  ket link create <from> <to> supersedes
  ket link create <from> <to> contradicts
  ket link ls <cid>")]
    Link {
        #[command(subcommand)]
        action: LinkAction,
    },

    /// Track files for bulk drift detection
    #[command(long_about = "\
Register files for automatic drift detection.

Tracked files have their BLAKE3 hash stored. When you run 'ket drift',
all tracked files are re-hashed and compared — any changes are flagged.

Files scanned with 'ket scan' are auto-tracked.

Examples:
  ket track add src/main.py --agent claude
  ket track ls
  ket track rm src/old.py")]
    Track {
        #[command(subcommand)]
        action: TrackAction,
    },

    /// Check all tracked files for drift (content changes since last hash)
    #[command(long_about = "\
Re-hash all tracked files and compare against stored CIDs.

Reports: OK (unchanged), DRIFTED (content changed), MISSING (file deleted).
Agents should run this before reasoning on context to detect stale state.

Example:
  ket drift")]
    Drift,

    /// Garbage collect unreferenced CAS blobs
    #[command(long_about = "\
Find and optionally delete CAS blobs not referenced by any DAG node.

A blob is referenced if it's a DAG node, a node's output content, or
a node's parent. Everything else is an orphan (e.g., raw puts without
DAG nodes).

Default is dry run — use --delete to actually remove.

Examples:
  ket gc                   Dry run — show what would be deleted
  ket gc --delete          Actually delete orphan blobs")]
    Gc {
        /// Actually delete unreferenced blobs (default: dry run)
        #[arg(long)]
        delete: bool,
    },

    /// Export a DAG subgraph as a portable JSON bundle
    #[command(long_about = "\
Bundle a DAG node and all its ancestors into a self-contained JSON file.

The bundle includes serialized nodes and their content (base64-encoded),
so it can be imported into another ket instance without any shared state.

Examples:
  ket export <cid> -o reasoning.json    Export to file
  ket export <cid> > bundle.json        Export to stdout
  ket export <cid> | ket import -       Pipe between instances")]
    Export {
        /// Root node CID (this node + all ancestors are included)
        cid: String,
        /// Output file path (default: stdout)
        #[arg(short, long)]
        out: Option<String>,
    },

    /// Import a DAG bundle from another ket instance
    #[command(long_about = "\
Ingest a DAG bundle, storing all nodes and content in the local CAS.

Imported nodes are also synced to SQL if Dolt is available.
Deduplication is automatic — existing blobs are skipped.

Example:
  ket import reasoning.json")]
    Import {
        /// Path to the bundle JSON file
        path: String,
    },

    /// Create a merge node combining multiple parent lineages
    #[command(long_about = "\
Create a DAG node with multiple parents, synthesizing divergent branches.

Useful when two agents work independently and you want to create a
convergent node that captures both reasoning chains.

Examples:
  ket merge 'synthesis' --parents <cid1> <cid2> --agent human
  ket merge 'combined review' --parents <a> <b> <c> --kind reasoning")]
    Merge {
        /// Content for the merge node
        content: String,
        /// Parent CIDs to merge (at least 2 required)
        #[arg(long, required = true, num_args = 2..)]
        parents: Vec<String>,
        /// Node kind (memory, code, reasoning, task, cdom, score, context)
        #[arg(long, default_value = "reasoning")]
        kind: String,
        /// Agent name
        #[arg(long, default_value = "human")]
        agent: String,
    },

    /// Show CAS store statistics (blob counts, sizes, breakdown)
    #[command(long_about = "\
Display detailed CAS store statistics.

Shows total blobs, byte size, and a breakdown into DAG nodes,
content blobs (referenced by nodes), and orphan blobs (unreferenced).

Example:
  ket cas-stats")]
    CasStats,

    /// Output DAG as Graphviz DOT for visualization
    #[command(long_about = "\
Generate Graphviz DOT output for visualizing the DAG structure.

Nodes are color-coded by kind. Soft links appear as dashed edges.
Pipe to 'dot' to render: ket dot | dot -Tpng -o dag.png

Examples:
  ket dot                          Full DAG
  ket dot --root <cid>             Subgraph from a specific node
  ket dot | dot -Tsvg -o dag.svg   Render as SVG")]
    Dot {
        /// Scope output to this node and its ancestors
        #[arg(long)]
        root: Option<String>,
    },

    /// Full-text search across all CAS content
    #[command(long_about = "\
Search for text across all content stored in CAS.

Case-insensitive substring search. Shows matching CIDs with context lines.
Useful for finding which memories or artifacts mention specific topics.

Examples:
  ket search 'authentication'
  ket search 'TODO' --limit 50
  ket --json search 'error handling'")]
    Search {
        /// Text to search for (case-insensitive substring match)
        query: String,
        /// Maximum number of matching blobs to return
        #[arg(short, long, default_value = "20")]
        limit: usize,
    },

    /// Named snapshots for bookmarking known-good DAG states
    #[command(long_about = "\
Create, list, and verify named snapshots of the DAG.

A snapshot records all current DAG node CIDs. Later, 'verify' re-checks
that all nodes and their content still exist and are uncorrupted.

Examples:
  ket snapshot create v1           Bookmark current state
  ket snapshot ls                  List all snapshots
  ket snapshot verify v1           Check integrity against snapshot")]
    Snapshot {
        #[command(subcommand)]
        action: SnapshotAction,
    },
}

#[derive(Subcommand)]
enum SnapshotAction {
    /// Create a named snapshot of the current DAG state
    Create {
        /// Snapshot name (e.g., v1, pre-refactor, baseline)
        name: String,
    },
    /// List all saved snapshots
    Ls,
    /// Verify all nodes in a snapshot still exist and are uncorrupted
    Verify {
        /// Snapshot name to verify against
        name: String,
    },
}

#[derive(Subcommand)]
enum DagAction {
    /// List all DAG nodes (summary view)
    Ls,
    /// Show full details of a specific node
    Show {
        /// Node CID (64-char hex)
        cid: String,
    },
    /// Create a new DAG node with content
    Create {
        /// Content string to store (becomes the node's output)
        content: String,
        /// Node kind: memory, code, reasoning, task, cdom, score, context
        #[arg(long, default_value = "code")]
        kind: String,
        /// Agent that produced this (human, claude, codex, copilot)
        #[arg(long, default_value = "human")]
        agent: String,
        /// Parent node CIDs (can specify multiple for merge)
        #[arg(long)]
        parent: Vec<String>,
    },
    /// Trace the full ancestry of a node
    Lineage {
        /// Node CID to trace from
        cid: String,
    },
    /// Check if a file has changed since a CID was recorded
    Drift {
        /// File path to check
        path: String,
        /// CID that was recorded when the file was last known-good
        cid: String,
    },
}

#[derive(Subcommand)]
enum TaskAction {
    /// Create a task
    Create {
        /// Task title
        title: String,
        /// Creator
        #[arg(long, default_value = "human")]
        by: String,
    },
    /// List tasks
    Ls,
    /// Show task details
    Show {
        /// Task ID
        id: String,
    },
    /// Assign task to agent
    Assign {
        /// Task ID
        id: String,
        /// Agent name
        agent: String,
    },
}

#[derive(Subcommand)]
enum AgentAction {
    /// Register an agent
    Register {
        /// Agent name (claude, codex, copilot)
        name: String,
    },
    /// List agents
    Ls,
}

#[derive(Subcommand)]
enum ScoreAction {
    /// Record a score for a DAG node
    Add {
        /// CID of the node being scored
        node_cid: String,
        /// Agent that produced the node's content
        #[arg(long)]
        agent: String,
        /// Who is recording this score (human, auto:compile, peer:claude)
        #[arg(long, default_value = "human")]
        scorer: String,
        /// Scoring dimension: correctness, efficiency, style, completeness
        #[arg(long)]
        dim: String,
        /// Score value between 0.0 (worst) and 1.0 (best)
        #[arg(long)]
        value: f64,
        /// Free-text evidence or justification
        #[arg(long, default_value = "")]
        evidence: String,
    },
    /// Show all scores recorded for a node
    Show {
        /// Node CID
        node_cid: String,
    },
    /// Show an agent's aggregated scoring profile (averages per dimension)
    Profile {
        /// Agent name (claude, codex, copilot, human)
        agent: String,
    },
    /// Find the best agent for a given dimension based on historical scores
    Route {
        /// Dimension to optimize for: correctness, efficiency, style, completeness
        dim: String,
    },
    /// Auto-score by running cargo build, test, and clippy
    #[command(long_about = "\
Automatically score a node by running build tools in the working directory.

Runs: cargo build (correctness), cargo test (completeness), cargo clippy (style).
Records scores and returns results.")]
    Auto {
        /// Node CID to score
        node_cid: String,
        /// Agent that produced the code
        #[arg(long)]
        agent: String,
        /// Working directory containing the Cargo project
        #[arg(long, default_value = ".")]
        dir: String,
    },
}

#[derive(Subcommand)]
enum CalibrateAction {
    /// Run WQS calibration on a DAG subtree
    Run {
        /// Root CID of the subtree to calibrate
        root: String,
        /// Maximum total compute cost
        #[arg(long, default_value = "50.0")]
        max_cost: f64,
        /// Maximum depth to explore
        #[arg(long, default_value = "20")]
        max_depth: u32,
        /// Maximum number of Tier 3 (Deep) calls
        #[arg(long, default_value = "5")]
        max_tier3: u32,
        /// Agent name
        #[arg(long, default_value = "claude")]
        agent: String,
    },
    /// Inspect a stored calibration result
    Inspect {
        /// CID of the calibration DAG node
        cid: String,
    },
    /// Show calibration history for a subtree root
    History {
        /// Root CID
        root: String,
    },
}

#[derive(Subcommand)]
enum LinkAction {
    /// Create a soft link between two nodes
    Create {
        /// Source node CID
        from: String,
        /// Target node CID
        to: String,
        /// Relation type (e.g., supersedes, related_to, contradicts, refines)
        relation: String,
    },
    /// List all soft links involving a node (both directions)
    Ls {
        /// Node CID to query
        cid: String,
    },
}

#[derive(Subcommand)]
enum TrackAction {
    /// Track a file for drift detection
    Add {
        /// File path to track
        path: String,
        /// Agent tracking this file
        #[arg(long, default_value = "human")]
        agent: String,
    },
    /// List tracked files
    Ls,
    /// Remove a file from tracking
    Rm {
        /// File path to untrack
        path: String,
    },
}

fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let base = ket_dir(&cli.home);

    match cli.command {
        Commands::Init => cmd_init(&base, cli.json),
        Commands::Put { path } => cmd_put(&base, &path, cli.json),
        Commands::Get { cid } => cmd_get(&base, &cid, cli.json),
        Commands::Verify { cid } => cmd_verify(&base, &cid, cli.json),
        Commands::Dag { action } => cmd_dag(&base, action, cli.json),
        Commands::Sql { query } => cmd_sql(&base, &query, cli.json),
        Commands::Task { action } => cmd_task(&base, action, cli.json),
        Commands::Agent { action } => cmd_agent(&base, action, cli.json),
        Commands::Run {
            task_id,
            agent,
            prompt,
        } => cmd_run(&base, &task_id, &agent, prompt.as_deref(), cli.json),
        Commands::Scan { path } => cmd_scan(&base, &path, cli.json),
        Commands::Cdom { query, path } => cmd_cdom(&base, &query, path.as_deref(), cli.json),
        Commands::Log { n } => cmd_log(&base, n, cli.json),
        Commands::Mcp => cmd_mcp(&base),
        Commands::Scores { action } => cmd_scores(&base, action, cli.json),
        Commands::Calibrate { action } => cmd_calibrate(&base, action, cli.json),
        Commands::Repair { dry_run } => cmd_repair(&base, dry_run, cli.json),
        Commands::Status => cmd_status(&base, cli.json),
        Commands::History { n } => cmd_history(&base, n, cli.json),
        Commands::Diff { from, to } => cmd_diff(&base, from.as_deref(), to.as_deref(), cli.json),
        Commands::Link { action } => cmd_link(&base, action, cli.json),
        Commands::Track { action } => cmd_track(&base, action, cli.json),
        Commands::Drift => cmd_drift(&base, cli.json),
        Commands::Gc { delete } => cmd_gc(&base, delete, cli.json),
        Commands::Export { cid, out } => cmd_export(&base, &cid, out.as_deref(), cli.json),
        Commands::Import { path } => cmd_import(&base, &path, cli.json),
        Commands::Merge {
            content,
            parents,
            kind,
            agent,
        } => cmd_merge(&base, &content, &parents, &kind, &agent, cli.json),
        Commands::CasStats => cmd_cas_stats(&base, cli.json),
        Commands::Dot { root } => cmd_dot(&base, root.as_deref()),
        Commands::Search { query, limit } => cmd_search(&base, &query, limit, cli.json),
        Commands::Snapshot { action } => cmd_snapshot(&base, action, cli.json),
    }
}

fn cmd_init(base: &PathBuf, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    if base.exists() {
        return Err(format!("{} already exists", base.display()).into());
    }

    fs::create_dir_all(base)?;

    // Create CAS directory
    let cas_dir = base.join("cas");
    ket_cas::Store::init(&cas_dir)?;

    // Create empty log and manifest
    fs::write(base.join("log"), "")?;
    fs::write(base.join("manifest"), "")?;

    // Create config.yaml
    let config = serde_json::json!({
        "agents": {
            "claude": {
                "cli_command": "claude -p --output-format json",
                "mcp_capable": true,
                "model": "claude-sonnet-4-5-20250929"
            }
        }
    });
    fs::write(
        base.join("config.yaml"),
        serde_json::to_string_pretty(&config)?,
    )?;

    // Try to init Dolt DB
    let db_path = base.join("ket.db");
    let dolt_result = ket_sql::DoltDb::init(&db_path);

    if json {
        let dolt_ok = dolt_result.is_ok();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "initialized": true,
                "path": base.display().to_string(),
                "cas": cas_dir.display().to_string(),
                "dolt": dolt_ok,
            }))?
        );
    } else {
        println!("Initialized ket at {}", base.display());
        println!("  CAS store: {}", cas_dir.display());
        match dolt_result {
            Ok(_) => println!("  Dolt DB:   {}", db_path.display()),
            Err(e) => println!("  Dolt DB:   skipped ({e})"),
        }
    }

    // Log the init event
    let log_path = base.join("log");
    log::append(&log_path, "init", &base.display().to_string())?;

    Ok(())
}

fn cmd_put(base: &PathBuf, path: &str, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let cas = open_cas(base)?;

    let cid = if path == "-" {
        let mut data = Vec::new();
        std::io::Read::read_to_end(&mut std::io::stdin(), &mut data)?;
        cas.put(&data)?
    } else {
        cas.put_file(std::path::Path::new(path))?
    };

    // Log the put
    let log_path = base.join("log");
    log::append(&log_path, "put", &format!("{path} -> {cid}"))?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "cid": cid.as_str(),
                "path": path,
            }))?
        );
    } else {
        println!("{cid}");
    }

    Ok(())
}

fn cmd_get(base: &PathBuf, cid: &str, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let cas = open_cas(base)?;
    let data = cas.get(&ket_cas::Cid::from(cid))?;

    if json {
        let content = String::from_utf8_lossy(&data).into_owned();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "cid": cid,
                "content": content,
                "size": data.len(),
            }))?
        );
    } else {
        std::io::Write::write_all(&mut std::io::stdout(), &data)?;
    }

    Ok(())
}

fn cmd_verify(base: &PathBuf, cid: &str, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let cas = open_cas(base)?;
    let valid = cas.verify(&ket_cas::Cid::from(cid))?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "cid": cid,
                "valid": valid,
            }))?
        );
    } else if valid {
        println!("OK: {cid}");
    } else {
        println!("CORRUPTED: {cid}");
        std::process::exit(1);
    }

    Ok(())
}

fn cmd_dag(
    base: &PathBuf,
    action: DagAction,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let cas = open_cas(base)?;
    let dag = ket_dag::Dag::new(&cas);

    match action {
        DagAction::Ls => {
            // List all CIDs in CAS that are valid DAG nodes
            let cids = cas.list()?;
            let mut nodes = Vec::new();
            for cid in &cids {
                if let Ok(node) = dag.get_node(cid) {
                    nodes.push((cid.clone(), node));
                }
            }

            if json {
                let items: Vec<serde_json::Value> = nodes
                    .iter()
                    .map(|(cid, node)| {
                        serde_json::json!({
                            "cid": cid.as_str(),
                            "kind": node.kind.to_string(),
                            "agent": node.agent,
                            "timestamp": node.timestamp,
                            "output_cid": node.output_cid.as_str(),
                            "parents": node.parents.len(),
                        })
                    })
                    .collect();
                println!("{}", serde_json::to_string_pretty(&items)?);
            } else if nodes.is_empty() {
                println!("No DAG nodes.");
            } else {
                for (cid, node) in &nodes {
                    println!(
                        "{}  {}  {}  {}  parents:{}",
                        &cid.as_str()[..12],
                        node.kind,
                        node.agent,
                        &node.timestamp[..19],
                        node.parents.len()
                    );
                }
            }
        }
        DagAction::Show { cid } => {
            let node = dag.get_node(&ket_cas::Cid::from(cid.as_str()))?;
            if json {
                println!("{}", serde_json::to_string_pretty(&node)?);
            } else {
                println!("CID:        {cid}");
                println!("Kind:       {}", node.kind);
                println!("Agent:      {}", node.agent);
                println!("Timestamp:  {}", node.timestamp);
                println!("Output CID: {}", node.output_cid);
                println!("Parents:    {}", node.parents.len());
                for (i, p) in node.parents.iter().enumerate() {
                    println!("  [{i}] {p}");
                }
                if !node.meta.is_empty() {
                    println!("Meta:");
                    for (k, v) in &node.meta {
                        println!("  {k}: {v}");
                    }
                }
            }
        }
        DagAction::Create {
            content,
            kind,
            agent,
            parent,
        } => {
            let node_kind = match kind.as_str() {
                "memory" => ket_dag::NodeKind::Memory,
                "code" => ket_dag::NodeKind::Code,
                "reasoning" => ket_dag::NodeKind::Reasoning,
                "task" => ket_dag::NodeKind::Task,
                "cdom" => ket_dag::NodeKind::Cdom,
                "score" => ket_dag::NodeKind::Score,
                "context" => ket_dag::NodeKind::Context,
                _ => return Err(format!("Unknown kind: {kind}").into()),
            };

            let parents: Vec<ket_cas::Cid> = parent.into_iter().map(ket_cas::Cid::from).collect();
            let (node_cid, content_cid) =
                dag.store_with_node(content.as_bytes(), node_kind, parents.clone(), &agent)?;

            // Dual-write to SQL if Dolt is available (single transaction)
            if let Ok(db) = open_db(base) {
                let node = dag.get_node(&node_cid)?;
                let parent_refs: Vec<(&str, i32)> = parents
                    .iter()
                    .enumerate()
                    .map(|(i, p)| (p.as_str(), i as i32))
                    .collect();
                let _ = db.sync_dag_node(
                    node_cid.as_str(),
                    &kind,
                    &agent,
                    &node.timestamp,
                    content_cid.as_str(),
                    "",
                    &parent_refs,
                );
            }

            // Log
            let log_path = base.join("log");
            log::append(&log_path, "dag:create", &format!("{node_cid}"))?;

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "node_cid": node_cid.as_str(),
                        "content_cid": content_cid.as_str(),
                    }))?
                );
            } else {
                println!("Node CID:    {node_cid}");
                println!("Content CID: {content_cid}");
            }
        }
        DagAction::Lineage { cid } => {
            let lineage = dag.lineage(&ket_cas::Cid::from(cid.as_str()))?;
            if json {
                let items: Vec<serde_json::Value> = lineage
                    .iter()
                    .map(|(cid, node)| {
                        serde_json::json!({
                            "cid": cid.as_str(),
                            "kind": node.kind.to_string(),
                            "agent": node.agent,
                            "timestamp": node.timestamp,
                        })
                    })
                    .collect();
                println!("{}", serde_json::to_string_pretty(&items)?);
            } else if lineage.is_empty() {
                println!("No lineage found.");
            } else {
                for (i, (cid, node)) in lineage.iter().enumerate() {
                    let indent = "  ".repeat(i);
                    println!(
                        "{indent}{}  {}  {}",
                        &cid.as_str()[..12],
                        node.kind,
                        node.agent
                    );
                }
            }
        }
        DagAction::Drift { path, cid } => {
            let drifted = dag.check_drift(
                std::path::Path::new(&path),
                &ket_cas::Cid::from(cid.as_str()),
            )?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "path": path,
                        "expected_cid": cid,
                        "drifted": drifted,
                    }))?
                );
            } else if drifted {
                println!("DRIFTED: {path} has changed since CID {}", &cid[..12]);
                std::process::exit(1);
            } else {
                println!("OK: {path} matches CID {}", &cid[..12]);
            }
        }
    }

    Ok(())
}

fn cmd_sql(base: &PathBuf, query: &str, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let db = open_db(base)?;
    let result = db.query(query)?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "query": query,
                "result": result.trim(),
            }))?
        );
    } else {
        print!("{result}");
    }

    Ok(())
}

fn cmd_task(
    base: &PathBuf,
    action: TaskAction,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let cas = open_cas(base)?;
    let db = open_db(base)?;
    let orch = ket_agent::Orchestrator::new(&cas, &db);

    match action {
        TaskAction::Create { title, by } => {
            let id = orch.create_task(&title, &by, None, None)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "id": id,
                        "title": title,
                        "status": "pending",
                    }))?
                );
            } else {
                println!("Created task: {id}");
            }
        }
        TaskAction::Ls => {
            let result = orch.list_tasks()?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "tasks": result.trim(),
                    }))?
                );
            } else {
                print!("{result}");
            }
        }
        TaskAction::Show { id } => {
            let result = db.query(&format!(
                "SELECT * FROM tasks WHERE id = '{id}'"
            ))?;
            print!("{result}");
        }
        TaskAction::Assign { id, agent } => {
            orch.assign_task(&id, &agent)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "id": id,
                        "assigned_to": agent,
                    }))?
                );
            } else {
                println!("Assigned task {id} to {agent}");
            }
        }
    }

    Ok(())
}

fn cmd_agent(
    base: &PathBuf,
    action: AgentAction,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let cas = open_cas(base)?;
    let db = open_db(base)?;
    let orch = ket_agent::Orchestrator::new(&cas, &db);

    match action {
        AgentAction::Register { name } => {
            let config = match name.as_str() {
                "claude" => ket_agent::AgentConfig::claude(),
                "codex" => ket_agent::AgentConfig::codex(),
                "copilot" => ket_agent::AgentConfig::copilot(),
                _ => {
                    return Err(format!("Unknown agent preset: {name}. Use claude/codex/copilot").into());
                }
            };
            orch.register_agent(&config)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&config)?);
            } else {
                println!("Registered agent: {name}");
            }
        }
        AgentAction::Ls => {
            let result = orch.list_agents()?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "agents": result.trim(),
                    }))?
                );
            } else {
                print!("{result}");
            }
        }
    }

    Ok(())
}

fn cmd_run(
    base: &PathBuf,
    task_id: &str,
    agent: &str,
    prompt: Option<&str>,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let cas = open_cas(base)?;
    let db = open_db(base)?;
    let orch = ket_agent::Orchestrator::new(&cas, &db);

    let prompt = prompt.unwrap_or("Complete the assigned task");

    let rt = tokio::runtime::Runtime::new()?;
    let result_cid = rt.block_on(orch.run_task(task_id, prompt, agent, None))?;

    // Log the run
    let log_path = base.join("log");
    log::append(&log_path, "run", &format!("{task_id} -> {result_cid}"))?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "task_id": task_id,
                "agent": agent,
                "result_cid": result_cid.as_str(),
            }))?
        );
    } else {
        println!("Result CID: {result_cid}");
    }

    Ok(())
}

fn cmd_scan(base: &PathBuf, path: &str, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let cas = open_cas(base)?;
    let db = open_db(base).ok(); // SQL index is optional
    let file_path = std::path::Path::new(path);

    let mut snapshots = Vec::new();
    if file_path.is_dir() {
        scan_dir(file_path, &cas, &mut snapshots)?;
    } else {
        match ket_cdom::scan_file(file_path, &cas) {
            Ok(snapshot) => snapshots.push(snapshot),
            Err(e) => return Err(format!("{}: {e}", file_path.display()).into()),
        }
    }

    // Write to SQL symbol index if available
    let mut indexed = 0usize;
    if let Some(ref db) = db {
        for snap in &snapshots {
            let sym_tuples: Vec<(String, String, usize, usize, Option<String>)> = snap
                .symbols
                .iter()
                .map(|s| {
                    (
                        s.name.clone(),
                        s.kind.to_string(),
                        s.start_line,
                        s.end_line,
                        s.parent.clone(),
                    )
                })
                .collect();
            if db
                .sync_cdom_symbols(&snap.file_path, snap.content_cid.as_str(), &sym_tuples)
                .is_ok()
            {
                indexed += 1;
            }
        }
        // Also track each file for drift detection
        for snap in &snapshots {
            let _ = db.track_context_file(&snap.file_path, snap.content_cid.as_str(), "cdom");
        }
    }

    let total_symbols: usize = snapshots.iter().map(|s| s.symbols.len()).sum();

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "files": snapshots.len(),
                "symbols": total_symbols,
                "indexed": indexed,
                "snapshots": snapshots,
            }))?
        );
    } else {
        for snap in &snapshots {
            println!(
                "{} ({}) — {} symbols",
                snap.file_path, snap.language, snap.symbols.len()
            );
            for sym in &snap.symbols {
                let parent = sym.parent.as_deref().unwrap_or("");
                let parent_str = if parent.is_empty() {
                    String::new()
                } else {
                    format!(" [{}]", parent)
                };
                println!(
                    "  {}:{}-{} {}{}",
                    sym.kind, sym.start_line, sym.end_line, sym.name, parent_str
                );
            }
        }
        println!(
            "\n{} files, {} symbols{}",
            snapshots.len(),
            total_symbols,
            if indexed > 0 {
                format!(", {} indexed in SQL", indexed)
            } else {
                String::new()
            }
        );
    }

    Ok(())
}

fn scan_dir(
    dir: &std::path::Path,
    cas: &ket_cas::Store,
    snapshots: &mut Vec<ket_cdom::CdomSnapshot>,
) -> Result<(), Box<dyn std::error::Error>> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            if name.starts_with('.')
                || name == "target"
                || name == "node_modules"
                || name == "__pycache__"
            {
                continue;
            }
            scan_dir(&path, cas, snapshots)?;
        } else if ket_cdom::detect_language(&path).is_some() {
            match ket_cdom::scan_file(&path, cas) {
                Ok(snapshot) => snapshots.push(snapshot),
                Err(e) => eprintln!("Warning: {}: {e}", path.display()),
            }
        }
    }
    Ok(())
}

fn cmd_cdom(
    base: &PathBuf,
    query: &str,
    path: Option<&str>,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // If a specific file given, parse it directly
    if let Some(p) = path {
        let symbols = ket_cdom::parse_file(std::path::Path::new(p))?;
        let matches = ket_cdom::query_symbols(&symbols, query);

        if json {
            let results: Vec<serde_json::Value> = matches
                .iter()
                .map(|s| {
                    serde_json::json!({
                        "file": p,
                        "name": s.name,
                        "kind": s.kind.to_string(),
                        "start_line": s.start_line,
                        "end_line": s.end_line,
                        "parent": s.parent,
                    })
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&results)?);
        } else if matches.is_empty() {
            println!("No symbols matching '{query}' in {p}");
        } else {
            for s in matches {
                println!(
                    "  {p}:{}:{} {} {}",
                    s.start_line,
                    s.kind,
                    s.name,
                    s.parent.as_deref().unwrap_or("")
                );
            }
        }
        return Ok(());
    }

    // No file specified — search the SQL index
    let db = open_db(base)?;
    let result = db.search_symbols(query)?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "query": query,
                "results": result.trim(),
            }))?
        );
    } else if result.lines().count() <= 1 {
        println!("No indexed symbols matching '{query}'. Run `ket scan <dir>` first.");
    } else {
        print!("{result}");
    }

    Ok(())
}

fn cmd_log(base: &PathBuf, n: usize, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let log_path = base.join("log");
    let entries = log::read(&log_path, n)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&entries)?);
    } else if entries.is_empty() {
        println!("No log entries.");
    } else {
        for entry in &entries {
            println!("{}", entry);
        }
    }

    Ok(())
}

fn cmd_mcp(base: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let cas = open_cas(base)?;
    let db = open_db(base)?;
    ket_mcp::run_stdio_server(&cas, &db)?;
    Ok(())
}

fn cmd_scores(
    base: &PathBuf,
    action: ScoreAction,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = open_db(base)?;
    let engine = ket_score::ScoringEngine::new(&db);

    match action {
        ScoreAction::Add {
            node_cid,
            agent,
            scorer,
            dim,
            value,
            evidence,
        } => {
            let dimension = ket_score::Dimension::parse(&dim)?;
            let score = ket_score::Score::new(
                ket_cas::Cid::from(node_cid.as_str()),
                &agent,
                &scorer,
                dimension,
                value,
                &evidence,
            )?;
            engine.record(&score)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "recorded": true,
                        "node_cid": node_cid,
                        "dimension": dim,
                        "value": value,
                    }))?
                );
            } else {
                println!("Recorded {dim}={value} for {}", &node_cid[..12]);
            }
        }
        ScoreAction::Show { node_cid } => {
            let result = engine.scores_for(&ket_cas::Cid::from(node_cid.as_str()))?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "node_cid": node_cid,
                        "scores": result.trim(),
                    }))?
                );
            } else {
                print!("{result}");
            }
        }
        ScoreAction::Profile { agent } => {
            let result = engine.agent_profile(&agent)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "agent": agent,
                        "profile": result.trim(),
                    }))?
                );
            } else {
                println!("Agent: {agent}");
                print!("{result}");
            }
        }
        ScoreAction::Route { dim } => {
            let result = engine.route(&dim)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "dimension": dim,
                        "best": result.trim(),
                    }))?
                );
            } else {
                println!("Best agent for {dim}:");
                print!("{result}");
            }
        }
        ScoreAction::Auto {
            node_cid,
            agent,
            dir,
        } => {
            let results = engine.auto_score_code(
                &ket_cas::Cid::from(node_cid.as_str()),
                &agent,
                std::path::Path::new(&dir),
            )?;
            if json {
                println!("{}", serde_json::to_string_pretty(&results)?);
            } else {
                for r in &results {
                    let icon = if r.value >= 0.8 {
                        "PASS"
                    } else if r.value >= 0.3 {
                        "WARN"
                    } else {
                        "FAIL"
                    };
                    println!(
                        "  [{icon}] {}: {:.1} — {}",
                        r.dimension.as_str(),
                        r.value,
                        r.evidence
                    );
                }
            }
        }
    }

    Ok(())
}

fn cmd_calibrate(
    base: &PathBuf,
    action: CalibrateAction,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let cas = open_cas(base)?;
    let db = open_db(base)?;
    let dag = ket_dag::Dag::new(&cas);

    match action {
        CalibrateAction::Run {
            root,
            max_cost,
            max_depth,
            max_tier3,
            agent,
        } => {
            let constraints = ket_opt::Constraints {
                max_cost,
                max_depth,
                max_tier3_calls: max_tier3,
            };
            let root_cid = ket_cas::Cid::from(root.as_str());
            let (node_cid, result) =
                ket_opt::calibrate(&cas, &dag, &db, &root_cid, &constraints, &agent)?;

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "node_cid": node_cid.as_str(),
                        "result": result,
                    }))?
                );
            } else {
                println!("Calibration stored: {}", node_cid.as_str());
                println!("  Root:       {}", &result.root_cid[..12.min(result.root_cid.len())]);
                println!("  Gain:       {:.3}", result.total_gain);
                println!("  Cost:       {:.3}", result.total_cost);
                println!("  Iterations: {}", result.iterations);
                println!("  Lambdas:    cost={:.4} depth={:.4} tier3={:.4}",
                    result.lambdas.lambda_cost,
                    result.lambdas.lambda_depth,
                    result.lambdas.lambda_tier3,
                );
                println!("  Nodes:      {}", result.allocated_tiers.len());
            }
        }
        CalibrateAction::Inspect { cid } => {
            let result = ket_opt::inspect_calibration(&db, &cid)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else {
                println!("Calibration: {}", &cid[..12.min(cid.len())]);
                println!("  Root:       {}", &result.root_cid[..12.min(result.root_cid.len())]);
                println!("  Gain:       {:.3}", result.total_gain);
                println!("  Cost:       {:.3}", result.total_cost);
                println!("  Iterations: {}", result.iterations);
                println!("  Lambdas:    cost={:.4} depth={:.4} tier3={:.4}",
                    result.lambdas.lambda_cost,
                    result.lambdas.lambda_depth,
                    result.lambdas.lambda_tier3,
                );
            }
        }
        CalibrateAction::History { root } => {
            let results = ket_opt::calibration_history(&db, &root)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&results)?);
            } else {
                println!("Calibration history for {}:", &root[..12.min(root.len())]);
                for (i, r) in results.iter().enumerate() {
                    println!(
                        "  [{}] gain={:.3} cost={:.3} iters={} lambda_cost={:.4}",
                        i, r.total_gain, r.total_cost, r.iterations, r.lambdas.lambda_cost,
                    );
                }
                if results.is_empty() {
                    println!("  (none)");
                }
            }
        }
    }

    Ok(())
}

fn cmd_repair(
    base: &PathBuf,
    dry_run: bool,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let cas = open_cas(base)?;
    let db = open_db(base)?;
    let dag = ket_dag::Dag::new(&cas);

    // Scan all CAS blobs, find valid DAG nodes
    let cids = cas.list()?;
    let mut synced = 0u64;
    let mut skipped = 0u64;
    let mut errors = 0u64;

    for cid in &cids {
        // Try to parse as a DagNode
        let node = match dag.get_node(cid) {
            Ok(n) => n,
            Err(_) => continue, // raw content blob, not a node
        };

        // Check if already in SQL
        match db.dag_node_exists(cid.as_str()) {
            Ok(true) => {
                skipped += 1;
                continue;
            }
            Ok(false) => {}
            Err(_) => {
                errors += 1;
                continue;
            }
        }

        if dry_run {
            if !json {
                println!(
                    "  would sync: {}  {}  {}",
                    &cid.as_str()[..12],
                    node.kind,
                    node.agent
                );
            }
            synced += 1;
            continue;
        }

        // Sync node + edges in one transaction
        let parent_refs: Vec<(&str, i32)> = node
            .parents
            .iter()
            .enumerate()
            .map(|(i, p)| (p.as_str(), i as i32))
            .collect();

        match db.sync_dag_node(
            cid.as_str(),
            &node.kind.to_string(),
            &node.agent,
            &node.timestamp,
            node.output_cid.as_str(),
            "",
            &parent_refs,
        ) {
            Ok(()) => {
                synced += 1;
                if !json {
                    println!(
                        "  synced: {}  {}  {}",
                        &cid.as_str()[..12],
                        node.kind,
                        node.agent
                    );
                }
            }
            Err(e) => {
                errors += 1;
                if !json {
                    eprintln!("  error: {}: {e}", &cid.as_str()[..12]);
                }
            }
        }
    }

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "dry_run": dry_run,
                "synced": synced,
                "skipped": skipped,
                "errors": errors,
            }))?
        );
    } else {
        let verb = if dry_run { "would sync" } else { "synced" };
        println!(
            "\nRepair: {synced} {verb}, {skipped} already in sync, {errors} errors"
        );
    }

    // Log the repair
    if !dry_run {
        let log_path = base.join("log");
        log::append(
            &log_path,
            "repair",
            &format!("synced={synced} skipped={skipped} errors={errors}"),
        )?;
    }

    Ok(())
}

fn cmd_status(base: &PathBuf, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let cas_exists = base.join("cas").exists();
    let db_exists = base.join("ket.db").join(".dolt").exists();

    let cas_blobs = if cas_exists {
        let cas = open_cas(base)?;
        cas.list()?.len()
    } else {
        0
    };

    if db_exists {
        let db = open_db(base)?;
        let stats = db.stats()?;
        let head = db.dolt_head().unwrap_or_else(|_| "unknown".into());

        if json {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "path": base.display().to_string(),
                    "cas_blobs": cas_blobs,
                    "dolt_head": head,
                    "db": stats,
                }))?
            );
        } else {
            println!("ket status: {}", base.display());
            println!("  CAS blobs:     {cas_blobs}");
            println!("  Dolt HEAD:     {head}");
            println!("  DAG nodes:     {}", stats.nodes);
            println!("  DAG edges:     {}", stats.edges);
            println!("  Tasks:         {}", stats.tasks);
            println!("  Agents:        {}", stats.agents);
            println!("  Scores:        {}", stats.scores);
            println!("  Soft links:    {}", stats.soft_links);
            println!("  Context files: {}", stats.context_files);
            println!("  CDOM symbols:  {}", stats.symbols);
            println!("  Calibrations:  {}", stats.calibrations);
        }
    } else if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "path": base.display().to_string(),
                "cas_blobs": cas_blobs,
                "dolt": false,
            }))?
        );
    } else {
        println!("ket status: {}", base.display());
        println!("  CAS blobs: {cas_blobs}");
        println!("  Dolt:      not initialized");
    }

    Ok(())
}

fn cmd_history(base: &PathBuf, n: usize, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let db = open_db(base)?;
    let history = db.dolt_log(n)?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "history": history.trim(),
            }))?
        );
    } else {
        print!("{history}");
    }

    Ok(())
}

fn cmd_diff(
    base: &PathBuf,
    from: Option<&str>,
    to: Option<&str>,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = open_db(base)?;
    let diff = db.dolt_diff(from, to)?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "from": from,
                "to": to,
                "diff": diff.trim(),
            }))?
        );
    } else if diff.trim().is_empty() {
        println!("No changes.");
    } else {
        print!("{diff}");
    }

    Ok(())
}

fn cmd_link(
    base: &PathBuf,
    action: LinkAction,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = open_db(base)?;

    match action {
        LinkAction::Create { from, to, relation } => {
            db.insert_soft_link(&from, &to, &relation)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "from": from,
                        "to": to,
                        "relation": relation,
                    }))?
                );
            } else {
                println!(
                    "Linked {} --[{}]--> {}",
                    &from[..12.min(from.len())],
                    relation,
                    &to[..12.min(to.len())]
                );
            }
        }
        LinkAction::Ls { cid } => {
            let result = db.soft_links_for(&cid)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "cid": cid,
                        "links": result.trim(),
                    }))?
                );
            } else if result.lines().count() <= 1 {
                println!("No soft links for {}", &cid[..12.min(cid.len())]);
            } else {
                print!("{result}");
            }
        }
    }

    Ok(())
}

fn cmd_track(
    base: &PathBuf,
    action: TrackAction,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = open_db(base)?;

    match action {
        TrackAction::Add { path, agent } => {
            let file_path = std::path::Path::new(&path);
            let cid = ket_cas::hash_file(file_path)?;
            db.track_context_file(&path, cid.as_str(), &agent)?;

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "path": path,
                        "cid": cid.as_str(),
                        "agent": agent,
                    }))?
                );
            } else {
                println!("Tracking {} (CID: {})", path, &cid.as_str()[..12]);
            }
        }
        TrackAction::Ls => {
            let result = db.list_context_files()?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "files": result.trim(),
                    }))?
                );
            } else if result.lines().count() <= 1 {
                println!("No tracked files. Use `ket track add <file>` to start tracking.");
            } else {
                print!("{result}");
            }
        }
        TrackAction::Rm { path } => {
            db.untrack_context_file(&path)?;
            if !json {
                println!("Untracked {path}");
            }
        }
    }

    Ok(())
}

fn cmd_drift(base: &PathBuf, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let db = open_db(base)?;
    let tracked = db.list_context_files()?;

    let mut drifted = Vec::new();
    let mut ok = Vec::new();
    let mut missing = Vec::new();

    // Parse CSV: path,cid,tracked_at,agent
    for line in tracked.lines().skip(1) {
        let parts: Vec<&str> = line.splitn(4, ',').collect();
        if parts.len() < 2 {
            continue;
        }
        let path = parts[0];
        let expected_cid = parts[1];

        let file_path = std::path::Path::new(path);
        if !file_path.exists() {
            missing.push(path.to_string());
            continue;
        }

        match ket_cas::hash_file(file_path) {
            Ok(current_cid) => {
                if current_cid.as_str() != expected_cid {
                    drifted.push((path.to_string(), expected_cid.to_string(), current_cid.0));
                } else {
                    ok.push(path.to_string());
                }
            }
            Err(_) => {
                missing.push(path.to_string());
            }
        }
    }

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "ok": ok.len(),
                "drifted": drifted.iter().map(|(p, old, new)| {
                    serde_json::json!({"path": p, "expected": old, "actual": new})
                }).collect::<Vec<_>>(),
                "missing": missing,
            }))?
        );
    } else {
        if !drifted.is_empty() {
            println!("DRIFTED ({}):", drifted.len());
            for (path, expected, actual) in &drifted {
                println!(
                    "  {} expected:{} actual:{}",
                    path,
                    &expected[..12],
                    &actual[..12]
                );
            }
        }
        if !missing.is_empty() {
            println!("MISSING ({}):", missing.len());
            for path in &missing {
                println!("  {path}");
            }
        }
        if drifted.is_empty() && missing.is_empty() {
            println!("No drift detected. {} files OK.", ok.len());
        } else {
            println!("\n{} OK, {} drifted, {} missing", ok.len(), drifted.len(), missing.len());
        }
    }

    Ok(())
}

fn cmd_gc(base: &PathBuf, delete: bool, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let cas = open_cas(base)?;
    let dag = ket_dag::Dag::new(&cas);

    let all_cids = cas.list()?;
    let referenced = dag.referenced_cids()?;

    let mut unreferenced = Vec::new();
    let mut unreferenced_bytes = 0u64;

    for cid in &all_cids {
        if !referenced.contains(cid) {
            let size = cas.blob_size(cid).unwrap_or(0);
            unreferenced.push((cid.clone(), size));
            unreferenced_bytes += size;
        }
    }

    if delete {
        for (cid, _) in &unreferenced {
            cas.delete(cid)?;
        }
    }

    // Log
    if delete && !unreferenced.is_empty() {
        let log_path = base.join("log");
        log::append(
            &log_path,
            "gc",
            &format!("deleted {} blobs ({} bytes)", unreferenced.len(), unreferenced_bytes),
        )?;
    }

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "total_blobs": all_cids.len(),
                "referenced": referenced.len(),
                "unreferenced": unreferenced.len(),
                "unreferenced_bytes": unreferenced_bytes,
                "deleted": delete,
            }))?
        );
    } else {
        let verb = if delete { "Deleted" } else { "Would delete" };
        println!(
            "{} {} unreferenced blobs ({} bytes)",
            verb,
            unreferenced.len(),
            unreferenced_bytes
        );
        println!(
            "{} referenced / {} total",
            referenced.len(),
            all_cids.len()
        );
        if !delete && !unreferenced.is_empty() {
            println!("\nRun with --delete to actually remove them.");
        }
    }

    Ok(())
}

fn cmd_export(
    base: &PathBuf,
    cid: &str,
    out: Option<&str>,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let cas = open_cas(base)?;
    let dag = ket_dag::Dag::new(&cas);

    let bundle = dag.export(&ket_cas::Cid::from(cid))?;
    let serialized = serde_json::to_string_pretty(&bundle)?;

    if let Some(path) = out {
        fs::write(path, &serialized)?;
        if json {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "exported": true,
                    "root_cid": cid,
                    "entries": bundle.entries.len(),
                    "file": path,
                }))?
            );
        } else {
            println!(
                "Exported {} nodes to {}",
                bundle.entries.len(),
                path
            );
        }
    } else {
        // Write bundle to stdout
        println!("{serialized}");
    }

    // Log
    let log_path = base.join("log");
    log::append(&log_path, "export", &format!("{} ({} nodes)", &cid[..12.min(cid.len())], bundle.entries.len()))?;

    Ok(())
}

fn cmd_import(base: &PathBuf, path: &str, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let cas = open_cas(base)?;
    let dag = ket_dag::Dag::new(&cas);

    let data = fs::read_to_string(path)?;
    let bundle: ket_dag::DagBundle = serde_json::from_str(&data)?;
    let imported = dag.import(&bundle)?;

    // Sync imported nodes to SQL if available
    let mut sql_synced = 0;
    if let Ok(db) = open_db(base) {
        for entry in &bundle.entries {
            if let Ok(node) = dag.get_node(&entry.node_cid) {
                let parent_refs: Vec<(&str, i32)> = node
                    .parents
                    .iter()
                    .enumerate()
                    .map(|(i, p)| (p.as_str(), i as i32))
                    .collect();
                if db
                    .sync_dag_node(
                        entry.node_cid.as_str(),
                        &node.kind.to_string(),
                        &node.agent,
                        &node.timestamp,
                        node.output_cid.as_str(),
                        "",
                        &parent_refs,
                    )
                    .is_ok()
                {
                    sql_synced += 1;
                }
            }
        }
    }

    // Log
    let log_path = base.join("log");
    log::append(
        &log_path,
        "import",
        &format!("{} blobs from {}", imported, path),
    )?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "imported_blobs": imported,
                "root_cid": bundle.root_cid.as_str(),
                "entries": bundle.entries.len(),
                "sql_synced": sql_synced,
            }))?
        );
    } else {
        println!(
            "Imported {} new blobs ({} nodes, root: {})",
            imported,
            bundle.entries.len(),
            &bundle.root_cid.as_str()[..12]
        );
        if sql_synced > 0 {
            println!("  {} nodes synced to SQL", sql_synced);
        }
    }

    Ok(())
}

fn cmd_merge(
    base: &PathBuf,
    content: &str,
    parents: &[String],
    kind: &str,
    agent: &str,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let cas = open_cas(base)?;
    let dag = ket_dag::Dag::new(&cas);

    let node_kind = match kind {
        "memory" => ket_dag::NodeKind::Memory,
        "code" => ket_dag::NodeKind::Code,
        "reasoning" => ket_dag::NodeKind::Reasoning,
        "task" => ket_dag::NodeKind::Task,
        "cdom" => ket_dag::NodeKind::Cdom,
        "score" => ket_dag::NodeKind::Score,
        "context" => ket_dag::NodeKind::Context,
        _ => return Err(format!("Unknown kind: {kind}").into()),
    };

    let parent_cids: Vec<ket_cas::Cid> = parents.iter().map(|p| ket_cas::Cid::from(p.as_str())).collect();
    let (node_cid, content_cid) =
        dag.store_with_node(content.as_bytes(), node_kind, parent_cids.clone(), agent)?;

    // Dual-write to SQL
    if let Ok(db) = open_db(base) {
        let node = dag.get_node(&node_cid)?;
        let parent_refs: Vec<(&str, i32)> = parent_cids
            .iter()
            .enumerate()
            .map(|(i, p)| (p.as_str(), i as i32))
            .collect();
        let _ = db.sync_dag_node(
            node_cid.as_str(),
            kind,
            agent,
            &node.timestamp,
            content_cid.as_str(),
            "",
            &parent_refs,
        );
    }

    // Log
    let log_path = base.join("log");
    log::append(
        &log_path,
        "merge",
        &format!("{} ({} parents)", node_cid, parents.len()),
    )?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "node_cid": node_cid.as_str(),
                "content_cid": content_cid.as_str(),
                "parents": parents,
                "kind": kind,
                "agent": agent,
            }))?
        );
    } else {
        println!("Merge node: {node_cid}");
        println!("  Content:  {content_cid}");
        println!("  Parents:  {}", parents.len());
        for (i, p) in parents.iter().enumerate() {
            println!("    [{i}] {}", &p[..12.min(p.len())]);
        }
    }

    Ok(())
}

fn cmd_dot(base: &PathBuf, root: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let cas = open_cas(base)?;
    let dag = ket_dag::Dag::new(&cas);

    let nodes: Vec<(ket_cas::Cid, ket_dag::DagNode)> = if let Some(root_cid) = root {
        dag.lineage(&ket_cas::Cid::from(root_cid))?
    } else {
        // All DAG nodes
        let cids = cas.list()?;
        cids.iter()
            .filter_map(|cid| dag.get_node(cid).ok().map(|n| (cid.clone(), n)))
            .collect()
    };

    println!("digraph ket {{");
    println!("  rankdir=BT;");
    println!("  node [shape=box, style=filled, fontname=\"monospace\"];");
    println!();

    // Node kind -> color mapping
    for (cid, node) in &nodes {
        let short = &cid.as_str()[..12];
        let color = match node.kind {
            ket_dag::NodeKind::Memory => "#E8F5E9",
            ket_dag::NodeKind::Code => "#E3F2FD",
            ket_dag::NodeKind::Reasoning => "#FFF3E0",
            ket_dag::NodeKind::Task => "#F3E5F5",
            ket_dag::NodeKind::Cdom => "#E0F7FA",
            ket_dag::NodeKind::Score => "#FBE9E7",
            ket_dag::NodeKind::Context => "#F1F8E9",
        };
        println!(
            "  \"{}\" [label=\"{}\\n{}\\n{}\", fillcolor=\"{}\"];",
            short, short, node.kind, node.agent, color
        );
    }

    println!();

    // Edges
    for (cid, node) in &nodes {
        let child_short = &cid.as_str()[..12];
        for parent in &node.parents {
            let parent_short = &parent.as_str()[..12];
            println!("  \"{}\" -> \"{}\";", child_short, parent_short);
        }
    }

    // Soft links (dashed)
    if let Ok(db) = open_db(base) {
        for (cid, _) in &nodes {
            if let Ok(links) = db.soft_links_from(cid.as_str()) {
                for line in links.lines().skip(1) {
                    let parts: Vec<&str> = line.splitn(3, ',').collect();
                    if parts.len() >= 2 {
                        let to_short = &parts[0][..12.min(parts[0].len())];
                        let relation = if parts.len() >= 3 { parts[1] } else { "" };
                        let from_short = &cid.as_str()[..12];
                        println!(
                            "  \"{}\" -> \"{}\" [style=dashed, label=\"{}\", color=\"gray\"];",
                            from_short, to_short, relation
                        );
                    }
                }
            }
        }
    }

    println!("}}");
    Ok(())
}

fn cmd_search(
    base: &PathBuf,
    query: &str,
    limit: usize,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let cas = open_cas(base)?;
    let cids = cas.list()?;
    let query_lower = query.to_lowercase();

    let mut results = Vec::new();

    for cid in &cids {
        if results.len() >= limit {
            break;
        }
        if let Ok(data) = cas.get(cid) {
            if let Ok(text) = std::str::from_utf8(&data) {
                let text_lower = text.to_lowercase();
                if text_lower.contains(&query_lower) {
                    // Find matching lines with context
                    let mut match_lines = Vec::new();
                    for (i, line) in text.lines().enumerate() {
                        if line.to_lowercase().contains(&query_lower) {
                            match_lines.push((i + 1, line.to_string()));
                        }
                    }
                    results.push((cid.clone(), data.len(), match_lines));
                }
            }
        }
    }

    if json {
        let items: Vec<serde_json::Value> = results
            .iter()
            .map(|(cid, size, lines)| {
                serde_json::json!({
                    "cid": cid.as_str(),
                    "size": size,
                    "matches": lines.iter().map(|(n, l)| {
                        serde_json::json!({"line": n, "text": l})
                    }).collect::<Vec<_>>(),
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&items)?);
    } else if results.is_empty() {
        println!("No results for '{query}'");
    } else {
        for (cid, size, lines) in &results {
            println!("{}  ({} bytes)", &cid.as_str()[..12], size);
            for (n, line) in lines.iter().take(3) {
                let display = if line.len() > 100 {
                    format!("{}...", &line[..100])
                } else {
                    line.to_string()
                };
                println!("  L{n}: {display}");
            }
            if lines.len() > 3 {
                println!("  ... {} more matches", lines.len() - 3);
            }
        }
        println!("\n{} blobs matched", results.len());
    }

    Ok(())
}

fn cmd_snapshot(
    base: &PathBuf,
    action: SnapshotAction,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshots_dir = base.join("snapshots");

    match action {
        SnapshotAction::Create { name } => {
            fs::create_dir_all(&snapshots_dir)?;

            let cas = open_cas(base)?;
            let dag = ket_dag::Dag::new(&cas);

            // Collect all DAG node CIDs + their output CIDs
            let cids = cas.list()?;
            let mut snapshot_data = Vec::new();

            for cid in &cids {
                if let Ok(node) = dag.get_node(cid) {
                    snapshot_data.push(serde_json::json!({
                        "node_cid": cid.as_str(),
                        "output_cid": node.output_cid.as_str(),
                        "kind": node.kind.to_string(),
                        "agent": node.agent,
                        "timestamp": node.timestamp,
                        "parents": node.parents.len(),
                    }));
                }
            }

            let snapshot = serde_json::json!({
                "name": name,
                "created_at": chrono::Utc::now().to_rfc3339(),
                "total_blobs": cids.len(),
                "dag_nodes": snapshot_data.len(),
                "nodes": snapshot_data,
            });

            let path = snapshots_dir.join(format!("{name}.json"));
            fs::write(&path, serde_json::to_string_pretty(&snapshot)?)?;

            // Log
            let log_path = base.join("log");
            log::append(&log_path, "snapshot:create", &name)?;

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "name": name,
                        "dag_nodes": snapshot_data.len(),
                        "total_blobs": cids.len(),
                        "path": path.display().to_string(),
                    }))?
                );
            } else {
                println!(
                    "Snapshot '{}': {} DAG nodes, {} total blobs",
                    name,
                    snapshot_data.len(),
                    cids.len()
                );
            }
        }
        SnapshotAction::Ls => {
            if !snapshots_dir.exists() {
                if json {
                    println!("[]");
                } else {
                    println!("No snapshots. Use `ket snapshot create <name>` to create one.");
                }
                return Ok(());
            }

            let mut snaps = Vec::new();
            for entry in fs::read_dir(&snapshots_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().is_some_and(|e| e == "json") {
                    let data = fs::read_to_string(&path)?;
                    let snap: serde_json::Value = serde_json::from_str(&data)?;
                    snaps.push(snap);
                }
            }

            if json {
                println!("{}", serde_json::to_string_pretty(&snaps)?);
            } else if snaps.is_empty() {
                println!("No snapshots.");
            } else {
                for snap in &snaps {
                    println!(
                        "  {}  {} nodes  {}",
                        snap["name"].as_str().unwrap_or("?"),
                        snap["dag_nodes"],
                        snap["created_at"].as_str().unwrap_or("")
                    );
                }
            }
        }
        SnapshotAction::Verify { name } => {
            let path = snapshots_dir.join(format!("{name}.json"));
            if !path.exists() {
                return Err(format!("Snapshot '{name}' not found").into());
            }

            let data = fs::read_to_string(&path)?;
            let snap: serde_json::Value = serde_json::from_str(&data)?;
            let cas = open_cas(base)?;

            let nodes = snap["nodes"].as_array().unwrap();
            let mut present = 0u64;
            let mut missing = Vec::new();
            let mut corrupted = Vec::new();

            for node in nodes {
                let node_cid = ket_cas::Cid::from(node["node_cid"].as_str().unwrap());
                let output_cid = ket_cas::Cid::from(node["output_cid"].as_str().unwrap());

                if !cas.exists(&node_cid) {
                    missing.push(node_cid.as_str().to_string());
                } else if !cas.verify(&node_cid)? {
                    corrupted.push(node_cid.as_str().to_string());
                } else {
                    present += 1;
                }

                if !cas.exists(&output_cid) {
                    missing.push(output_cid.as_str().to_string());
                } else if !cas.verify(&output_cid)? {
                    corrupted.push(output_cid.as_str().to_string());
                }
            }

            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "name": name,
                        "snapshot_nodes": nodes.len(),
                        "present": present,
                        "missing": missing,
                        "corrupted": corrupted,
                        "ok": missing.is_empty() && corrupted.is_empty(),
                    }))?
                );
            } else if missing.is_empty() && corrupted.is_empty() {
                println!("Snapshot '{}' verified: all {} nodes present and intact", name, nodes.len());
            } else {
                if !missing.is_empty() {
                    println!("MISSING ({}):", missing.len());
                    for cid in &missing {
                        println!("  {}", &cid[..12]);
                    }
                }
                if !corrupted.is_empty() {
                    println!("CORRUPTED ({}):", corrupted.len());
                    for cid in &corrupted {
                        println!("  {}", &cid[..12]);
                    }
                }
                println!("\n{} nodes OK, {} missing, {} corrupted", present, missing.len(), corrupted.len());
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

fn cmd_cas_stats(base: &PathBuf, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let cas = open_cas(base)?;
    let dag = ket_dag::Dag::new(&cas);

    let all_cids = cas.list()?;
    let total_size = cas.total_size()?;
    let referenced = dag.referenced_cids()?;

    let mut node_count = 0u64;
    let mut content_count = 0u64;
    let mut orphan_count = 0u64;

    for cid in &all_cids {
        if dag.get_node(cid).is_ok() {
            node_count += 1;
        } else if referenced.contains(cid) {
            content_count += 1;
        } else {
            orphan_count += 1;
        }
    }

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "total_blobs": all_cids.len(),
                "total_bytes": total_size,
                "dag_nodes": node_count,
                "content_blobs": content_count,
                "orphan_blobs": orphan_count,
            }))?
        );
    } else {
        println!("CAS Store: {}", cas.root().display());
        println!("  Total blobs:    {}", all_cids.len());
        println!("  Total size:     {} bytes", total_size);
        println!("  DAG nodes:      {node_count}");
        println!("  Content blobs:  {content_count}");
        println!("  Orphan blobs:   {orphan_count}");
    }

    Ok(())
}

// --- Helpers ---

fn ket_dir(home: &Option<String>) -> PathBuf {
    if let Some(h) = home {
        PathBuf::from(h)
    } else {
        PathBuf::from(".ket")
    }
}

fn open_cas(base: &PathBuf) -> Result<ket_cas::Store, Box<dyn std::error::Error>> {
    let cas_dir = base.join("cas");
    Ok(ket_cas::Store::open(cas_dir)?)
}

fn open_db(base: &PathBuf) -> Result<ket_sql::DoltDb, Box<dyn std::error::Error>> {
    let db_path = base.join("ket.db");
    Ok(ket_sql::DoltDb::open(db_path)?)
}
