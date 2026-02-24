mod log;

use clap::{Parser, Subcommand};
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "ket",
    about = "Content-addressable substrate for EverMemOS",
    version
)]
struct Cli {
    /// Path to .ket directory
    #[arg(long, global = true, env = "KET_HOME")]
    home: Option<String>,

    /// Emit JSON output
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new .ket directory
    Init,

    /// Store content in CAS
    Put {
        /// File to store (or - for stdin)
        path: String,
    },

    /// Retrieve content by CID
    Get {
        /// Content identifier
        cid: String,
    },

    /// Verify CID integrity
    Verify {
        /// Content identifier to verify
        cid: String,
    },

    /// DAG operations
    Dag {
        #[command(subcommand)]
        action: DagAction,
    },

    /// SQL operations (Dolt)
    Sql {
        /// SQL query to execute
        query: String,
    },

    /// Task management
    Task {
        #[command(subcommand)]
        action: TaskAction,
    },

    /// Agent management
    Agent {
        #[command(subcommand)]
        action: AgentAction,
    },

    /// Run a task with an agent
    Run {
        /// Task ID to run
        task_id: String,
        /// Agent to use
        #[arg(long, default_value = "claude")]
        agent: String,
        /// Prompt for the agent
        #[arg(long)]
        prompt: Option<String>,
    },

    /// Scan source files for CDOM symbols
    Scan {
        /// File or directory to scan
        path: String,
    },

    /// Query CDOM symbols (searches SQL index across all scanned files)
    Cdom {
        /// Symbol query (substring match)
        query: String,
        /// File to search in (optional — omit to search all indexed files)
        path: Option<String>,
    },

    /// Show mutation log
    Log {
        /// Number of entries to show
        #[arg(short, long, default_value = "20")]
        n: usize,
    },

    /// Run MCP server on stdio
    Mcp,

    /// Show agent scores or profile
    Scores {
        #[command(subcommand)]
        action: ScoreAction,
    },

    /// Rebuild SQL index from CAS (source of truth)
    Repair {
        /// Dry run — show what would be synced without writing
        #[arg(long)]
        dry_run: bool,
    },

    /// Show .ket status overview
    Status,

    /// Dolt version history
    History {
        /// Number of commits to show
        #[arg(short, long, default_value = "10")]
        n: usize,
    },

    /// Dolt diff (working set vs HEAD, or between commits)
    Diff {
        /// From commit
        from: Option<String>,
        /// To commit
        to: Option<String>,
    },

    /// Soft link management
    Link {
        #[command(subcommand)]
        action: LinkAction,
    },

    /// Context file tracking for drift detection
    Track {
        #[command(subcommand)]
        action: TrackAction,
    },

    /// Check all tracked files for drift
    Drift,

    /// Garbage collect unreferenced CAS blobs
    Gc {
        /// Actually delete (default: dry run)
        #[arg(long)]
        delete: bool,
    },

    /// Export a DAG subgraph as a portable bundle
    Export {
        /// Root node CID to export
        cid: String,
        /// Output file (default: stdout)
        #[arg(short, long)]
        out: Option<String>,
    },

    /// Import a DAG bundle
    Import {
        /// Bundle file path
        path: String,
    },

    /// Create a merge node combining multiple parent lineages
    Merge {
        /// Content for the merge node
        content: String,
        /// Parent CIDs to merge (at least 2)
        #[arg(long, required = true, num_args = 2..)]
        parents: Vec<String>,
        /// Node kind
        #[arg(long, default_value = "reasoning")]
        kind: String,
        /// Agent name
        #[arg(long, default_value = "human")]
        agent: String,
    },

    /// Show CAS store statistics
    CasStats,
}

#[derive(Subcommand)]
enum DagAction {
    /// List DAG nodes
    Ls,
    /// Show a specific node
    Show {
        /// Node CID
        cid: String,
    },
    /// Create a DAG node
    Create {
        /// Content for the node
        content: String,
        /// Node kind
        #[arg(long, default_value = "code")]
        kind: String,
        /// Agent name
        #[arg(long, default_value = "human")]
        agent: String,
        /// Parent CIDs
        #[arg(long)]
        parent: Vec<String>,
    },
    /// Trace lineage of a node
    Lineage {
        /// Node CID
        cid: String,
    },
    /// Check drift of a file against a CID
    Drift {
        /// File path
        path: String,
        /// Expected CID
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
    /// Score a node
    Add {
        /// Node CID
        node_cid: String,
        /// Agent that produced it
        #[arg(long)]
        agent: String,
        /// Who is scoring
        #[arg(long, default_value = "human")]
        scorer: String,
        /// Dimension (correctness, efficiency, style, completeness)
        #[arg(long)]
        dim: String,
        /// Score value 0.0-1.0
        #[arg(long)]
        value: f64,
        /// Evidence
        #[arg(long, default_value = "")]
        evidence: String,
    },
    /// Show scores for a node
    Show {
        /// Node CID
        node_cid: String,
    },
    /// Show an agent's scoring profile
    Profile {
        /// Agent name
        agent: String,
    },
    /// Find the best agent for a dimension
    Route {
        /// Dimension
        dim: String,
    },
    /// Auto-score by running build/test/lint
    Auto {
        /// Node CID to score
        node_cid: String,
        /// Agent that produced it
        #[arg(long)]
        agent: String,
        /// Working directory for build/test
        #[arg(long, default_value = ".")]
        dir: String,
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
        /// Relation type (e.g., "supersedes", "related_to", "contradicts")
        relation: String,
    },
    /// List soft links for a node
    Ls {
        /// Node CID
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
