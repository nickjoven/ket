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

    /// Query CDOM symbols
    Cdom {
        /// Symbol query
        query: String,
        /// File to search in
        path: String,
    },

    /// Show mutation log
    Log {
        /// Number of entries to show
        #[arg(short, long, default_value = "20")]
        n: usize,
    },

    /// Run MCP server on stdio
    Mcp,

    /// Show agent scores
    Scores {
        /// Node CID to show scores for
        node_cid: String,
    },
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
        Commands::Cdom { query, path } => cmd_cdom(&query, &path, cli.json),
        Commands::Log { n } => cmd_log(&base, n, cli.json),
        Commands::Mcp => cmd_mcp(&base),
        Commands::Scores { node_cid } => cmd_scores(&base, &node_cid, cli.json),
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
                dag.store_with_node(content.as_bytes(), node_kind, parents, &agent)?;

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
    let file_path = std::path::Path::new(path);

    if file_path.is_dir() {
        // Scan directory recursively
        let mut snapshots = Vec::new();
        scan_dir(file_path, &cas, &mut snapshots)?;

        if json {
            println!("{}", serde_json::to_string_pretty(&snapshots)?);
        } else {
            for snap in &snapshots {
                println!(
                    "{} ({}) — {} symbols",
                    snap.file_path,
                    snap.language,
                    snap.symbols.len()
                );
                for sym in &snap.symbols {
                    println!("  {}:{}-{} {} {}", sym.kind, sym.start_line, sym.end_line, sym.name, sym.parent.as_deref().unwrap_or(""));
                }
            }
        }
    } else {
        let snapshot = ket_cdom::scan_file(file_path, &cas)?;

        if json {
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        } else {
            println!(
                "{} ({}) — {} symbols",
                snapshot.file_path,
                snapshot.language,
                snapshot.symbols.len()
            );
            for sym in &snapshot.symbols {
                println!("  {}:{}-{} {}", sym.kind, sym.start_line, sym.end_line, sym.name);
            }
        }
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
            // Skip hidden dirs and common ignores
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            if name.starts_with('.') || name == "target" || name == "node_modules" || name == "__pycache__" {
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

fn cmd_cdom(query: &str, path: &str, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let symbols = ket_cdom::parse_file(std::path::Path::new(path))?;
    let matches = ket_cdom::query_symbols(&symbols, query);

    if json {
        let results: Vec<serde_json::Value> = matches
            .iter()
            .map(|s| {
                serde_json::json!({
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
        println!("No symbols matching '{query}'");
    } else {
        for s in matches {
            println!(
                "  {}:{}-{} {} {}",
                s.kind,
                s.start_line,
                s.end_line,
                s.name,
                s.parent.as_deref().unwrap_or("")
            );
        }
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
    node_cid: &str,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = open_db(base)?;
    let engine = ket_score::ScoringEngine::new(&db);
    let result = engine.scores_for(&ket_cas::Cid::from(node_cid))?;

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
