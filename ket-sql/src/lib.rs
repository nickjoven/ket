//! Dolt SQL layer for Ket.
//!
//! Shells out to `dolt sql -q` for queries. Manages 6 tables:
//! dag_nodes, dag_edges, soft_links, tasks, agents, scores.

use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    #[error("Dolt command failed: {0}")]
    DoltError(String),
    #[error("Dolt not found. Install from https://docs.dolthub.com/introduction/installation")]
    DoltNotFound,
    #[error("Database not initialized at {0}")]
    NotInitialized(PathBuf),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Result of a dolt sql query.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueryResult {
    pub rows: Vec<Vec<String>>,
    pub columns: Vec<String>,
}

/// The Dolt database handle.
pub struct DoltDb {
    db_path: PathBuf,
}

impl DoltDb {
    /// Open an existing Dolt database.
    pub fn open(db_path: PathBuf) -> Result<Self, SqlError> {
        if !db_path.join(".dolt").exists() {
            return Err(SqlError::NotInitialized(db_path));
        }
        Ok(DoltDb { db_path })
    }

    /// Initialize a new Dolt database and create schema.
    pub fn init(db_path: &Path) -> Result<Self, SqlError> {
        std::fs::create_dir_all(db_path).map_err(SqlError::Io)?;

        // Check dolt is available
        Command::new("dolt")
            .arg("version")
            .output()
            .map_err(|_| SqlError::DoltNotFound)?;

        // Init the dolt repo
        let output = Command::new("dolt")
            .arg("init")
            .arg("--name")
            .arg("ket")
            .arg("--email")
            .arg("ket@local")
            .current_dir(db_path)
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Already initialized is fine
            if !stderr.contains("already") {
                return Err(SqlError::DoltError(stderr.into_owned()));
            }
        }

        let db = DoltDb {
            db_path: db_path.to_path_buf(),
        };
        db.create_schema()?;
        Ok(db)
    }

    /// Execute a SQL query, return raw output.
    pub fn query(&self, sql: &str) -> Result<String, SqlError> {
        let output = Command::new("dolt")
            .args(["sql", "-q", sql, "-r", "csv"])
            .current_dir(&self.db_path)
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SqlError::DoltError(format!("{sql}: {stderr}")));
        }

        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// Execute a SQL statement (INSERT/UPDATE/DELETE), no result expected.
    pub fn exec(&self, sql: &str) -> Result<(), SqlError> {
        let output = Command::new("dolt")
            .args(["sql", "-q", sql])
            .current_dir(&self.db_path)
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SqlError::DoltError(format!("{sql}: {stderr}")));
        }

        Ok(())
    }

    /// Execute multiple SQL statements in a single dolt invocation.
    /// Wraps them in a transaction for atomicity.
    pub fn exec_batch(&self, statements: &[String]) -> Result<(), SqlError> {
        if statements.is_empty() {
            return Ok(());
        }
        let mut batch = String::from("BEGIN;\n");
        for stmt in statements {
            batch.push_str(stmt);
            batch.push_str(";\n");
        }
        batch.push_str("COMMIT;");

        let output = Command::new("dolt")
            .args(["sql", "-q", &batch])
            .current_dir(&self.db_path)
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SqlError::DoltError(format!("batch: {stderr}")));
        }

        Ok(())
    }

    /// Commit the current state in Dolt.
    pub fn commit(&self, message: &str) -> Result<(), SqlError> {
        // Stage all changes
        let _ = Command::new("dolt")
            .args(["add", "."])
            .current_dir(&self.db_path)
            .output()?;

        let output = Command::new("dolt")
            .args(["commit", "-m", message, "--allow-empty"])
            .current_dir(&self.db_path)
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // "nothing to commit" is fine
            if !stderr.contains("nothing to commit") {
                return Err(SqlError::DoltError(stderr.into_owned()));
            }
        }

        Ok(())
    }

    fn create_schema(&self) -> Result<(), SqlError> {
        let ddl = [
            "CREATE TABLE IF NOT EXISTS dag_nodes (
                cid VARCHAR(64) PRIMARY KEY,
                kind VARCHAR(20) NOT NULL,
                agent VARCHAR(100) NOT NULL,
                created_at VARCHAR(40) NOT NULL,
                output_cid VARCHAR(64) NOT NULL,
                meta TEXT
            )",
            "CREATE TABLE IF NOT EXISTS dag_edges (
                parent_cid VARCHAR(64) NOT NULL,
                child_cid VARCHAR(64) NOT NULL,
                ordinal INT NOT NULL DEFAULT 0,
                PRIMARY KEY (parent_cid, child_cid)
            )",
            "CREATE TABLE IF NOT EXISTS soft_links (
                from_cid VARCHAR(64) NOT NULL,
                to_cid VARCHAR(64) NOT NULL,
                relation VARCHAR(100) NOT NULL,
                created_at VARCHAR(40) NOT NULL,
                PRIMARY KEY (from_cid, to_cid, relation)
            )",
            "CREATE TABLE IF NOT EXISTS tasks (
                id VARCHAR(36) PRIMARY KEY,
                title TEXT NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                assigned_to VARCHAR(100),
                created_by VARCHAR(100) NOT NULL,
                created_at VARCHAR(40) NOT NULL,
                updated_at VARCHAR(40) NOT NULL,
                parent_task VARCHAR(36),
                context_cid VARCHAR(64),
                result_cid VARCHAR(64),
                meta TEXT
            )",
            "CREATE TABLE IF NOT EXISTS agents (
                name VARCHAR(100) PRIMARY KEY,
                cli_command TEXT NOT NULL,
                mcp_capable BOOLEAN NOT NULL DEFAULT FALSE,
                capabilities TEXT,
                model VARCHAR(100),
                updated_at VARCHAR(40) NOT NULL
            )",
            "CREATE TABLE IF NOT EXISTS context_files (
                path VARCHAR(512) PRIMARY KEY,
                cid VARCHAR(64) NOT NULL,
                tracked_at VARCHAR(40) NOT NULL,
                agent VARCHAR(100) NOT NULL DEFAULT 'human'
            )",
            "CREATE TABLE IF NOT EXISTS cdom_symbols (
                id INT AUTO_INCREMENT PRIMARY KEY,
                file_path VARCHAR(512) NOT NULL,
                file_cid VARCHAR(64) NOT NULL,
                name VARCHAR(200) NOT NULL,
                kind VARCHAR(20) NOT NULL,
                start_line INT NOT NULL,
                end_line INT NOT NULL,
                parent_symbol VARCHAR(200),
                scanned_at VARCHAR(40) NOT NULL,
                INDEX idx_name (name),
                INDEX idx_kind (kind),
                INDEX idx_file_cid (file_cid)
            )",
            "CREATE TABLE IF NOT EXISTS scores (
                id VARCHAR(36) PRIMARY KEY,
                node_cid VARCHAR(64) NOT NULL,
                agent VARCHAR(100) NOT NULL,
                scorer VARCHAR(100) NOT NULL,
                dimension VARCHAR(50) NOT NULL,
                value FLOAT NOT NULL,
                evidence TEXT,
                created_at VARCHAR(40) NOT NULL
            )",
            "CREATE TABLE IF NOT EXISTS calibrations (
                cid VARCHAR(64) PRIMARY KEY,
                root_cid VARCHAR(64) NOT NULL,
                lambda_cost FLOAT,
                lambda_depth FLOAT,
                lambda_tier3 FLOAT,
                total_gain FLOAT,
                total_cost FLOAT,
                iterations INT,
                agent VARCHAR(100),
                ts VARCHAR(40)
            )",
        ];

        for stmt in &ddl {
            self.exec(stmt)?;
        }

        self.commit("Initialize ket schema")?;
        Ok(())
    }

    /// Insert a DAG node record.
    pub fn insert_dag_node(
        &self,
        cid: &str,
        kind: &str,
        agent: &str,
        created_at: &str,
        output_cid: &str,
        meta: &str,
    ) -> Result<(), SqlError> {
        let sql = format!(
            "INSERT INTO dag_nodes (cid, kind, agent, created_at, output_cid, meta) \
             VALUES ('{cid}', '{kind}', '{agent}', '{created_at}', '{output_cid}', '{}')",
            escape_sql(meta)
        );
        self.exec(&sql)
    }

    /// Insert a DAG edge.
    pub fn insert_dag_edge(
        &self,
        parent_cid: &str,
        child_cid: &str,
        ordinal: i32,
    ) -> Result<(), SqlError> {
        let sql = format!(
            "INSERT INTO dag_edges (parent_cid, child_cid, ordinal) \
             VALUES ('{parent_cid}', '{child_cid}', {ordinal})"
        );
        self.exec(&sql)
    }

    /// Sync a DAG node + its edges to SQL in a single transaction.
    /// Uses INSERT IGNORE so re-syncing the same node is idempotent.
    #[allow(clippy::too_many_arguments)]
    pub fn sync_dag_node(
        &self,
        cid: &str,
        kind: &str,
        agent: &str,
        created_at: &str,
        output_cid: &str,
        meta: &str,
        parent_cids: &[(&str, i32)],
    ) -> Result<(), SqlError> {
        let mut stmts = Vec::with_capacity(1 + parent_cids.len());

        stmts.push(format!(
            "INSERT IGNORE INTO dag_nodes (cid, kind, agent, created_at, output_cid, meta) \
             VALUES ('{cid}', '{kind}', '{agent}', '{created_at}', '{output_cid}', '{}')",
            escape_sql(meta)
        ));

        for (parent_cid, ordinal) in parent_cids {
            stmts.push(format!(
                "INSERT IGNORE INTO dag_edges (parent_cid, child_cid, ordinal) \
                 VALUES ('{parent_cid}', '{cid}', {ordinal})"
            ));
        }

        self.exec_batch(&stmts)
    }

    /// Check if a DAG node exists in SQL.
    pub fn dag_node_exists(&self, cid: &str) -> Result<bool, SqlError> {
        let result = self.query(&format!(
            "SELECT COUNT(*) AS cnt FROM dag_nodes WHERE cid = '{cid}'"
        ))?;
        // CSV output: "cnt\n0\n" or "cnt\n1\n"
        Ok(!result.contains("\n0"))
    }

    /// Insert a soft link.
    pub fn insert_soft_link(
        &self,
        from_cid: &str,
        to_cid: &str,
        relation: &str,
    ) -> Result<(), SqlError> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = format!(
            "INSERT INTO soft_links (from_cid, to_cid, relation, created_at) \
             VALUES ('{from_cid}', '{to_cid}', '{relation}', '{now}')"
        );
        self.exec(&sql)
    }

    /// Insert a task.
    pub fn insert_task(
        &self,
        id: &str,
        title: &str,
        created_by: &str,
        parent_task: Option<&str>,
        context_cid: Option<&str>,
    ) -> Result<(), SqlError> {
        let now = chrono::Utc::now().to_rfc3339();
        let parent = parent_task.unwrap_or("");
        let ctx = context_cid.unwrap_or("");
        let sql = format!(
            "INSERT INTO tasks (id, title, status, created_by, created_at, updated_at, parent_task, context_cid) \
             VALUES ('{id}', '{}', 'pending', '{created_by}', '{now}', '{now}', '{parent}', '{ctx}')",
            escape_sql(title)
        );
        self.exec(&sql)
    }

    /// Update task status.
    pub fn update_task_status(&self, id: &str, status: &str) -> Result<(), SqlError> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = format!(
            "UPDATE tasks SET status = '{status}', updated_at = '{now}' WHERE id = '{id}'"
        );
        self.exec(&sql)
    }

    /// Assign a task to an agent.
    pub fn assign_task(&self, id: &str, agent: &str) -> Result<(), SqlError> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = format!(
            "UPDATE tasks SET assigned_to = '{agent}', status = 'assigned', updated_at = '{now}' WHERE id = '{id}'"
        );
        self.exec(&sql)
    }

    /// Insert or update an agent.
    pub fn upsert_agent(
        &self,
        name: &str,
        cli_command: &str,
        mcp_capable: bool,
        capabilities: &str,
        model: &str,
    ) -> Result<(), SqlError> {
        let now = chrono::Utc::now().to_rfc3339();
        let mcp = if mcp_capable { "TRUE" } else { "FALSE" };
        let sql = format!(
            "REPLACE INTO agents (name, cli_command, mcp_capable, capabilities, model, updated_at) \
             VALUES ('{name}', '{}', {mcp}, '{}', '{model}', '{now}')",
            escape_sql(cli_command),
            escape_sql(capabilities)
        );
        self.exec(&sql)
    }

    /// Insert a score.
    #[allow(clippy::too_many_arguments)]
    pub fn insert_score(
        &self,
        id: &str,
        node_cid: &str,
        agent: &str,
        scorer: &str,
        dimension: &str,
        value: f64,
        evidence: &str,
    ) -> Result<(), SqlError> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = format!(
            "INSERT INTO scores (id, node_cid, agent, scorer, dimension, value, evidence, created_at) \
             VALUES ('{id}', '{node_cid}', '{agent}', '{scorer}', '{dimension}', {value}, '{}', '{now}')",
            escape_sql(evidence)
        );
        self.exec(&sql)
    }

    /// Query all tasks.
    pub fn list_tasks(&self) -> Result<String, SqlError> {
        self.query("SELECT id, title, status, assigned_to, created_by, created_at FROM tasks ORDER BY created_at")
    }

    /// Query all dag nodes.
    pub fn list_dag_nodes(&self) -> Result<String, SqlError> {
        self.query("SELECT cid, kind, agent, created_at, output_cid FROM dag_nodes ORDER BY created_at")
    }

    /// Query all agents.
    pub fn list_agents(&self) -> Result<String, SqlError> {
        self.query("SELECT name, cli_command, mcp_capable, model, updated_at FROM agents ORDER BY name")
    }

    // --- Context tracking ---

    /// Track a file's CID for drift detection.
    pub fn track_context_file(&self, path: &str, cid: &str, agent: &str) -> Result<(), SqlError> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = format!(
            "REPLACE INTO context_files (path, cid, tracked_at, agent) \
             VALUES ('{}', '{cid}', '{now}', '{agent}')",
            escape_sql(path)
        );
        self.exec(&sql)
    }

    /// Get tracked CID for a file.
    pub fn get_tracked_cid(&self, path: &str) -> Result<Option<String>, SqlError> {
        let result = self.query(&format!(
            "SELECT cid FROM context_files WHERE path = '{}'",
            escape_sql(path)
        ))?;
        let cid = result.lines().nth(1).map(|s| s.trim().to_string());
        Ok(cid.filter(|s| !s.is_empty()))
    }

    /// List all tracked context files.
    pub fn list_context_files(&self) -> Result<String, SqlError> {
        self.query("SELECT path, cid, tracked_at, agent FROM context_files ORDER BY path")
    }

    /// Remove a tracked file.
    pub fn untrack_context_file(&self, path: &str) -> Result<(), SqlError> {
        self.exec(&format!(
            "DELETE FROM context_files WHERE path = '{}'",
            escape_sql(path)
        ))
    }

    // --- CDOM symbol index ---

    /// Upsert symbols for a file. Deletes old entries first, then bulk inserts.
    pub fn sync_cdom_symbols(
        &self,
        file_path: &str,
        file_cid: &str,
        symbols: &[(String, String, usize, usize, Option<String>)], // (name, kind, start, end, parent)
    ) -> Result<(), SqlError> {
        let now = chrono::Utc::now().to_rfc3339();
        let mut stmts = Vec::with_capacity(1 + symbols.len());

        // Delete old entries for this file
        stmts.push(format!(
            "DELETE FROM cdom_symbols WHERE file_path = '{}'",
            escape_sql(file_path)
        ));

        for (name, kind, start, end, parent) in symbols {
            let parent_val = parent.as_deref().unwrap_or("");
            stmts.push(format!(
                "INSERT INTO cdom_symbols (file_path, file_cid, name, kind, start_line, end_line, parent_symbol, scanned_at) \
                 VALUES ('{}', '{file_cid}', '{}', '{kind}', {start}, {end}, '{}', '{now}')",
                escape_sql(file_path),
                escape_sql(name),
                escape_sql(parent_val)
            ));
        }

        self.exec_batch(&stmts)
    }

    /// Search symbols by name across all files.
    pub fn search_symbols(&self, query: &str) -> Result<String, SqlError> {
        self.query(&format!(
            "SELECT file_path, name, kind, start_line, end_line, parent_symbol \
             FROM cdom_symbols WHERE name LIKE '%{}%' ORDER BY file_path, start_line",
            escape_sql(query)
        ))
    }

    /// Search symbols by kind.
    pub fn symbols_by_kind(&self, kind: &str) -> Result<String, SqlError> {
        self.query(&format!(
            "SELECT file_path, name, start_line, end_line \
             FROM cdom_symbols WHERE kind = '{kind}' ORDER BY file_path, start_line"
        ))
    }

    /// Get all symbols in a file.
    pub fn symbols_in_file(&self, file_path: &str) -> Result<String, SqlError> {
        self.query(&format!(
            "SELECT name, kind, start_line, end_line, parent_symbol \
             FROM cdom_symbols WHERE file_path = '{}' ORDER BY start_line",
            escape_sql(file_path)
        ))
    }

    /// Count symbols by kind across the codebase.
    pub fn symbol_stats(&self) -> Result<String, SqlError> {
        self.query(
            "SELECT kind, COUNT(*) AS n FROM cdom_symbols GROUP BY kind ORDER BY n DESC"
        )
    }

    /// Query scores for a node.
    pub fn scores_for_node(&self, node_cid: &str) -> Result<String, SqlError> {
        self.query(&format!(
            "SELECT dimension, value, scorer, evidence FROM scores WHERE node_cid = '{node_cid}'"
        ))
    }

    /// Average scores per agent per dimension.
    pub fn agent_score_profile(&self, agent: &str) -> Result<String, SqlError> {
        self.query(&format!(
            "SELECT dimension, ROUND(AVG(value), 3) AS avg_score, COUNT(*) AS n \
             FROM scores WHERE agent = '{agent}' GROUP BY dimension ORDER BY dimension"
        ))
    }

    /// Get the best agent for a given dimension based on average score.
    pub fn best_agent_for(&self, dimension: &str) -> Result<String, SqlError> {
        self.query(&format!(
            "SELECT agent, ROUND(AVG(value), 3) AS avg_score, COUNT(*) AS n \
             FROM scores WHERE dimension = '{dimension}' \
             GROUP BY agent ORDER BY avg_score DESC LIMIT 1"
        ))
    }

    // --- Dolt versioning ---

    /// Commit current working set with a message. Returns the commit hash.
    pub fn dolt_commit(&self, message: &str) -> Result<String, SqlError> {
        let _ = Command::new("dolt")
            .args(["add", "."])
            .current_dir(&self.db_path)
            .output()?;

        let output = Command::new("dolt")
            .args(["commit", "-m", message, "--allow-empty"])
            .current_dir(&self.db_path)
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("nothing to commit") {
                // Return current HEAD
                return self.dolt_head();
            }
            return Err(SqlError::DoltError(stderr.into_owned()));
        }

        self.dolt_head()
    }

    /// Get current HEAD commit hash.
    pub fn dolt_head(&self) -> Result<String, SqlError> {
        let output = Command::new("dolt")
            .args(["log", "-n", "1", "--oneline"])
            .current_dir(&self.db_path)
            .output()?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        Ok(stdout.split_whitespace().next().unwrap_or("unknown").to_string())
    }

    /// Get commit history.
    pub fn dolt_log(&self, n: usize) -> Result<String, SqlError> {
        let output = Command::new("dolt")
            .args(["log", "-n", &n.to_string()])
            .current_dir(&self.db_path)
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SqlError::DoltError(stderr.into_owned()));
        }

        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// Diff between two commits (or working set vs HEAD).
    pub fn dolt_diff(&self, from: Option<&str>, to: Option<&str>) -> Result<String, SqlError> {
        let mut args = vec!["diff".to_string()];
        if let Some(f) = from {
            args.push(f.to_string());
        }
        if let Some(t) = to {
            args.push(t.to_string());
        }

        let output = Command::new("dolt")
            .args(&args)
            .current_dir(&self.db_path)
            .output()?;

        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// Create a Dolt branch.
    pub fn dolt_branch(&self, name: &str) -> Result<(), SqlError> {
        let output = Command::new("dolt")
            .args(["branch", name])
            .current_dir(&self.db_path)
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SqlError::DoltError(stderr.into_owned()));
        }

        Ok(())
    }

    /// List Dolt branches.
    pub fn dolt_branches(&self) -> Result<String, SqlError> {
        let output = Command::new("dolt")
            .args(["branch", "--list"])
            .current_dir(&self.db_path)
            .output()?;

        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// Query a table at a specific commit.
    pub fn query_at_commit(&self, sql: &str, commit: &str) -> Result<String, SqlError> {
        // Dolt supports AS OF syntax
        // But it's easier to use dolt sql with the commit ref
        // Dolt supports querying at a specific commit via AS OF
        let as_of_sql = format!("{sql} AS OF '{commit}'");
        self.query(&as_of_sql)
    }

    // --- Soft link queries ---

    /// Query soft links from a node.
    pub fn soft_links_from(&self, cid: &str) -> Result<String, SqlError> {
        self.query(&format!(
            "SELECT to_cid, relation, created_at FROM soft_links WHERE from_cid = '{cid}' ORDER BY created_at"
        ))
    }

    /// Query soft links to a node.
    pub fn soft_links_to(&self, cid: &str) -> Result<String, SqlError> {
        self.query(&format!(
            "SELECT from_cid, relation, created_at FROM soft_links WHERE to_cid = '{cid}' ORDER BY created_at"
        ))
    }

    /// Query all soft links for a node (both directions).
    pub fn soft_links_for(&self, cid: &str) -> Result<String, SqlError> {
        self.query(&format!(
            "SELECT from_cid, to_cid, relation, created_at FROM soft_links \
             WHERE from_cid = '{cid}' OR to_cid = '{cid}' ORDER BY created_at"
        ))
    }

    // --- Graph queries ---

    /// Find all children of a node (one level).
    pub fn children_of(&self, cid: &str) -> Result<String, SqlError> {
        self.query(&format!(
            "SELECT e.child_cid, n.kind, n.agent, n.created_at \
             FROM dag_edges e JOIN dag_nodes n ON e.child_cid = n.cid \
             WHERE e.parent_cid = '{cid}' ORDER BY e.ordinal"
        ))
    }

    /// Find all parents of a node (one level).
    pub fn parents_of(&self, cid: &str) -> Result<String, SqlError> {
        self.query(&format!(
            "SELECT e.parent_cid, n.kind, n.agent, n.created_at \
             FROM dag_edges e JOIN dag_nodes n ON e.parent_cid = n.cid \
             WHERE e.child_cid = '{cid}' ORDER BY e.ordinal"
        ))
    }

    /// Find root nodes (nodes with no parents).
    pub fn root_nodes(&self) -> Result<String, SqlError> {
        self.query(
            "SELECT n.cid, n.kind, n.agent, n.created_at \
             FROM dag_nodes n LEFT JOIN dag_edges e ON n.cid = e.child_cid \
             WHERE e.parent_cid IS NULL ORDER BY n.created_at"
        )
    }

    /// Find leaf nodes (nodes with no children).
    pub fn leaf_nodes(&self) -> Result<String, SqlError> {
        self.query(
            "SELECT n.cid, n.kind, n.agent, n.created_at \
             FROM dag_nodes n LEFT JOIN dag_edges e ON n.cid = e.parent_cid \
             WHERE e.child_cid IS NULL ORDER BY n.created_at"
        )
    }

    /// Count nodes by kind.
    pub fn node_counts_by_kind(&self) -> Result<String, SqlError> {
        self.query("SELECT kind, COUNT(*) AS n FROM dag_nodes GROUP BY kind ORDER BY n DESC")
    }

    /// Count nodes by agent.
    pub fn node_counts_by_agent(&self) -> Result<String, SqlError> {
        self.query("SELECT agent, COUNT(*) AS n FROM dag_nodes GROUP BY agent ORDER BY n DESC")
    }

    /// Summary stats for the database.
    pub fn stats(&self) -> Result<DbStats, SqlError> {
        let node_count = self.query("SELECT COUNT(*) AS n FROM dag_nodes")?;
        let edge_count = self.query("SELECT COUNT(*) AS n FROM dag_edges")?;
        let task_count = self.query("SELECT COUNT(*) AS n FROM tasks")?;
        let agent_count = self.query("SELECT COUNT(*) AS n FROM agents")?;
        let score_count = self.query("SELECT COUNT(*) AS n FROM scores")?;
        let link_count = self.query("SELECT COUNT(*) AS n FROM soft_links")?;
        let context_count = self.query("SELECT COUNT(*) AS n FROM context_files")?;
        let symbol_count = self.query("SELECT COUNT(*) AS n FROM cdom_symbols")?;
        let calibration_count = self.query("SELECT COUNT(*) AS n FROM calibrations")?;

        Ok(DbStats {
            nodes: parse_count(&node_count),
            edges: parse_count(&edge_count),
            tasks: parse_count(&task_count),
            agents: parse_count(&agent_count),
            scores: parse_count(&score_count),
            soft_links: parse_count(&link_count),
            context_files: parse_count(&context_count),
            symbols: parse_count(&symbol_count),
            calibrations: parse_count(&calibration_count),
        })
    }
}

/// Summary stats from the Dolt database.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DbStats {
    pub nodes: u64,
    pub edges: u64,
    pub tasks: u64,
    pub agents: u64,
    pub scores: u64,
    pub soft_links: u64,
    pub context_files: u64,
    pub symbols: u64,
    pub calibrations: u64,
}

fn parse_count(csv: &str) -> u64 {
    // CSV format: "n\n42\n"
    csv.lines()
        .nth(1)
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn escape_sql(s: &str) -> String {
    s.replace('\'', "''")
}
