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

    /// Query scores for a node.
    pub fn scores_for_node(&self, node_cid: &str) -> Result<String, SqlError> {
        self.query(&format!(
            "SELECT dimension, value, scorer, evidence FROM scores WHERE node_cid = '{node_cid}'"
        ))
    }
}

fn escape_sql(s: &str) -> String {
    s.replace('\'', "''")
}
