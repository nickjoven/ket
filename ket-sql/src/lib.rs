//! Dolt SQL layer for Ket.
//!
//! Shells out to `dolt sql -q` for queries. Manages 6 tables:
//! dag_nodes, dag_edges, soft_links, tasks, agents, scores.

use std::path::{Path, PathBuf};
use std::process::Command;

/// An exclusive, cross-process advisory lock on one Dolt store.
///
/// Dolt's local storage does not tolerate concurrent writers — a second
/// `dolt sql -q` against the same store while another is writing fails with
/// "cannot update manifest: database is read only", and ket used to swallow
/// that error and silently drop the row. Every `dolt` invocation for a store
/// now runs while holding this lock, so access to the projection is
/// serialized across processes and threads. The CAS write that precedes a
/// projection sync is itself lock-free and content-addressed; only the Dolt
/// mirror serializes, which is inherent to Dolt and invisible to the caller
/// beyond a short wait. The lock is released when the guard's file descriptor
/// closes (flock semantics), so a crashed holder never wedges the store.
/// Held for the lifetime of one locked section, keyed by store path.
struct DoltGuard {
    path: PathBuf,
}

fn dolt_lock_path(db_path: &Path) -> PathBuf {
    db_path.join(".ket-dolt.lock")
}

// Reentrant within a thread: the public methods lock, and some of them call
// other locked methods (`init` → `create_schema` → `exec`; `dolt_commit` →
// `dolt_head`). A plain flock would self-deadlock there, because a second
// open file description on the same file blocks even within one process. A
// per-thread depth counter keyed by store path takes the real flock only at
// the outermost entry and releases it when the last guard drops. Two threads
// (or two processes) still exclude each other: each opens its own file
// description and blocks on the flock.
#[cfg(unix)]
thread_local! {
    static DOLT_LOCKS: std::cell::RefCell<std::collections::HashMap<PathBuf, (u32, std::fs::File)>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
}

fn acquire_dolt_lock(db_path: &Path) -> Result<DoltGuard, SqlError> {
    let key = db_path.to_path_buf();
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        DOLT_LOCKS.with(|cell| -> Result<(), SqlError> {
            let mut map = cell.borrow_mut();
            if let Some((depth, _)) = map.get_mut(&key) {
                *depth += 1; // already held on this thread; reentrant no-op
                return Ok(());
            }
            let file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(dolt_lock_path(db_path))?;
            // Blocking exclusive lock; released when `file` is dropped below.
            let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
            if rc != 0 {
                return Err(SqlError::Io(std::io::Error::last_os_error()));
            }
            map.insert(key.clone(), (1, file));
            Ok(())
        })?;
    }
    Ok(DoltGuard { path: key })
}

impl Drop for DoltGuard {
    fn drop(&mut self) {
        #[cfg(unix)]
        DOLT_LOCKS.with(|cell| {
            let mut map = cell.borrow_mut();
            if let Some((depth, _)) = map.get_mut(&self.path) {
                *depth -= 1;
                if *depth == 0 {
                    map.remove(&self.path); // drops the File → releases the flock
                }
            }
        });
    }
}

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

/// A row of the `dag_edges` projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeRow {
    pub parent_cid: String,
    pub child_cid: String,
    pub ordinal: i64,
    pub edge_kind: String,
}

/// The projected columns of a `dag_nodes` row that are a pure function of the
/// content-addressed node (so they can be re-derived and audited). `meta` is
/// omitted: ket always projects it empty, so it carries no state to verify.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRow {
    pub cid: String,
    pub kind: String,
    pub agent: String,
    pub created_at: String,
    pub output_cid: String,
    pub schema_cid: String,
}

/// Result of auditing the `dag_edges` projection against the CAS/DAG.
///
/// The projection is *correct* exactly when all three lists are empty: every
/// edge in SQL is derivable from a node in the CAS, with the same ordinal and
/// kind, and nothing extra.
#[derive(Debug, Default)]
pub struct ProjectionDiff {
    /// Edges the nodes imply but SQL lacks — the projection is stale/incomplete.
    pub missing: Vec<EdgeRow>,
    /// Edges in SQL with no node-derived counterpart — orphan/stale rows.
    pub extra: Vec<EdgeRow>,
    /// Same (parent, child) but ordinal or kind disagree: (expected, actual).
    pub mismatched: Vec<(EdgeRow, EdgeRow)>,
    /// Nodes the CAS implies but `dag_nodes` lacks — the loss concurrent
    /// writers used to cause silently. Edge-only auditing missed a lost
    /// parentless node entirely.
    pub missing_nodes: Vec<NodeRow>,
    /// Rows in `dag_nodes` with no node in the CAS — orphan/stale.
    pub extra_nodes: Vec<NodeRow>,
    /// Same cid, but a projected column disagrees: (expected, actual).
    pub mismatched_nodes: Vec<(NodeRow, NodeRow)>,
}

/// Result of replaying the substrate into the projection.
///
/// `edges_purged` is the count of rows wiped from `dag_edges` before the
/// replay; `edges_written` is the count of rows the replay re-inserted. A
/// healthy rebuild against an already-clean projection has
/// `purged == written`; a heal of a tampered projection will have them differ.
#[derive(Debug, Default)]
pub struct RebuildReport {
    pub nodes_purged: u64,
    pub nodes_written: usize,
    pub edges_purged: u64,
    pub edges_written: usize,
}

impl ProjectionDiff {
    pub fn is_clean(&self) -> bool {
        self.missing.is_empty()
            && self.extra.is_empty()
            && self.mismatched.is_empty()
            && self.missing_nodes.is_empty()
            && self.extra_nodes.is_empty()
            && self.mismatched_nodes.is_empty()
    }
}

/// Pure diff of two edge sets, keyed by the `dag_edges` primary key
/// `(parent_cid, child_cid)`. Factored out so the audit logic is testable
/// without a live Dolt database.
fn diff_edges(expected: Vec<EdgeRow>, actual: Vec<EdgeRow>) -> ProjectionDiff {
    use std::collections::HashMap;
    let key = |r: &EdgeRow| (r.parent_cid.clone(), r.child_cid.clone());

    let exp: HashMap<_, _> = expected.into_iter().map(|r| (key(&r), r)).collect();
    let act: HashMap<_, _> = actual.into_iter().map(|r| (key(&r), r)).collect();

    let mut diff = ProjectionDiff::default();
    for (k, e) in &exp {
        match act.get(k) {
            None => diff.missing.push(e.clone()),
            Some(a) if a.ordinal != e.ordinal || a.edge_kind != e.edge_kind => {
                diff.mismatched.push((e.clone(), a.clone()));
            }
            Some(_) => {}
        }
    }
    for (k, a) in &act {
        if !exp.contains_key(k) {
            diff.extra.push(a.clone());
        }
    }
    diff
}

/// Pure diff of two node sets keyed by cid, folded into an existing
/// `ProjectionDiff` (which also carries the edge diff).
fn diff_nodes(diff: &mut ProjectionDiff, expected: Vec<NodeRow>, actual: Vec<NodeRow>) {
    use std::collections::HashMap;
    let exp: HashMap<_, _> = expected.into_iter().map(|r| (r.cid.clone(), r)).collect();
    let act: HashMap<_, _> = actual.into_iter().map(|r| (r.cid.clone(), r)).collect();
    for (k, e) in &exp {
        match act.get(k) {
            None => diff.missing_nodes.push(e.clone()),
            Some(a) if a != e => diff.mismatched_nodes.push((e.clone(), a.clone())),
            Some(_) => {}
        }
    }
    for (k, a) in &act {
        if !exp.contains_key(k) {
            diff.extra_nodes.push(a.clone());
        }
    }
}

/// Split one CSV record into fields, honoring RFC-4180 double-quote quoting
/// (Dolt quotes a field that contains a comma, quote, or newline). Agent names
/// are user-controlled and may contain commas, so a naive `split(',')` would
/// mis-diff them.
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut cur = String::new();
    let mut chars = line.chars().peekable();
    let mut in_quotes = false;
    while let Some(c) = chars.next() {
        match c {
            '"' if in_quotes => {
                if chars.peek() == Some(&'"') {
                    cur.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            }
            '"' => in_quotes = true,
            ',' if !in_quotes => {
                fields.push(std::mem::take(&mut cur));
            }
            _ => cur.push(c),
        }
    }
    fields.push(cur);
    fields
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
        let _guard = acquire_dolt_lock(db_path)?;

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
        let _guard = acquire_dolt_lock(&self.db_path)?;
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
        let _guard = acquire_dolt_lock(&self.db_path)?;
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
        let _guard = acquire_dolt_lock(&self.db_path)?;
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
        let _guard = acquire_dolt_lock(&self.db_path)?;
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
                meta TEXT,
                schema_cid VARCHAR(64),
                saturation FLOAT
            )",
            "CREATE TABLE IF NOT EXISTS dag_edges (
                parent_cid VARCHAR(64) NOT NULL,
                child_cid VARCHAR(64) NOT NULL,
                ordinal INT NOT NULL DEFAULT 0,
                edge_kind VARCHAR(20) NOT NULL DEFAULT 'derives',
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

        // Migration: add edge_kind column to existing dag_edges tables
        let _ = self.exec(
            "ALTER TABLE dag_edges ADD COLUMN edge_kind VARCHAR(20) NOT NULL DEFAULT 'derives'",
        );

        self.commit("Initialize ket schema")?;
        Ok(())
    }

    /// Insert a DAG node record.
    // The arity mirrors the dag_nodes table columns one-for-one. Collapsing
    // them into a params struct would buy a lint and cost the call-site
    // readability that makes a column omission obvious at a glance.
    #[allow(clippy::too_many_arguments)]
    pub fn insert_dag_node(
        &self,
        cid: &str,
        kind: &str,
        agent: &str,
        created_at: &str,
        output_cid: &str,
        meta: &str,
        schema_cid: Option<&str>,
    ) -> Result<(), SqlError> {
        let schema_val = schema_cid.unwrap_or("");
        let sql = format!(
            "INSERT INTO dag_nodes (cid, kind, agent, created_at, output_cid, meta, schema_cid) \
             VALUES ('{cid}', '{kind}', '{}', '{created_at}', '{output_cid}', '{}', '{schema_val}')",
            escape_sql(agent),
            escape_sql(meta)
        );
        self.exec(&sql)
    }

    /// Insert a DAG edge with an epistemic kind.
    ///
    /// Edge kinds classify the epistemic relationship between parent and child:
    /// - `grounds`: parent is irreducible (axiom, measurement, definition)
    /// - `derives`: child follows from parent by stated mechanism (default)
    /// - `proposes`: child is suggested by parent but not entailed
    pub fn insert_dag_edge(
        &self,
        parent_cid: &str,
        child_cid: &str,
        ordinal: i32,
        edge_kind: &str,
    ) -> Result<(), SqlError> {
        let kind = validate_edge_kind(edge_kind);
        let sql = format!(
            "INSERT INTO dag_edges (parent_cid, child_cid, ordinal, edge_kind) \
             VALUES ('{parent_cid}', '{child_cid}', {ordinal}, '{kind}')"
        );
        self.exec(&sql)
    }

    /// Sync a DAG node + its edges to SQL in a single transaction.
    /// Uses INSERT IGNORE so re-syncing the same node is idempotent.
    ///
    /// Each parent is a tuple of (cid, ordinal, edge_kind).
    /// Edge kind defaults to "derives" when not specified.
    #[allow(clippy::too_many_arguments)]
    pub fn sync_dag_node(
        &self,
        cid: &str,
        kind: &str,
        agent: &str,
        created_at: &str,
        output_cid: &str,
        meta: &str,
        parent_cids: &[(&str, i32, &str)],
        schema_cid: Option<&str>,
    ) -> Result<(), SqlError> {
        let mut stmts = Vec::with_capacity(1 + parent_cids.len());

        let schema_val = schema_cid.unwrap_or("");
        stmts.push(format!(
            "INSERT IGNORE INTO dag_nodes (cid, kind, agent, created_at, output_cid, meta, schema_cid) \
             VALUES ('{cid}', '{kind}', '{}', '{created_at}', '{output_cid}', '{}', '{schema_val}')",
            escape_sql(agent),
            escape_sql(meta)
        ));

        for (parent_cid, ordinal, edge_kind) in parent_cids {
            let ek = validate_edge_kind(edge_kind);
            stmts.push(format!(
                "INSERT IGNORE INTO dag_edges (parent_cid, child_cid, ordinal, edge_kind) \
                 VALUES ('{parent_cid}', '{cid}', {ordinal}, '{ek}')"
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
        let sql =
            format!("UPDATE tasks SET status = '{status}', updated_at = '{now}' WHERE id = '{id}'");
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
        self.query("SELECT cid, kind, agent, created_at, output_cid, schema_cid FROM dag_nodes ORDER BY created_at")
    }

    /// Read every row of the `dag_edges` projection.
    pub fn dag_edges(&self) -> Result<Vec<EdgeRow>, SqlError> {
        let csv = self.query(
            "SELECT parent_cid, child_cid, ordinal, edge_kind FROM dag_edges \
             ORDER BY child_cid, ordinal",
        )?;
        let mut rows = Vec::new();
        for line in csv.lines().skip(1) {
            // CSV: parent_cid,child_cid,ordinal,edge_kind. CIDs are hex and
            // kinds are bare words, so a plain split is safe here.
            let f: Vec<&str> = line.split(',').collect();
            if f.len() != 4 {
                continue;
            }
            rows.push(EdgeRow {
                parent_cid: f[0].to_string(),
                child_cid: f[1].to_string(),
                ordinal: f[2].parse().unwrap_or(0),
                edge_kind: f[3].to_string(),
            });
        }
        Ok(rows)
    }

    /// Audit the `dag_edges` projection against the CAS/DAG — the source of
    /// truth — and return what disagrees.
    ///
    /// Re-derives the edges every `DagNode` in `cas` implies (via
    /// `DagNode::parent_links()`) and diffs them against what `dag_edges` holds.
    /// A clean result proves the projection is *reconstructible* from the
    /// substrate (self-heal) and *agrees* with it (self-audit). This is the L3
    /// mirror of `verify_cas` — and it only became possible once the edge kind
    /// moved into the content-addressed node (ket >= 0.3), so this table no
    /// longer holds primary state with no upstream source.
    ///
    /// Sketch: linear scan of the CAS and a full in-memory read of the table. A
    /// production version would page both rather than collecting them whole.
    /// `rebuild_projection` is the companion *heal* — replay the derived edges
    /// back into Dolt to repair any divergence this surfaces.
    pub fn verify_projection(&self, cas: &ket_cas::Store) -> Result<ProjectionDiff, SqlError> {
        let expected = expected_edges_from_cas(cas)?;
        let mut diff = diff_edges(expected, self.dag_edges()?);
        diff_nodes(
            &mut diff,
            expected_nodes_from_cas(cas)?,
            self.dag_node_rows()?,
        );
        Ok(diff)
    }

    /// The `dag_nodes` rows, as projected columns, for the audit.
    pub fn dag_node_rows(&self) -> Result<Vec<NodeRow>, SqlError> {
        let csv = self.query(
            "SELECT cid, kind, agent, created_at, output_cid, schema_cid FROM dag_nodes ORDER BY cid",
        )?;
        let mut rows = Vec::new();
        for line in csv.lines().skip(1) {
            let f = parse_csv_line(line);
            if f.len() != 6 {
                continue;
            }
            rows.push(NodeRow {
                cid: f[0].clone(),
                kind: f[1].clone(),
                agent: f[2].clone(),
                created_at: f[3].clone(),
                output_cid: f[4].clone(),
                schema_cid: f[5].clone(),
            });
        }
        Ok(rows)
    }

    /// Heal the `dag_edges` projection by replaying the substrate.
    ///
    /// Truncates `dag_edges` and re-inserts every edge derivable from the
    /// `DagNode`s in `cas`, all inside one transaction. Idempotent: running it
    /// twice from the same CAS leaves the projection bit-identical. After it
    /// returns, `verify_projection(cas).is_clean()` must hold — that is the
    /// invariant; if it doesn't, the canonicalization of edges itself is the
    /// bug, not the projection.
    ///
    /// Sketch: full in-memory read of the CAS into one batch, same as
    /// `verify_projection`. A production version would page the replay; here
    /// the rebuild is the *audit's heal partner*, not the high-throughput path.
    /// Closes the second DESIGN.md L3 target (`verify` had no `rebuild`, so a
    /// surfaced divergence had no mechanical fix).
    pub fn rebuild_projection(&self, cas: &ket_cas::Store) -> Result<RebuildReport, SqlError> {
        let expected_edges = expected_edges_from_cas(cas)?;
        let expected_nodes = expected_nodes_from_cas(cas)?;

        // One transaction: wipe + replay BOTH dag_nodes and dag_edges from the
        // CAS. exec_batch wraps in BEGIN/COMMIT, so a failure mid-replay leaves
        // the projection at its pre-rebuild state. Only the two DAG-projection
        // tables are touched; soft_links, scores, tasks and agents are primary
        // Dolt state with no CAS upstream and are left alone.
        let mut stmts = Vec::with_capacity(2 + expected_nodes.len() + expected_edges.len());
        stmts.push("DELETE FROM dag_edges".to_string());
        stmts.push("DELETE FROM dag_nodes".to_string());
        for n in &expected_nodes {
            stmts.push(format!(
                "INSERT INTO dag_nodes (cid, kind, agent, created_at, output_cid, meta, schema_cid) \
                 VALUES ('{}', '{}', '{}', '{}', '{}', '', '{}')",
                n.cid,
                n.kind,
                escape_sql(&n.agent),
                n.created_at,
                n.output_cid,
                n.schema_cid,
            ));
        }
        for row in &expected_edges {
            stmts.push(format!(
                "INSERT INTO dag_edges (parent_cid, child_cid, ordinal, edge_kind) \
                 VALUES ('{}', '{}', {}, '{}')",
                row.parent_cid,
                row.child_cid,
                row.ordinal,
                validate_edge_kind(&row.edge_kind),
            ));
        }

        // Row counts before the purge are the audit-relevant "what was there";
        // read first so the report is honest even if the replay surprises us.
        let edges_purged = self.count_dag_edges()?;
        let nodes_purged = self.count_dag_nodes()?;
        self.exec_batch(&stmts)?;

        Ok(RebuildReport {
            nodes_purged,
            nodes_written: expected_nodes.len(),
            edges_purged,
            edges_written: expected_edges.len(),
        })
    }

    /// Row count of `dag_edges`, used by [`rebuild_projection`] to report what
    /// the wipe removed before the replay.
    fn count_dag_edges(&self) -> Result<u64, SqlError> {
        let csv = self.query("SELECT COUNT(*) AS n FROM dag_edges")?;
        let n = csv
            .lines()
            .nth(1)
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0);
        Ok(n)
    }

    fn count_dag_nodes(&self) -> Result<u64, SqlError> {
        let csv = self.query("SELECT COUNT(*) AS n FROM dag_nodes")?;
        let n = csv
            .lines()
            .nth(1)
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0);
        Ok(n)
    }

    /// Query all agents.
    pub fn list_agents(&self) -> Result<String, SqlError> {
        self.query(
            "SELECT name, cli_command, mcp_capable, model, updated_at FROM agents ORDER BY name",
        )
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
        self.query("SELECT kind, COUNT(*) AS n FROM cdom_symbols GROUP BY kind ORDER BY n DESC")
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
        let _guard = acquire_dolt_lock(&self.db_path)?;
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
        let _guard = acquire_dolt_lock(&self.db_path)?;
        let output = Command::new("dolt")
            .args(["log", "-n", "1", "--oneline"])
            .current_dir(&self.db_path)
            .output()?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        Ok(stdout
            .split_whitespace()
            .next()
            .unwrap_or("unknown")
            .to_string())
    }

    /// Get commit history.
    pub fn dolt_log(&self, n: usize) -> Result<String, SqlError> {
        let _guard = acquire_dolt_lock(&self.db_path)?;
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
        let _guard = acquire_dolt_lock(&self.db_path)?;
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
             WHERE e.parent_cid IS NULL ORDER BY n.created_at",
        )
    }

    /// Find leaf nodes (nodes with no children).
    pub fn leaf_nodes(&self) -> Result<String, SqlError> {
        self.query(
            "SELECT n.cid, n.kind, n.agent, n.created_at \
             FROM dag_nodes n LEFT JOIN dag_edges e ON n.cid = e.parent_cid \
             WHERE e.child_cid IS NULL ORDER BY n.created_at",
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

    // --- Saturation queries ---

    /// Write the saturation value for a node into the SQL index.
    ///
    /// The canonical saturation lives in the CAS-stored `DagNode` metadata.
    /// This mirrors it into a dedicated SQL column so the optimizer and CLI can
    /// run efficient range queries without scanning CAS blobs.
    ///
    /// Call this after creating or updating a node that carries a `saturation`
    /// key in its `DagNode.meta`.
    pub fn set_node_saturation(&self, cid: &str, saturation: f32) -> Result<(), SqlError> {
        let clamped = saturation.clamp(0.0, 1.0);
        self.exec(&format!(
            "UPDATE dag_nodes SET saturation = {clamped} WHERE cid = '{cid}'"
        ))
    }

    /// Return nodes that are open **queries** — saturation is NULL (undeclared)
    /// or 0.0 (explicitly unresolved). These are the nodes that most need
    /// exploration; no separate query substrate table is required.
    pub fn open_queries(&self) -> Result<String, SqlError> {
        self.query(
            "SELECT cid, kind, agent, created_at, saturation \
             FROM dag_nodes \
             WHERE saturation IS NULL OR saturation = 0.0 \
             ORDER BY created_at DESC",
        )
    }

    /// Return nodes that are settled **claims** — saturation = 1.0.
    /// These can be safely pruned by the optimizer (info_potential = 0).
    pub fn settled_claims(&self) -> Result<String, SqlError> {
        self.query(
            "SELECT cid, kind, agent, created_at, saturation \
             FROM dag_nodes \
             WHERE saturation >= 1.0 \
             ORDER BY created_at DESC",
        )
    }

    /// Return nodes with saturation below `threshold` — partially resolved
    /// beliefs that still carry exploration potential for the optimizer.
    pub fn nodes_below_saturation(&self, threshold: f32) -> Result<String, SqlError> {
        self.query(&format!(
            "SELECT cid, kind, agent, created_at, saturation \
             FROM dag_nodes \
             WHERE saturation < {threshold} \
             ORDER BY saturation ASC, created_at DESC"
        ))
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

/// Validate and normalize an edge kind string.
///
/// Valid edge kinds:
/// - `grounds`: irreducible input (axiom, measurement, definition)
/// - `derives`: logically follows from parent (default)
/// - `proposes`: suggested by parent but not entailed (hypothesis)
/// - `confirms` / `refutes`: verification evidence for / against the parent
/// - `supersedes`: child replaces parent as canonical; parent stays addressable
fn validate_edge_kind(kind: &str) -> &str {
    match kind {
        "grounds" | "derives" | "proposes" | "confirms" | "refutes" | "supersedes" => kind,
        "" => "derives",
        _ => "derives", // unknown kinds fall back to derives
    }
}

/// Derive every edge the substrate implies, in canonical
/// `(child_cid, ordinal)` order. The single source of truth for what
/// `dag_edges` *ought* to contain — shared by `verify_projection` (audit) and
/// `rebuild_projection` (heal), so they cannot disagree about what's expected.
///
/// Only blobs that parse as `DagNode`s contribute; content/schema blobs are
/// silently skipped (they have no edges to project).
fn expected_edges_from_cas(cas: &ket_cas::Store) -> Result<Vec<EdgeRow>, SqlError> {
    let mut expected = Vec::new();
    let mut cids = cas.list().map_err(|e| SqlError::DoltError(e.to_string()))?;
    cids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    for cid in cids {
        let bytes = match cas.get(&cid) {
            Ok(b) => b,
            Err(_) => continue,
        };
        let node = match ket_dag::DagNode::from_bytes(&bytes) {
            Ok(n) => n,
            Err(_) => continue,
        };
        let child = cid.as_str().to_string();
        for (i, (parent, kind)) in node.parent_links().enumerate() {
            expected.push(EdgeRow {
                parent_cid: parent.as_str().to_string(),
                child_cid: child.clone(),
                ordinal: i as i64,
                edge_kind: kind.as_str().to_string(),
            });
        }
    }
    Ok(expected)
}

/// Derive the `dag_nodes` rows every node in the CAS implies — the companion
/// of [`expected_edges_from_cas`] for the node table. Shared by the audit and
/// the heal so they cannot disagree about what the projection should hold.
fn expected_nodes_from_cas(cas: &ket_cas::Store) -> Result<Vec<NodeRow>, SqlError> {
    let mut expected = Vec::new();
    let mut cids = cas.list().map_err(|e| SqlError::DoltError(e.to_string()))?;
    cids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    for cid in cids {
        let bytes = match cas.get(&cid) {
            Ok(b) => b,
            Err(_) => continue,
        };
        let node = match ket_dag::DagNode::from_bytes(&bytes) {
            Ok(n) => n,
            Err(_) => continue,
        };
        expected.push(NodeRow {
            cid: cid.as_str().to_string(),
            kind: node.kind.to_string(),
            agent: node.agent.clone(),
            created_at: node.timestamp.clone(),
            output_cid: node.output_cid.as_str().to_string(),
            schema_cid: node
                .schema_cid
                .as_ref()
                .map(|c| c.as_str())
                .unwrap_or("")
                .to_string(),
        });
    }
    Ok(expected)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edge(parent: &str, child: &str, ord: i64, kind: &str) -> EdgeRow {
        EdgeRow {
            parent_cid: parent.into(),
            child_cid: child.into(),
            ordinal: ord,
            edge_kind: kind.into(),
        }
    }

    #[test]
    fn projection_in_sync_is_clean() {
        let nodes = vec![edge("a", "c", 0, "grounds"), edge("b", "c", 1, "derives")];
        let sql = nodes.clone();
        assert!(diff_edges(nodes, sql).is_clean());
    }

    #[test]
    fn rebuild_plan_matches_verify_plan_over_a_real_cas() {
        // The single fact: verify and rebuild compute the *same* expected
        // edge set. If they ever drift, a heal would re-introduce the
        // divergence the audit just flagged.
        use ket_cas::Store as CasStore;
        use ket_dag::{DagNode, EdgeKind, NodeKind};
        use std::fs;

        let dir = std::env::temp_dir().join("ket-sql-rebuild-plan-test");
        let _ = fs::remove_dir_all(&dir);
        let cas = CasStore::init(&dir).unwrap();

        // Two grounding leaves + one node with mixed-kind parents. The mixed
        // kinds are what node-sourced edges *had* to make possible; verifying
        // them lives in the same plan rebuild relies on.
        let a = cas.put(b"axiom-a").unwrap();
        let b = cas.put(b"axiom-b").unwrap();
        let out = cas.put(b"derived-output").unwrap();
        let child = DagNode::new_typed(
            NodeKind::Reasoning,
            vec![
                (a.clone(), EdgeKind::Grounds),
                (b.clone(), EdgeKind::Derives),
            ],
            out,
            "claude",
        );
        let child_cid = cas.put(&child.to_bytes().unwrap()).unwrap();

        let plan = expected_edges_from_cas(&cas).unwrap();
        // Both grounding-leaf blobs are not DagNodes, so they contribute no
        // edges. Only `child` contributes — two parent links.
        let mut have: Vec<_> = plan
            .iter()
            .filter(|r| r.child_cid == child_cid.as_str())
            .collect();
        have.sort_by_key(|r| r.ordinal);
        assert_eq!(have.len(), 2);
        assert_eq!(have[0].parent_cid, a.as_str());
        assert_eq!(have[0].edge_kind, "grounds");
        assert_eq!(have[1].parent_cid, b.as_str());
        assert_eq!(have[1].edge_kind, "derives");

        // Verify-shape: feeding the plan as both expected and actual is
        // clean (the round-trip invariant rebuild must satisfy).
        assert!(diff_edges(plan.clone(), plan).is_clean());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn projection_diff_classifies_every_divergence() {
        // expected (from nodes): a->c grounds@0, b->c derives@1, d->e derives@0
        let expected = vec![
            edge("a", "c", 0, "grounds"),
            edge("b", "c", 1, "derives"),
            edge("d", "e", 0, "derives"),
        ];
        // actual (in SQL): a->c kind disagrees (stale), b->c ok, d->e absent,
        // x->y orphan row with no node behind it.
        let actual = vec![
            edge("a", "c", 0, "derives"), // kind drifted from grounds
            edge("b", "c", 1, "derives"), // agrees
            edge("x", "y", 0, "derives"), // orphan
        ];

        let diff = diff_edges(expected, actual);
        assert!(!diff.is_clean());

        // d->e is implied by a node but missing from SQL.
        assert_eq!(diff.missing, vec![edge("d", "e", 0, "derives")]);
        // x->y is in SQL with no node behind it.
        assert_eq!(diff.extra, vec![edge("x", "y", 0, "derives")]);
        // a->c exists in both but the kind disagrees.
        assert_eq!(diff.mismatched.len(), 1);
        let (exp, act) = &diff.mismatched[0];
        assert_eq!(exp.edge_kind, "grounds");
        assert_eq!(act.edge_kind, "derives");
    }

    fn node(cid: &str, agent: &str) -> NodeRow {
        NodeRow {
            cid: cid.into(),
            kind: "memory".into(),
            agent: agent.into(),
            created_at: "2026-01-01T00:00:00Z".into(),
            output_cid: "o".into(),
            schema_cid: "".into(),
        }
    }

    #[test]
    fn diff_nodes_flags_missing_extra_and_altered_rows() {
        let mut diff = ProjectionDiff::default();
        diff_nodes(
            &mut diff,
            vec![node("a", "honest"), node("b", "x")], // expected (CAS)
            vec![node("a", "evil"), node("c", "y")],   // actual (SQL)
        );
        // "a" present in both but agent altered -> mismatch; "b" only in CAS
        // -> missing; "c" only in SQL -> extra.
        assert_eq!(diff.mismatched_nodes.len(), 1);
        assert_eq!(diff.mismatched_nodes[0].0.agent, "honest");
        assert_eq!(diff.mismatched_nodes[0].1.agent, "evil");
        assert_eq!(diff.missing_nodes.len(), 1);
        assert_eq!(diff.missing_nodes[0].cid, "b");
        assert_eq!(diff.extra_nodes.len(), 1);
        assert_eq!(diff.extra_nodes[0].cid, "c");
        assert!(!diff.is_clean());
    }

    #[test]
    fn parse_csv_line_honors_quoting() {
        // A field with a comma is quoted by Dolt; a doubled quote is one quote.
        assert_eq!(parse_csv_line("a,b,c"), vec!["a", "b", "c"]);
        assert_eq!(
            parse_csv_line(r#"cid,memory,"audit:sec, and more",ts"#),
            vec!["cid", "memory", "audit:sec, and more", "ts"]
        );
        // A doubled quote inside a quoted field decodes to a single quote.
        assert_eq!(
            parse_csv_line(r#""he said ""hi""",tail"#),
            vec![r#"he said "hi""#, "tail"]
        );
    }
}
