//! Agent orchestration for multi-agent coordination.
//!
//! Manages agent registry, task lifecycle, subprocess spawning.
//! Supports Claude, Codex, and Copilot agents.

use ket_cas::{Cid, Store as CasStore};
use ket_dag::{Dag, DagNode, NodeKind};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    #[error("SQL error: {0}")]
    Sql(#[from] ket_sql::SqlError),
    #[error("CAS error: {0}")]
    Cas(#[from] ket_cas::CasError),
    #[error("DAG error: {0}")]
    Dag(#[from] ket_dag::DagError),
    #[error("Agent not found: {0}")]
    NotFound(String),
    #[error("Task not found: {0}")]
    TaskNotFound(String),
    #[error("Agent invocation failed: {0}")]
    InvocationFailed(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("task {task} is {status}; it was not run (pass --force to run it anyway)")]
    NotRunnable { task: String, status: String },
    #[error("Agent timed out: {0}")]
    TimedOut(String),
    #[error("Interrupted: {0}")]
    Interrupted(String),
    #[error("Max subtask depth ({0}) exceeded")]
    MaxDepth(u32),
    #[error("Task context unavailable: {0}")]
    ContextUnavailable(String),
}

/// Task status in the lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Pending,
    Assigned,
    Running,
    Done,
    Failed,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Pending => "pending",
            TaskStatus::Assigned => "assigned",
            TaskStatus::Running => "running",
            TaskStatus::Done => "done",
            TaskStatus::Failed => "failed",
        }
    }
}

/// Agent configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    pub name: String,
    pub cli_command: String,
    pub mcp_capable: bool,
    pub capabilities: Vec<String>,
    pub model: Option<String>,
}

impl AgentConfig {
    pub fn claude() -> Self {
        AgentConfig {
            name: "claude".to_string(),
            cli_command: "claude -p --output-format json".to_string(),
            mcp_capable: true,
            capabilities: vec!["code".into(), "reasoning".into(), "review".into()],
            model: Some("claude-sonnet-4-5-20250929".to_string()),
        }
    }

    pub fn codex() -> Self {
        AgentConfig {
            name: "codex".to_string(),
            cli_command: "codex exec".to_string(),
            mcp_capable: false,
            capabilities: vec!["code".into(), "execution".into()],
            model: None,
        }
    }

    pub fn copilot() -> Self {
        AgentConfig {
            name: "copilot".to_string(),
            cli_command: "gh copilot suggest".to_string(),
            mcp_capable: true,
            capabilities: vec!["code".into(), "suggestion".into()],
            model: None,
        }
    }
}

/// Upper bound on the failure reason kept in `tasks.meta`.
const MAX_REASON_BYTES: usize = 8 * 1024;

/// `s` if it fits in `max` bytes, else its head and tail (where an error
/// message and a stack trace's last line usually are) around a marker
/// saying how much was dropped. Cuts land on char boundaries.
fn bounded(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let half = max / 2;
    let mut head = half;
    while !s.is_char_boundary(head) {
        head -= 1;
    }
    let mut tail = s.len() - half;
    while !s.is_char_boundary(tail) {
        tail += 1;
    }
    format!(
        "{}\n[... {} bytes elided ...]\n{}",
        &s[..head],
        tail - head,
        &s[tail..]
    )
}

/// How long an agent's output pipes are read after it has exited, and how
/// long a killed agent is waited for. Output written before the exit is
/// already in the pipe and is read at once; the grace only bounds a pipe
/// that a process the agent left behind is still holding open.
const OUTPUT_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

/// How a run is allowed to proceed.
#[derive(Debug, Clone, Default)]
pub struct RunOptions {
    /// Kill the agent and fail the task (phase `timeout`) if it is still
    /// running after this long. `None` waits for as long as it takes.
    pub timeout: Option<std::time::Duration>,
    /// Run a task that is already `running` or `done`. Without it such a
    /// task is refused, so concurrent runs cannot race on one task and a
    /// finished task is not silently run again.
    pub force: bool,
}

/// Sleep until `deadline`; never completes when there is none.
async fn at(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending().await,
    }
}

/// Resolves with the name of the first SIGINT or SIGTERM delivered to this
/// process. The handlers are installed when this is called, not when the
/// returned future is first polled. If they cannot be installed the future
/// never resolves and the run proceeds without interrupt handling.
fn shutdown_signal() -> impl std::future::Future<Output = &'static str> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let int = signal(SignalKind::interrupt());
        let term = signal(SignalKind::terminate());
        async move {
            match (int, term) {
                (Ok(mut int), Ok(mut term)) => tokio::select! {
                    _ = int.recv() => "SIGINT",
                    _ = term.recv() => "SIGTERM",
                },
                _ => std::future::pending().await,
            }
        }
    }
    #[cfg(not(unix))]
    async {
        match tokio::signal::ctrl_c().await {
            Ok(()) => "Ctrl-C",
            Err(_) => std::future::pending().await,
        }
    }
}

/// The agent orchestrator.
pub struct Orchestrator<'a> {
    cas: &'a CasStore,
    db: &'a ket_sql::DoltDb,
}

impl<'a> Orchestrator<'a> {
    pub fn new(cas: &'a CasStore, db: &'a ket_sql::DoltDb) -> Self {
        Orchestrator { cas, db }
    }

    /// Register an agent in the database.
    pub fn register_agent(&self, config: &AgentConfig) -> Result<(), AgentError> {
        let caps = config.capabilities.join(",");
        let model = config.model.as_deref().unwrap_or("");
        self.db.upsert_agent(
            &config.name,
            &config.cli_command,
            config.mcp_capable,
            &caps,
            model,
        )?;
        Ok(())
    }

    /// Create a new task.
    pub fn create_task(
        &self,
        title: &str,
        created_by: &str,
        parent_task: Option<&str>,
        context_cid: Option<&Cid>,
    ) -> Result<String, AgentError> {
        let id = uuid::Uuid::now_v7().to_string();
        self.db.insert_task(
            &id,
            title,
            created_by,
            parent_task,
            context_cid.map(|c| c.as_str()),
        )?;
        Ok(id)
    }

    /// Assign a task to an agent.
    pub fn assign_task(&self, task_id: &str, agent: &str) -> Result<(), AgentError> {
        if !self.db.task_exists(task_id)? {
            return Err(AgentError::NotFound(format!("task {task_id}")));
        }
        if !self.agent_is_known(agent)? {
            return Err(AgentError::NotFound(format!(
                "agent {agent} (register it first, or use a preset: claude/codex/copilot)"
            )));
        }
        self.db.assign_task(task_id, agent)?;
        Ok(())
    }

    /// An agent is known if it is registered or is one of the built-in presets
    /// `run` can invoke. Guards `assign`/`run` from silently accepting a
    /// misspelled agent that would only fail (or do nothing) at run time.
    fn agent_is_known(&self, agent: &str) -> Result<bool, AgentError> {
        if matches!(agent, "claude" | "codex" | "copilot") {
            return Ok(true);
        }
        Ok(self.db.agent_command(agent)?.is_some())
    }

    /// The bytes a task's context names, for `run_task` to put on the
    /// agent's stdin: `None` when the task has no context, the node's
    /// content when the context is a DAG node (as `dag create --kind
    /// context` makes), and the blob itself otherwise.
    ///
    /// A context that cannot be read (a dangling CID, a node whose content
    /// is gone, a CAS read error) fails the task with phase `context` and a
    /// reason naming the CID: running the agent without the material it was
    /// asked to work on would be a wrong answer, not a degraded one. Only a
    /// blob that does not parse as a node counts as "not a node"; a read
    /// error is never mistaken for one.
    pub fn task_context(&self, task_id: &str) -> Result<Option<Vec<u8>>, AgentError> {
        let Some(ctx) = self.db.task_context_cid(task_id)? else {
            return Ok(None);
        };
        let cid = Cid::from(ctx.as_str());
        let resolved = self
            .cas
            .get(&cid)
            .map_err(|e| format!("the task's context {cid} could not be read: {e}"))
            .and_then(|bytes| match DagNode::from_bytes(&bytes) {
                Ok(node) => self.cas.get(&node.output_cid).map_err(|e| {
                    format!(
                        "the task's context {cid} is a DAG node whose content {} could not be read: {e}",
                        node.output_cid
                    )
                }),
                Err(_) => Ok(bytes),
            });
        match resolved {
            Ok(bytes) => Ok(Some(bytes)),
            Err(reason) => {
                let err = AgentError::ContextUnavailable(format!("task {task_id}: {reason}"));
                Err(self.fail_task(task_id, "context", &reason, err))
            }
        }
    }

    /// Run a task by spawning the assigned agent.
    ///
    /// Every run ends in a recorded state. Once the task has been claimed
    /// (`running`), each way out — a launch failure, a nonzero exit, a
    /// timeout, SIGINT/SIGTERM delivered to ket, a result that cannot be
    /// stored — moves it to `failed` with the phase and reason in
    /// `tasks.meta`. The one exception cannot be closed from inside the
    /// process: SIGKILL of ket itself leaves the task `running`, and a later
    /// `run` refuses it until `force` reclaims it.
    pub async fn run_task(
        &self,
        task_id: &str,
        prompt: &str,
        agent_name: &str,
        opts: &RunOptions,
    ) -> Result<Cid, AgentError> {
        // Refuse an unknown task or agent instead of running and minting an
        // orphan node.
        if !self.db.task_exists(task_id)? {
            return Err(AgentError::NotFound(format!("task {task_id}")));
        }
        if !self.agent_is_known(agent_name)? {
            return Err(AgentError::NotFound(format!(
                "agent {agent_name} (register it first, or use a preset: claude/codex/copilot)"
            )));
        }
        // Resolve what to execute before the task is marked `running`: every
        // way this can fail (an agent with no command, a blank registered
        // command, a database error) leaves the task as it was instead of
        // stuck `running` with nothing executing.
        //
        // Prefer the command `ket agent register` stored for this agent; the
        // prompt is appended as the final argument (and the context, if any,
        // is written to stdin, below). Fall back to the built-in
        // presets, then error. (Simple whitespace splitting: agent commands
        // are flags, not shell.) This is the fix for `run` ignoring the
        // registered `cli_command`.
        let (program, args): (String, Vec<String>) = if let Some(cmd) =
            self.db.agent_command(agent_name)?
        {
            let mut parts = cmd.split_whitespace().map(str::to_string);
            let program = parts.next().ok_or_else(|| {
                    AgentError::NotFound(format!(
                        "agent {agent_name} has a blank registered command; `ket agent register {agent_name} --command <cmd>`"
                    ))
                })?;
            let mut args: Vec<String> = parts.collect();
            args.push(prompt.to_string());
            (program, args)
        } else {
            match agent_name {
                    "claude" => (
                        "claude".to_string(),
                        vec![
                            "-p".to_string(),
                            "--output-format".to_string(),
                            "json".to_string(),
                            prompt.to_string(),
                        ],
                    ),
                    "codex" => (
                        "codex".to_string(),
                        vec!["exec".to_string(), prompt.to_string()],
                    ),
                    _ => {
                        return Err(AgentError::NotFound(format!(
                            "agent {agent_name} has no registered command and no built-in invocation; `ket agent register {agent_name}` first"
                        )))
                    }
                }
        };

        // Listen for SIGINT/SIGTERM before the task reads `running`, so there
        // is no window in which an interrupt ends ket with the task claimed.
        let interrupted = shutdown_signal();
        tokio::pin!(interrupted);

        // Claim the task: check its status and mark it `running` in one
        // locked step. A task another run holds (or one that already
        // finished) is refused and left exactly as it was; two `ket run`s
        // cannot both start it. The claim also clears the previous attempt's
        // failure reason and result, which belong to that attempt.
        match self.db.claim_task_for_run(task_id, opts.force)? {
            ket_sql::RunClaim::Claimed { .. } => {}
            ket_sql::RunClaim::NotFound => {
                return Err(AgentError::NotFound(format!("task {task_id}")))
            }
            ket_sql::RunClaim::Refused { status } => {
                return Err(AgentError::NotRunnable {
                    task: task_id.to_string(),
                    status,
                })
            }
        }

        // From here on the task reads `running`, so every exit that is not
        // success goes through `fail_task` and records why. The context is
        // resolved after the claim: a context that cannot be read fails the
        // task (`task_context`), and only the run that holds the task may
        // rewrite its status.
        let context = self.task_context(task_id)?;
        let context = context.as_deref();

        // Spawn the process. A launch failure (missing executable, permission
        // denied) used to propagate before the status update, leaving the task
        // `running` forever (#21): every exit from here on records a reason.
        //
        // The prompt (task line and instructions) is the final argument; the
        // task's context, which can be any size and any shape, goes to the
        // agent's stdin. As an argument it could exceed the per-argument
        // limit (128 KiB on Linux, E2BIG) and, coming first, a context that
        // began with `--` read as a flag. Both presets take piped input
        // alongside a prompt argument: `claude -p` reads stdin as input to
        // the prompt, `codex exec` appends it as a `<stdin>` block.
        let mut command = tokio::process::Command::new(&program);
        command
            .args(&args)
            .stdin(if context.is_some() {
                std::process::Stdio::piped()
            } else {
                std::process::Stdio::null()
            })
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            // If this future is dropped (a panic, a caller's own timeout),
            // the agent does not outlive the run that was tracking it.
            .kill_on_drop(true);
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(e) => {
                let reason = format!("{program}: {e}");
                let err = AgentError::InvocationFailed(reason.clone());
                return Err(self.fail_task(task_id, "launch", &reason, err));
            }
        };
        // Feed stdin while collecting output, so neither side can fill a pipe
        // and block the other. An agent that exits without reading all of its
        // context closes the pipe; that is its choice, not a ket failure.
        // The context is written byte for byte: it is content, not text, and
        // a binary blob must arrive as stored.
        let stdin = child.stdin.take();
        let feed = async move {
            if let (Some(ctx), Some(mut stdin)) = (context, stdin) {
                use tokio::io::AsyncWriteExt;
                match stdin.write_all(ctx).await {
                    Err(e) if e.kind() != std::io::ErrorKind::BrokenPipe => return Err(e),
                    _ => {}
                }
            }
            Ok(())
        };
        tokio::pin!(feed);
        let mut stdout = child.stdout.take().expect("stdout was piped");
        let mut stderr = child.stderr.take().expect("stderr was piped");
        let (mut out, mut errout) = (Vec::new(), Vec::new());
        let (mut out_open, mut err_open) = (true, true);
        // `None` until the feed finishes; it is abandoned once the agent has
        // exited. A background process the agent left behind can hold the
        // pipe open without ever reading it, and waiting on that write would
        // hang `ket run` for as long as that process lives.
        let mut fed: Option<std::io::Result<()>> = None;
        let mut status: Option<std::process::ExitStatus> = None;
        let deadline = opts.timeout.map(|t| tokio::time::Instant::now() + t);
        // Set when the agent exits: how long its output pipes may stay open
        // after that (the same left-behind process can hold those too).
        let mut drain_deadline: Option<tokio::time::Instant> = None;

        let stopped: Option<(&str, String)> = loop {
            if status.is_some() && !out_open && !err_open {
                break None;
            }
            use tokio::io::AsyncReadExt;
            tokio::select! {
                r = &mut feed, if fed.is_none() && status.is_none() => fed = Some(r),
                n = stdout.read_buf(&mut out), if out_open => {
                    out_open = matches!(n, Ok(n) if n > 0);
                }
                n = stderr.read_buf(&mut errout), if err_open => {
                    err_open = matches!(n, Ok(n) if n > 0);
                }
                exited = child.wait(), if status.is_none() => match exited {
                    Ok(s) => {
                        status = Some(s);
                        drain_deadline = Some(tokio::time::Instant::now() + OUTPUT_DRAIN_GRACE);
                    }
                    Err(e) => break Some(("exit", format!("{program}: waiting for it failed: {e}"))),
                },
                _ = at(drain_deadline), if drain_deadline.is_some() => break None,
                _ = at(deadline), if deadline.is_some() && status.is_none() => {
                    let limit = opts.timeout.unwrap_or_default();
                    break Some((
                        "timeout",
                        format!("{program} was still running after {limit:?} and was killed"),
                    ));
                }
                signal = &mut interrupted, if status.is_none() => {
                    break Some((
                        "interrupted",
                        format!("ket received {signal} while {program} was running; the agent was killed"),
                    ));
                }
            }
        };
        if let Some((phase, reason)) = stopped {
            // Stop the agent and reap it (bounded: a process that cannot be
            // reaped must not turn a recorded failure into a hang).
            let _ = child.start_kill();
            let _ = tokio::time::timeout(OUTPUT_DRAIN_GRACE, child.wait()).await;
            let err = match phase {
                "timeout" => AgentError::TimedOut(reason.clone()),
                "interrupted" => AgentError::Interrupted(reason.clone()),
                _ => AgentError::InvocationFailed(reason.clone()),
            };
            return Err(self.fail_task(task_id, phase, &reason, err));
        }
        let status = status.expect("the loop only ends without a stop once the agent has exited");

        // The agent's own outcome comes first: its exit status and stderr
        // are what happened. A failure to deliver the context is reported
        // alongside, never instead.
        let stdin_error = fed.and_then(Result::err).map(|e| {
            format!("{program}: could not write the whole task context to its stdin: {e}")
        });
        if !status.success() {
            let stderr = String::from_utf8_lossy(&errout);
            let mut reason = format!("{program} failed ({status}): {stderr}");
            if let Some(note) = &stdin_error {
                reason.push_str(&format!("\n(also: {note})"));
            }
            let err = AgentError::InvocationFailed(reason.clone());
            return Err(self.fail_task(task_id, "exit", &reason, err));
        }
        let result = String::from_utf8_lossy(&out).into_owned();

        // The agent finished; storing its result is a separate step, and a
        // storage failure must not read as "still running" either. The task
        // fails with a reason that says the output existed.
        match self.store_result(task_id, &result, agent_name) {
            Ok(cid) => {
                // The agent succeeded, so the task is `done`; but it may
                // have worked from a partial context, and that is kept on
                // the row rather than dropped.
                if let Some(note) = stdin_error {
                    eprintln!("warning: {note}");
                    let meta = serde_json::json!({
                        "warning": { "phase": "stdin", "reason": bounded(&note, MAX_REASON_BYTES) }
                    });
                    if let Err(e) = self.db.set_task_meta(task_id, &meta.to_string()) {
                        eprintln!("warning: could not record that on the task: {e}");
                    }
                }
                Ok(cid)
            }
            Err(e) => {
                let reason = format!("agent exited 0 but its result could not be stored: {e}");
                Err(self.fail_task(task_id, "store", &reason, e))
            }
        }
    }

    /// Move a task to `failed`, keep why on its row (`tasks.meta`), and hand
    /// back the error that caused it. The stored reason is bounded (an
    /// agent's stderr can be arbitrarily long, and the SQL carrying it is a
    /// single argv element), and a failure to record it never replaces the
    /// agent's own error: the caller always gets `err`, extended with the
    /// recording failure only when there was one.
    fn fail_task(&self, task_id: &str, phase: &str, reason: &str, err: AgentError) -> AgentError {
        let recorded = self
            .db
            .update_task_status(task_id, TaskStatus::Failed.as_str())
            .and_then(|()| {
                let meta = serde_json::json!({
                    "failed": {
                        "phase": phase,
                        "reason": bounded(reason, MAX_REASON_BYTES),
                        "at": chrono::Utc::now().to_rfc3339(),
                    }
                });
                self.db.set_task_meta(task_id, &meta.to_string())
            });
        match recorded {
            Ok(()) => err,
            Err(e) => AgentError::InvocationFailed(format!(
                "{err} (and the task could not be marked failed: {})",
                bounded(&e.to_string(), MAX_REASON_BYTES)
            )),
        }
    }

    /// Seal an agent's output as a Reasoning node, project it, and close the
    /// task with the node it produced.
    fn store_result(
        &self,
        task_id: &str,
        result: &str,
        agent_name: &str,
    ) -> Result<Cid, AgentError> {
        // Store result as DAG node. If the task was created against a context
        // blob that is itself a node, parent the result to it so the result
        // has lineage back to what it was reasoning over. (Tasks live in Dolt,
        // not the DAG, so a task with no context node yields a root result.)
        let dag = Dag::new(self.cas);
        let parents: Vec<Cid> = match self.db.task_context_cid(task_id)? {
            Some(ctx) => {
                let cid = Cid::from(ctx.as_str());
                if dag.get_node(&cid).is_ok() {
                    vec![cid]
                } else {
                    vec![]
                }
            }
            None => vec![],
        };
        let (node_cid, _) = dag.store_with_node(
            result.as_bytes(),
            NodeKind::Reasoning,
            parents.clone(),
            agent_name,
        )?;

        // Sync to SQL in single transaction, projecting the parent edge (if
        // any) from the sealed node so verify-projection stays clean.
        let node = dag.get_node(&node_cid)?;
        let parent_refs: Vec<(&str, i32, &str)> = node
            .parent_links()
            .enumerate()
            .map(|(i, (cid, k))| (cid.as_str(), i as i32, k.as_str()))
            .collect();
        self.db.sync_dag_node(
            node_cid.as_str(),
            &node.kind.to_string(),
            &node.agent,
            &node.timestamp,
            node.output_cid.as_str(),
            "",
            &parent_refs,
            node.schema_cid.as_ref().map(|c| c.as_str()),
        )?;

        // Record the result on the task and mark it done, so the task row
        // links to the node the run produced (`result_cid` was never set).
        self.db.set_task_result(task_id, node_cid.as_str())?;
        self.db
            .update_task_status(task_id, TaskStatus::Done.as_str())?;

        Ok(node_cid)
    }

    /// Store reasoning output as a DAG node (for manual/external agent results).
    pub fn store_reasoning(
        &self,
        content: &str,
        agent: &str,
        parents: Vec<Cid>,
    ) -> Result<Cid, AgentError> {
        let dag = Dag::new(self.cas);
        let (node_cid, _) = dag.store_with_node(
            content.as_bytes(),
            NodeKind::Reasoning,
            parents.clone(),
            agent,
        )?;

        let node = dag.get_node(&node_cid)?;
        let parent_refs: Vec<(&str, i32, &str)> = parents
            .iter()
            .enumerate()
            .map(|(i, p)| (p.as_str(), i as i32, "derives"))
            .collect();
        self.db.sync_dag_node(
            node_cid.as_str(),
            &node.kind.to_string(),
            &node.agent,
            &node.timestamp,
            node.output_cid.as_str(),
            "",
            &parent_refs,
            node.schema_cid.as_ref().map(|c| c.as_str()),
        )?;

        Ok(node_cid)
    }

    /// Get reasoning context — retrieve prior reasoning nodes for injection into prompts.
    pub fn get_reasoning_context(&self, node_cid: &Cid) -> Result<Vec<(Cid, DagNode)>, AgentError> {
        let dag = Dag::new(self.cas);
        Ok(dag.lineage(node_cid)?)
    }

    /// List all tasks.
    pub fn list_tasks(&self) -> Result<String, AgentError> {
        Ok(self.db.list_tasks()?)
    }

    /// List all agents.
    pub fn list_agents(&self) -> Result<String, AgentError> {
        Ok(self.db.list_agents()?)
    }
}

#[cfg(test)]
mod tests {
    use super::bounded;

    #[test]
    fn bounded_keeps_short_text_and_both_ends_of_long_text() {
        assert_eq!(bounded("short", 16), "short");
        let long = format!("HEAD{}TAIL", "x".repeat(1000));
        let b = bounded(&long, 64);
        assert!(b.starts_with("HEAD") && b.ends_with("TAIL"), "{b}");
        assert!(b.len() < 128, "{}", b.len());
        // never splits a multi-byte char
        let wide = "é".repeat(100);
        let b = bounded(&wide, 33);
        assert!(b.contains("elided"));
    }
}
