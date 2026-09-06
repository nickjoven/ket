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
    #[error("Max subtask depth ({0}) exceeded")]
    MaxDepth(u32),
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

    /// Run a task by spawning the assigned agent.
    pub async fn run_task(
        &self,
        task_id: &str,
        prompt: &str,
        agent_name: &str,
        context: Option<&str>,
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
        self.db
            .update_task_status(task_id, TaskStatus::Running.as_str())?;

        // Prefer the command `ket agent register` stored for this agent; the
        // prompt is appended as the final argument. Fall back to the built-in
        // presets, then error. (Simple whitespace splitting: agent commands
        // are flags, not shell.) This is the fix for `run` ignoring the
        // registered `cli_command`.
        let (program, args): (String, Vec<String>) =
            if let Some(cmd) = self.db.agent_command(agent_name)? {
                let mut parts = cmd.split_whitespace().map(str::to_string);
                let program = parts
                    .next()
                    .ok_or_else(|| AgentError::NotFound(agent_name.to_string()))?;
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
                    _ => return Err(AgentError::NotFound(agent_name.to_string())),
                }
            };

        // Build full prompt with context
        let full_prompt = if let Some(ctx) = context {
            format!("{ctx}\n\n---\n\n{prompt}")
        } else {
            prompt.to_string()
        };

        // Spawn the process
        let output = tokio::process::Command::new(&program)
            .args(&args[..args.len() - 1])
            .arg(&full_prompt)
            .output()
            .await
            .map_err(|e| AgentError::InvocationFailed(format!("{program}: {e}")))?;

        let result = if output.status.success() {
            String::from_utf8_lossy(&output.stdout).into_owned()
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            self.db
                .update_task_status(task_id, TaskStatus::Failed.as_str())?;
            return Err(AgentError::InvocationFailed(format!(
                "{program} failed: {stderr}"
            )));
        };

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
