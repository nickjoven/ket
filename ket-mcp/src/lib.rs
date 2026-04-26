//! MCP (Model Context Protocol) server for Ket.
//!
//! Exposes Ket operations as MCP tools over stdio JSON-RPC.
//! Tools: ket_put, ket_get, ket_verify, ket_dag_link, ket_dag_lineage,
//!        ket_check_drift, ket_query_cdom, ket_store_reasoning,
//!        ket_create_subtask, ket_get_reasoning, ket_score,
//!        ket_schema_stats, ket_dag_ls, ket_status, ket_search,
//!        decay_status.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum McpError {
    #[error("Unknown tool: {0}")]
    UnknownTool(String),
    #[error("Invalid params: {0}")]
    InvalidParams(String),
    #[error("CAS error: {0}")]
    Cas(#[from] ket_cas::CasError),
    #[error("DAG error: {0}")]
    Dag(#[from] ket_dag::DagError),
    #[error("SQL error: {0}")]
    Sql(#[from] ket_sql::SqlError),
    #[error("Agent error: {0}")]
    Agent(#[from] ket_agent::AgentError),
    #[error("Score error: {0}")]
    Score(#[from] ket_score::ScoreError),
    #[error("CDOM error: {0}")]
    Cdom(#[from] ket_cdom::CdomError),
    #[error("Opt error: {0}")]
    Opt(#[from] ket_opt::OptError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// JSON-RPC request.
#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

/// JSON-RPC response.
#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
}

/// MCP tool descriptor.
#[derive(Debug, Serialize)]
pub struct ToolDescriptor {
    pub name: String,
    pub description: String,
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
}

/// Get the list of tools this MCP server exposes.
pub fn tool_descriptors() -> Vec<ToolDescriptor> {
    vec![
        ToolDescriptor {
            name: "ket_put".into(),
            description: "Store content in the content-addressed store. Returns a CID (content identifier) — the BLAKE3 hash of the content. Identical content always produces the same CID, enabling automatic deduplication. Use this to store raw artifacts, then ket_dag_link to create a provenance node pointing to the content.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "content": { "type": "string", "description": "Content to store" }
                },
                "required": ["content"]
            }),
        },
        ToolDescriptor {
            name: "ket_get".into(),
            description: "Retrieve stored content by its CID. Returns the raw content bytes. Use after ket_dag_lineage or ket_dag_ls to inspect what a node's output_cid points to.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "cid": { "type": "string", "description": "Content identifier (BLAKE3 hash)" }
                },
                "required": ["cid"]
            }),
        },
        ToolDescriptor {
            name: "ket_verify".into(),
            description: "Verify that a CID's content hasn't been corrupted. Re-hashes the stored content and compares to the CID. Returns true if integrity holds.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "cid": { "type": "string", "description": "Content identifier to verify" }
                },
                "required": ["cid"]
            }),
        },
        ToolDescriptor {
            name: "ket_dag_link".into(),
            description: "Create a new DAG node with content and provenance. This is the primary way to record work — every node captures what was produced (content), what it derived from (parents), who produced it (agent), and what kind of artifact it is (kind). Always link parents to maintain provenance chains. Optional: declare epistemic confidence via `saturation` (0.0 = open query, 1.0 = settled claim) and time-decay via `activation`, `half_life_secs`, `activation_floor`.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "content": { "type": "string", "description": "Content for the new node" },
                    "kind": { "type": "string", "description": "Node kind: memory, code, reasoning, task, cdom, score, context" },
                    "parents": { "type": "array", "items": { "type": "string" }, "description": "Parent CIDs" },
                    "agent": { "type": "string", "description": "Agent name" },
                    "schema_cid": { "type": "string", "description": "Schema CID that the output conforms to" },
                    "saturation": { "type": "number", "minimum": 0.0, "maximum": 1.0, "description": "Epistemic confidence on [0.0, 1.0]. 0.0 = open query (maximally uncertain, prioritised for exploration); 1.0 = settled claim (optimizer treats subtree as exhausted). Omit to leave unset. Values outside the unit interval are rejected." },
                    "activation": { "type": "number", "minimum": 0.0, "description": "Initial activation value for time-decay (default 1.0). Must be finite and >= 0. Requires half_life_secs." },
                    "half_life_secs": { "type": "number", "exclusiveMinimum": 0.0, "description": "Exponential decay half-life in seconds. Must be finite and > 0. Omit the param entirely for no decay. Read back with decay_status tool." },
                    "activation_floor": { "type": "number", "minimum": 0.0, "description": "Minimum activation after decay (default 0.0). Must be finite, >= 0, and <= activation." }
                },
                "required": ["content", "kind", "agent"]
            }),
        },
        ToolDescriptor {
            name: "ket_dag_lineage".into(),
            description: "Trace a node's full ancestry by walking parent links up the DAG. Use this to understand how a piece of knowledge was derived — what reasoning, code, or memory it builds on.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "cid": { "type": "string", "description": "Node CID to trace from" }
                },
                "required": ["cid"]
            }),
        },
        ToolDescriptor {
            name: "ket_check_drift".into(),
            description: "Compare a file's current BLAKE3 hash to a previously stored CID. Returns true if the file has changed since the CID was recorded. Use this to detect when source material has changed and reasoning may be stale.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "File path to check" },
                    "expected_cid": { "type": "string", "description": "Expected CID" }
                },
                "required": ["path", "expected_cid"]
            }),
        },
        ToolDescriptor {
            name: "ket_query_cdom".into(),
            description: "Search code symbols (functions, structs, classes) extracted via tree-sitter parsing. Requires a file path to scan. Returns symbol names, kinds, and line ranges.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Symbol name to search for" },
                    "path": { "type": "string", "description": "File path to scan" }
                },
                "required": ["query", "path"]
            }),
        },
        ToolDescriptor {
            name: "ket_store_reasoning".into(),
            description: "Persist a reasoning step as a DAG node with kind=reasoning. Shorthand for ket_dag_link with kind pre-set. Use this to record conclusions, plans, or analysis so future sessions can retrieve context via ket_get_reasoning. Optional: declare epistemic confidence via `saturation` and time-decay via `activation` + `half_life_secs`.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "content": { "type": "string", "description": "Reasoning content" },
                    "agent": { "type": "string", "description": "Agent name" },
                    "parents": { "type": "array", "items": { "type": "string" }, "description": "Parent CIDs" },
                    "schema_cid": { "type": "string", "description": "Schema CID that the output conforms to" },
                    "saturation": { "type": "number", "minimum": 0.0, "maximum": 1.0, "description": "Epistemic confidence on [0.0, 1.0]. 0.0 = open question; 1.0 = settled conclusion. Omit to leave unset. Values outside the unit interval are rejected." },
                    "activation": { "type": "number", "minimum": 0.0, "description": "Initial activation value for time-decay (default 1.0). Must be finite and >= 0. Requires half_life_secs." },
                    "half_life_secs": { "type": "number", "exclusiveMinimum": 0.0, "description": "Exponential decay half-life in seconds. Must be finite and > 0. Omit the param entirely for no decay. Read back with decay_status tool." },
                    "activation_floor": { "type": "number", "minimum": 0.0, "description": "Minimum activation after decay (default 0.0). Must be finite, >= 0, and <= activation." }
                },
                "required": ["content", "agent"]
            }),
        },
        ToolDescriptor {
            name: "ket_create_subtask".into(),
            description: "Create a task record for delegating work to another agent. Requires Dolt for persistence. Tasks have lifecycle states (pending, assigned, completed) and can be nested via parent_task.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "title": { "type": "string", "description": "Task title" },
                    "created_by": { "type": "string", "description": "Creator agent" },
                    "parent_task": { "type": "string", "description": "Parent task ID" }
                },
                "required": ["title", "created_by"]
            }),
        },
        ToolDescriptor {
            name: "ket_get_reasoning".into(),
            description: "Retrieve a reasoning node's content by CID. Automatically unwraps the DAG node to return the reasoning text, agent, timestamp, and schema. Use this to inject prior reasoning into prompts.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "cid": { "type": "string", "description": "Reasoning node CID" }
                },
                "required": ["cid"]
            }),
        },
        ToolDescriptor {
            name: "ket_calibrate".into(),
            description: "Run weighted-quality-score optimization on a DAG subtree to allocate compute tiers (free/moderate/expensive) across nodes. Requires Dolt. Advanced — use after building a substantial DAG.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "root_cid": { "type": "string", "description": "Root CID of the subtree to calibrate" },
                    "max_cost": { "type": "number", "description": "Maximum total compute cost (default: 50)" },
                    "max_depth": { "type": "integer", "description": "Maximum depth to explore (default: 20)" },
                    "max_tier3": { "type": "integer", "description": "Maximum Tier 3 calls (default: 5)" }
                },
                "required": ["root_cid"]
            }),
        },
        ToolDescriptor {
            name: "ket_score".into(),
            description: "Record a quality score for a node across four dimensions: correctness, efficiency, style, completeness. Values are 0.0-1.0. Scores accumulate per-agent and per-node, enabling routing decisions (which agent is best at what). Requires Dolt.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "node_cid": { "type": "string", "description": "Node CID to score" },
                    "agent": { "type": "string", "description": "Agent that produced the output" },
                    "scorer": { "type": "string", "description": "Who is scoring" },
                    "dimension": { "type": "string", "description": "Dimension: correctness, efficiency, style, completeness" },
                    "value": { "type": "number", "description": "Score 0.0-1.0" },
                    "evidence": { "type": "string", "description": "Evidence for the score" }
                },
                "required": ["node_cid", "agent", "scorer", "dimension", "value"]
            }),
        },
        ToolDescriptor {
            name: "ket_schema_stats".into(),
            description: "Check whether a schema is producing effective content deduplication. Returns total nodes tagged with the schema vs. unique output CIDs. If total >> unique, the schema is working — identical observations hash identically.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "schema_cid": { "type": "string", "description": "Schema CID to check stats for" }
                },
                "required": ["schema_cid"]
            }),
        },
        ToolDescriptor {
            name: "ket_dag_ls".into(),
            description: "List DAG nodes to discover what's in the substrate. Use this to find nodes by kind (memory, code, reasoning, task) or to see recent activity. Returns summary metadata — use ket_get to retrieve full content.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "kind": { "type": "string", "description": "Filter by node kind: memory, code, reasoning, task, cdom, score, context" },
                    "limit": { "type": "integer", "description": "Maximum number of results (default 50)" }
                }
            }),
        },
        ToolDescriptor {
            name: "ket_status".into(),
            description: "Check substrate health and get counts of stored objects. Use this at the start of a session to understand what's available, or after mutations to verify state.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDescriptor {
            name: "ket_search".into(),
            description: "Search stored content by text. Scans all CAS blobs for matching text. Use this when you need to find content but don't have its CID — for example, finding prior reasoning about a specific topic.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Text to search for (case-insensitive)" },
                    "limit": { "type": "integer", "description": "Maximum number of results (default 20)" }
                },
                "required": ["query"]
            }),
        },
        ToolDescriptor {
            name: "decay_status".into(),
            description: "Get the current decay-adjusted activation for a node. Reports the stored activation, decay configuration (half-life, floor), and the computed activation at the given elapsed time. Decay is applied on query — the stored value is never mutated.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "cid": { "type": "string", "description": "Node CID to inspect" },
                    "elapsed_secs": { "type": "number", "description": "Seconds elapsed since the node was written (default 0)" }
                },
                "required": ["cid"]
            }),
        },
        ToolDescriptor {
            name: "ket_soft_link".into(),
            description: "Create a typed semantic edge between two DAG nodes. Use this to map ontological relationships between claims, theorems, observations, and models: 'supports', 'contradicts', 'formalizes', 'extends', 'cites', 'instantiates', 'refines', or any domain-specific label. Unlike DAG parent edges (which record derivation provenance), soft links record semantic relationships that cross the provenance graph and can be traversed by type. Requires Dolt.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "from_cid": { "type": "string", "description": "Source node CID" },
                    "to_cid": { "type": "string", "description": "Target node CID" },
                    "relation": { "type": "string", "description": "Relation type, e.g. supports, contradicts, formalizes, extends, cites, instantiates, refines" }
                },
                "required": ["from_cid", "to_cid", "relation"]
            }),
        },
        ToolDescriptor {
            name: "ket_soft_link_query".into(),
            description: "Query typed semantic edges for a DAG node. Returns all soft links involving the given CID. Use 'direction' to filter outgoing ('from'), incoming ('to'), or both (default). Use 'relation' to filter by edge type. Use this to traverse the ontology graph: find what a claim supports, what contradicts it, what formalizes a concept. Requires Dolt.".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "cid": { "type": "string", "description": "Node CID to query edges for" },
                    "relation": { "type": "string", "description": "Optional: filter by relation type (e.g. contradicts)" },
                    "direction": { "type": "string", "description": "Optional: 'from' (outgoing), 'to' (incoming), or 'both' (default)" }
                },
                "required": ["cid"]
            }),
        },
    ]
}

/// Parse and validate the optional `saturation` param.
///
/// Saturation encodes epistemic confidence on [0.0, 1.0]. Values outside
/// the unit interval, or non-finite values, are rejected — they would
/// silently break the optimizer's claim/query semantics.
fn parse_saturation_param(params: &Value) -> Result<Option<f32>, McpError> {
    match params.get("saturation") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => {
            let n = v.as_f64().ok_or_else(|| {
                McpError::InvalidParams("saturation must be a number".into())
            })?;
            if !n.is_finite() || !(0.0..=1.0).contains(&n) {
                return Err(McpError::InvalidParams(
                    "saturation must be finite and in [0.0, 1.0]".into(),
                ));
            }
            Ok(Some(n as f32))
        }
    }
}

/// Parse and validate the optional decay params.
///
/// Returns `Some((activation, config))` if `half_life_secs` is present
/// (decay requested), otherwise `None`. `activation` and `activation_floor`
/// default to 1.0 and 0.0 respectively when omitted but `half_life_secs`
/// is set. All values must be finite and non-negative; `half_life_secs`
/// must additionally be strictly positive (omit the param for "no decay"
/// rather than passing 0 or infinity).
fn parse_decay_params(params: &Value) -> Result<Option<(f64, ket_dag::DecayConfig)>, McpError> {
    fn finite_f64(params: &Value, key: &str) -> Result<Option<f64>, McpError> {
        match params.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => {
                let n = v.as_f64().ok_or_else(|| {
                    McpError::InvalidParams(format!("{} must be a number", key))
                })?;
                if !n.is_finite() {
                    return Err(McpError::InvalidParams(format!(
                        "{} must be finite",
                        key
                    )));
                }
                Ok(Some(n))
            }
        }
    }

    let half_life = match finite_f64(params, "half_life_secs")? {
        Some(hl) => hl,
        None => return Ok(None),
    };
    if half_life <= 0.0 {
        return Err(McpError::InvalidParams(
            "half_life_secs must be > 0 (omit the param for no decay)".into(),
        ));
    }

    let activation = finite_f64(params, "activation")?.unwrap_or(1.0);
    if activation < 0.0 {
        return Err(McpError::InvalidParams(
            "activation must be >= 0".into(),
        ));
    }

    let floor = finite_f64(params, "activation_floor")?.unwrap_or(0.0);
    if floor < 0.0 {
        return Err(McpError::InvalidParams(
            "activation_floor must be >= 0".into(),
        ));
    }
    if floor > activation {
        return Err(McpError::InvalidParams(
            "activation_floor must be <= activation".into(),
        ));
    }

    Ok(Some((
        activation,
        ket_dag::DecayConfig {
            half_life_secs: half_life,
            activation_floor: floor,
        },
    )))
}

/// Handle an MCP tool call.
pub fn handle_tool_call(
    tool_name: &str,
    params: &Value,
    cas: &ket_cas::Store,
    db: Option<&ket_sql::DoltDb>,
) -> Result<Value, McpError> {
    match tool_name {
        "ket_put" => {
            let content = params["content"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("content required".into()))?;
            let cid = cas.put(content.as_bytes())?;
            Ok(serde_json::json!({ "cid": cid.as_str() }))
        }
        "ket_get" => {
            let cid_str = params["cid"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("cid required".into()))?;
            let cid = ket_cas::Cid::from(cid_str);
            let data = cas.get(&cid)?;
            let content = String::from_utf8_lossy(&data).into_owned();
            Ok(serde_json::json!({ "content": content }))
        }
        "ket_verify" => {
            let cid_str = params["cid"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("cid required".into()))?;
            let cid = ket_cas::Cid::from(cid_str);
            let valid = cas.verify(&cid)?;
            Ok(serde_json::json!({ "valid": valid }))
        }
        "ket_dag_link" => {
            let content = params["content"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("content required".into()))?;
            let kind_str = params["kind"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("kind required".into()))?;
            let agent = params["agent"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("agent required".into()))?;
            let parents: Vec<ket_cas::Cid> = params
                .get("parents")
                .and_then(|p| p.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(ket_cas::Cid::from))
                        .collect()
                })
                .unwrap_or_default();
            let schema_cid_param = params.get("schema_cid").and_then(|v| v.as_str());

            let kind = parse_node_kind(kind_str)?;
            let saturation_param = parse_saturation_param(params)?;
            let decay_param = parse_decay_params(params)?;

            let dag = ket_dag::Dag::new(cas);
            let content_cid = cas.put(content.as_bytes())?;
            let mut node = ket_dag::DagNode::new(
                kind,
                parents.clone(),
                content_cid.clone(),
                agent,
            );
            if let Some(s) = schema_cid_param {
                node = node.with_schema(ket_cas::Cid::from(s));
            }
            if let Some(sat) = saturation_param {
                node = node.with_saturation(sat);
            }
            if let Some((activation, config)) = decay_param {
                node = node.with_decay(activation, config);
            }
            let node_cid = dag.put_node(&node)?;

            // Sync to SQL if Dolt is available
            if let Some(db) = db {
                let parent_refs: Vec<(&str, i32)> = parents
                    .iter()
                    .enumerate()
                    .map(|(i, p)| (p.as_str(), i as i32))
                    .collect();
                let _ = db.sync_dag_node(
                    node_cid.as_str(),
                    kind_str,
                    agent,
                    &node.timestamp,
                    content_cid.as_str(),
                    "",
                    &parent_refs,
                    node.schema_cid.as_ref().map(|c| c.as_str()),
                );
            }

            Ok(serde_json::json!({
                "node_cid": node_cid.as_str(),
                "content_cid": content_cid.as_str()
            }))
        }
        "ket_dag_lineage" => {
            let cid_str = params["cid"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("cid required".into()))?;
            let dag = ket_dag::Dag::new(cas);
            let lineage = dag.lineage(&ket_cas::Cid::from(cid_str))?;
            let nodes: Vec<Value> = lineage
                .iter()
                .map(|(cid, node)| {
                    serde_json::json!({
                        "cid": cid.as_str(),
                        "kind": node.kind.to_string(),
                        "agent": node.agent,
                        "timestamp": node.timestamp,
                        "output_cid": node.output_cid.as_str(),
                        "parents": node.parents.iter().map(|p| p.as_str()).collect::<Vec<_>>(),
                    })
                })
                .collect();
            Ok(serde_json::json!({ "lineage": nodes }))
        }
        "ket_check_drift" => {
            let path = params["path"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("path required".into()))?;
            let expected = params["expected_cid"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("expected_cid required".into()))?;
            let dag = ket_dag::Dag::new(cas);
            let drifted =
                dag.check_drift(std::path::Path::new(path), &ket_cas::Cid::from(expected))?;
            Ok(serde_json::json!({ "drifted": drifted }))
        }
        "ket_query_cdom" => {
            let query = params["query"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("query required".into()))?;
            let path = params["path"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("path required".into()))?;
            let symbols = ket_cdom::parse_file(std::path::Path::new(path))?;
            let matches = ket_cdom::query_symbols(&symbols, query);
            let results: Vec<Value> = matches
                .iter()
                .map(|s| {
                    serde_json::json!({
                        "name": s.name,
                        "kind": s.kind.to_string(),
                        "start_line": s.start_line,
                        "end_line": s.end_line,
                    })
                })
                .collect();
            Ok(serde_json::json!({ "symbols": results }))
        }
        "ket_store_reasoning" => {
            let content = params["content"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("content required".into()))?;
            let agent = params["agent"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("agent required".into()))?;
            let parents: Vec<ket_cas::Cid> = params
                .get("parents")
                .and_then(|p| p.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(ket_cas::Cid::from))
                        .collect()
                })
                .unwrap_or_default();
            let schema_cid_param = params.get("schema_cid").and_then(|v| v.as_str());
            let saturation_param = parse_saturation_param(params)?;
            let decay_param = parse_decay_params(params)?;

            let dag = ket_dag::Dag::new(cas);
            let content_cid = cas.put(content.as_bytes())?;
            let mut node = ket_dag::DagNode::new(
                ket_dag::NodeKind::Reasoning,
                parents.clone(),
                content_cid.clone(),
                agent,
            );
            if let Some(s) = schema_cid_param {
                node = node.with_schema(ket_cas::Cid::from(s));
            }
            if let Some(sat) = saturation_param {
                node = node.with_saturation(sat);
            }
            if let Some((activation, config)) = decay_param {
                node = node.with_decay(activation, config);
            }
            let node_cid = dag.put_node(&node)?;

            // Sync to SQL if Dolt is available
            if let Some(db) = db {
                let parent_refs: Vec<(&str, i32)> = parents
                    .iter()
                    .enumerate()
                    .map(|(i, p)| (p.as_str(), i as i32))
                    .collect();
                let _ = db.sync_dag_node(
                    node_cid.as_str(),
                    "reasoning",
                    agent,
                    &node.timestamp,
                    content_cid.as_str(),
                    "",
                    &parent_refs,
                    node.schema_cid.as_ref().map(|c| c.as_str()),
                );
            }

            Ok(serde_json::json!({ "node_cid": node_cid.as_str() }))
        }
        "ket_create_subtask" => {
            let db = db.ok_or_else(|| McpError::InvalidParams(
                "ket_create_subtask requires Dolt (see ket README)".into(),
            ))?;
            let title = params["title"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("title required".into()))?;
            let created_by = params["created_by"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("created_by required".into()))?;
            let parent_task = params.get("parent_task").and_then(|v| v.as_str());

            let orch = ket_agent::Orchestrator::new(cas, db);
            let task_id = orch.create_task(title, created_by, parent_task, None)?;
            Ok(serde_json::json!({ "task_id": task_id }))
        }
        "ket_get_reasoning" => {
            let cid_str = params["cid"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("cid required".into()))?;
            let cid = ket_cas::Cid::from(cid_str);
            let data = cas.get(&cid)?;
            // Try to parse as a DAG node, otherwise return raw content
            match ket_dag::DagNode::from_bytes(&data) {
                Ok(node) => {
                    let content_data = cas.get(&node.output_cid)?;
                    let content = String::from_utf8_lossy(&content_data).into_owned();
                    Ok(serde_json::json!({
                        "agent": node.agent,
                        "kind": node.kind.to_string(),
                        "timestamp": node.timestamp,
                        "content": content,
                        "schema_cid": node.schema_cid.as_ref().map(|c| c.as_str()).unwrap_or(""),
                    }))
                }
                Err(_) => {
                    let content = String::from_utf8_lossy(&data).into_owned();
                    Ok(serde_json::json!({ "content": content }))
                }
            }
        }
        "ket_calibrate" => {
            let db = db.ok_or_else(|| McpError::InvalidParams(
                "ket_calibrate requires Dolt (see ket README)".into(),
            ))?;
            let root_cid = params["root_cid"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("root_cid required".into()))?;
            let max_cost = params.get("max_cost").and_then(|v| v.as_f64()).unwrap_or(50.0);
            let max_depth = params.get("max_depth").and_then(|v| v.as_u64()).unwrap_or(20) as u32;
            let max_tier3 = params.get("max_tier3").and_then(|v| v.as_u64()).unwrap_or(5) as u32;

            let dag = ket_dag::Dag::new(cas);
            let constraints = ket_opt::Constraints {
                max_cost,
                max_depth,
                max_tier3_calls: max_tier3,
            };
            let cid = ket_cas::Cid::from(root_cid);
            let (node_cid, result) =
                ket_opt::calibrate(cas, &dag, db, &cid, &constraints, "mcp")?;

            Ok(serde_json::json!({
                "node_cid": node_cid.as_str(),
                "result": result,
            }))
        }
        "ket_score" => {
            let db = db.ok_or_else(|| McpError::InvalidParams(
                "ket_score requires Dolt (see ket README)".into(),
            ))?;
            let node_cid = params["node_cid"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("node_cid required".into()))?;
            let agent = params["agent"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("agent required".into()))?;
            let scorer = params["scorer"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("scorer required".into()))?;
            let dimension = params["dimension"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("dimension required".into()))?;
            let value = params["value"]
                .as_f64()
                .ok_or_else(|| McpError::InvalidParams("value required".into()))?;
            let evidence = params
                .get("evidence")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            let dim = ket_score::Dimension::parse(dimension)?;
            let score = ket_score::Score::new(
                ket_cas::Cid::from(node_cid),
                agent,
                scorer,
                dim,
                value,
                evidence,
            )?;

            let engine = ket_score::ScoringEngine::new(db);
            engine.record(&score)?;
            Ok(serde_json::json!({ "recorded": true }))
        }
        "ket_schema_stats" => {
            let schema_cid = params["schema_cid"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("schema_cid required".into()))?;
            let dag = ket_dag::Dag::new(cas);
            let (total, unique) = dag.schema_stats(&ket_cas::Cid::from(schema_cid))?;
            let dedup_ratio = if unique > 0 {
                format!("{:.2}", total as f64 / unique as f64)
            } else {
                "N/A".to_string()
            };
            Ok(serde_json::json!({
                "total_nodes": total,
                "unique_outputs": unique,
                "dedup_ratio": dedup_ratio,
            }))
        }
        "ket_dag_ls" => {
            let kind_filter = params.get("kind").and_then(|v| v.as_str());
            let limit = params
                .get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(50) as usize;

            let dag = ket_dag::Dag::new(cas);
            let cids = cas.list()?;
            let mut nodes = Vec::new();

            for cid in &cids {
                if nodes.len() >= limit {
                    break;
                }
                if let Ok(node) = dag.get_node(cid) {
                    if let Some(filter) = kind_filter {
                        if node.kind.to_string() != filter {
                            continue;
                        }
                    }
                    let mut obj = serde_json::json!({
                        "cid": cid.as_str(),
                        "kind": node.kind.to_string(),
                        "agent": node.agent,
                        "timestamp": node.timestamp,
                        "output_cid": node.output_cid.as_str(),
                    });
                    if let Some(ref s) = node.schema_cid {
                        obj["schema_cid"] = serde_json::json!(s.as_str());
                    }
                    nodes.push(obj);
                }
            }

            Ok(serde_json::json!({ "nodes": nodes }))
        }
        "ket_status" => {
            let cas_blobs = cas.list()?.len();

            // Count valid DAG nodes
            let dag = ket_dag::Dag::new(cas);
            let cids = cas.list()?;
            let dag_nodes = cids.iter().filter(|c| dag.get_node(c).is_ok()).count();

            let has_dolt = db.is_some();
            let dolt_stats = if let Some(db) = db {
                db.stats().ok().map(|s| serde_json::json!(s))
            } else {
                None
            };

            Ok(serde_json::json!({
                "cas_blobs": cas_blobs,
                "dag_nodes": dag_nodes,
                "has_dolt": has_dolt,
                "dolt_stats": dolt_stats,
            }))
        }
        "ket_search" => {
            let query = params["query"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("query required".into()))?;
            let limit = params
                .get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(20) as usize;

            let cids = cas.list()?;
            let query_lower = query.to_lowercase();
            let mut results = Vec::new();

            for cid in &cids {
                if results.len() >= limit {
                    break;
                }
                if let Ok(data) = cas.get(cid) {
                    if let Ok(text) = std::str::from_utf8(&data) {
                        if text.to_lowercase().contains(&query_lower) {
                            let snippet = if text.len() > 200 {
                                format!("{}...", &text[..200])
                            } else {
                                text.to_string()
                            };
                            results.push(serde_json::json!({
                                "cid": cid.as_str(),
                                "snippet": snippet,
                            }));
                        }
                    }
                }
            }

            Ok(serde_json::json!({ "results": results }))
        }
        "decay_status" => {
            let cid_str = params["cid"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("cid required".into()))?;
            let elapsed_secs = params.get("elapsed_secs").and_then(|v| v.as_f64()).unwrap_or(0.0);

            let dag = ket_dag::Dag::new(cas);
            let node = dag.get_node(&ket_cas::Cid::from(cid_str))?;
            let stored_activation = node.activation.unwrap_or(1.0);
            let decayed = node.decayed_activation(elapsed_secs);

            Ok(serde_json::json!({
                "cid": cid_str,
                "stored_activation": stored_activation,
                "decay_adjusted_activation": decayed,
                "elapsed_secs": elapsed_secs,
                "half_life_secs": node.decay_config.as_ref().map(|c| c.half_life_secs),
                "activation_floor": node.decay_config.as_ref().map(|c| c.activation_floor),
                "has_decay": node.decay_config.is_some(),
            }))
        }
        "ket_soft_link" => {
            let db = db.ok_or_else(|| McpError::InvalidParams("ket_soft_link requires Dolt".into()))?;
            let from_cid = params["from_cid"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("from_cid required".into()))?;
            let to_cid = params["to_cid"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("to_cid required".into()))?;
            let relation = params["relation"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("relation required".into()))?;
            db.insert_soft_link(from_cid, to_cid, relation)?;
            Ok(serde_json::json!({
                "created": true,
                "from_cid": from_cid,
                "to_cid": to_cid,
                "relation": relation,
            }))
        }
        "ket_soft_link_query" => {
            let db = db.ok_or_else(|| McpError::InvalidParams("ket_soft_link_query requires Dolt".into()))?;
            let cid = params["cid"]
                .as_str()
                .ok_or_else(|| McpError::InvalidParams("cid required".into()))?;
            let direction = params.get("direction").and_then(|v| v.as_str()).unwrap_or("both");
            let relation_filter = params.get("relation").and_then(|v| v.as_str());
            let csv = match direction {
                "from" => db.soft_links_from(cid)?,
                "to" => db.soft_links_to(cid)?,
                _ => db.soft_links_for(cid)?,
            };
            let links = parse_soft_links_csv(&csv, direction, relation_filter);
            Ok(serde_json::json!({ "links": links, "cid": cid, "direction": direction }))
        }
        _ => Err(McpError::UnknownTool(tool_name.to_string())),
    }
}

/// Handle a JSON-RPC request (MCP protocol).
pub fn handle_jsonrpc(
    request: &JsonRpcRequest,
    cas: &ket_cas::Store,
    db: Option<&ket_sql::DoltDb>,
) -> JsonRpcResponse {
    match request.method.as_str() {
        "initialize" => JsonRpcResponse {
            jsonrpc: "2.0".into(),
            id: request.id.clone(),
            result: Some(serde_json::json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {}
                },
                "serverInfo": {
                    "name": "ket",
                    "version": "0.1.0"
                }
            })),
            error: None,
        },
        "tools/list" => {
            let tools = tool_descriptors();
            JsonRpcResponse {
                jsonrpc: "2.0".into(),
                id: request.id.clone(),
                result: Some(serde_json::json!({ "tools": tools })),
                error: None,
            }
        }
        "tools/call" => {
            let tool_name = request.params["name"].as_str().unwrap_or("");
            let arguments = request.params.get("arguments").cloned().unwrap_or(Value::Object(Default::default()));

            match handle_tool_call(tool_name, &arguments, cas, db) {
                Ok(result) => JsonRpcResponse {
                    jsonrpc: "2.0".into(),
                    id: request.id.clone(),
                    result: Some(serde_json::json!({
                        "content": [{ "type": "text", "text": result.to_string() }]
                    })),
                    error: None,
                },
                Err(e) => JsonRpcResponse {
                    jsonrpc: "2.0".into(),
                    id: request.id.clone(),
                    result: None,
                    error: Some(JsonRpcError {
                        code: -32603,
                        message: e.to_string(),
                    }),
                },
            }
        }
        "notifications/initialized" => JsonRpcResponse {
            jsonrpc: "2.0".into(),
            id: request.id.clone(),
            result: Some(Value::Null),
            error: None,
        },
        _ => JsonRpcResponse {
            jsonrpc: "2.0".into(),
            id: request.id.clone(),
            result: None,
            error: Some(JsonRpcError {
                code: -32601,
                message: format!("Method not found: {}", request.method),
            }),
        },
    }
}

/// Parse soft_links CSV rows into JSON objects.
///
/// Column layout by direction:
/// - "both": from_cid, to_cid, relation, created_at
/// - "from": to_cid, relation, created_at
/// - "to":   from_cid, relation, created_at
fn parse_soft_links_csv(csv: &str, direction: &str, relation_filter: Option<&str>) -> Vec<serde_json::Value> {
    let mut lines = csv.lines();
    let _header = lines.next();
    let mut links = Vec::new();
    for line in lines {
        let cols: Vec<&str> = line.splitn(4, ',').collect();
        let (from, to, rel, ts) = match direction {
            "from" if cols.len() >= 3 => ("", cols[0], cols[1], cols.get(2).copied().unwrap_or("")),
            "to"   if cols.len() >= 3 => (cols[0], "", cols[1], cols.get(2).copied().unwrap_or("")),
            _      if cols.len() >= 4 => (cols[0], cols[1], cols[2], cols[3]),
            _ => continue,
        };
        if let Some(filter) = relation_filter {
            if rel.trim() != filter {
                continue;
            }
        }
        let mut obj = serde_json::json!({ "relation": rel.trim(), "created_at": ts.trim() });
        if !from.is_empty() {
            obj["from_cid"] = serde_json::json!(from.trim());
        }
        if !to.is_empty() {
            obj["to_cid"] = serde_json::json!(to.trim());
        }
        links.push(obj);
    }
    links
}

fn parse_node_kind(s: &str) -> Result<ket_dag::NodeKind, McpError> {
    match s {
        "memory" => Ok(ket_dag::NodeKind::Memory),
        "code" => Ok(ket_dag::NodeKind::Code),
        "reasoning" => Ok(ket_dag::NodeKind::Reasoning),
        "task" => Ok(ket_dag::NodeKind::Task),
        "cdom" => Ok(ket_dag::NodeKind::Cdom),
        "score" => Ok(ket_dag::NodeKind::Score),
        "context" => Ok(ket_dag::NodeKind::Context),
        _ => Err(McpError::InvalidParams(format!("Unknown node kind: {s}"))),
    }
}

/// Run the MCP server loop on stdio (synchronous, line-delimited JSON-RPC).
pub fn run_stdio_server(cas: &ket_cas::Store, db: Option<&ket_sql::DoltDb>) -> Result<(), McpError> {
    use std::io::{BufRead, BufReader, Write};

    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let reader = BufReader::new(stdin.lock());

    for line in reader.lines() {
        let line = line?;
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }

        let response = match serde_json::from_str::<JsonRpcRequest>(&line) {
            Ok(request) => handle_jsonrpc(&request, cas, db),
            Err(e) => JsonRpcResponse {
                jsonrpc: "2.0".into(),
                id: None,
                result: None,
                error: Some(JsonRpcError {
                    code: -32700,
                    message: format!("Parse error: {e}"),
                }),
            },
        };

        let response_json = serde_json::to_string(&response)?;
        writeln!(stdout, "{response_json}").map_err(McpError::Io)?;
        stdout.flush().map_err(McpError::Io)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn saturation_absent_is_none() {
        assert!(parse_saturation_param(&json!({})).unwrap().is_none());
        assert!(parse_saturation_param(&json!({ "saturation": null }))
            .unwrap()
            .is_none());
    }

    #[test]
    fn saturation_in_range_accepted() {
        assert_eq!(
            parse_saturation_param(&json!({ "saturation": 0.0 })).unwrap(),
            Some(0.0)
        );
        assert_eq!(
            parse_saturation_param(&json!({ "saturation": 1.0 })).unwrap(),
            Some(1.0)
        );
        assert_eq!(
            parse_saturation_param(&json!({ "saturation": 0.5 })).unwrap(),
            Some(0.5)
        );
    }

    #[test]
    fn saturation_out_of_range_rejected() {
        assert!(parse_saturation_param(&json!({ "saturation": -0.1 })).is_err());
        assert!(parse_saturation_param(&json!({ "saturation": 1.1 })).is_err());
        assert!(parse_saturation_param(&json!({ "saturation": 2.0 })).is_err());
    }

    #[test]
    fn saturation_non_numeric_rejected() {
        assert!(parse_saturation_param(&json!({ "saturation": "0.5" })).is_err());
        assert!(parse_saturation_param(&json!({ "saturation": true })).is_err());
    }

    #[test]
    fn decay_absent_is_none() {
        assert!(parse_decay_params(&json!({})).unwrap().is_none());
        assert!(parse_decay_params(&json!({ "activation": 1.0 }))
            .unwrap()
            .is_none());
    }

    #[test]
    fn decay_with_half_life_uses_defaults() {
        let (activation, config) = parse_decay_params(&json!({ "half_life_secs": 60.0 }))
            .unwrap()
            .unwrap();
        assert_eq!(activation, 1.0);
        assert_eq!(config.half_life_secs, 60.0);
        assert_eq!(config.activation_floor, 0.0);
    }

    #[test]
    fn decay_full_params_accepted() {
        let (activation, config) = parse_decay_params(&json!({
            "activation": 2.5,
            "half_life_secs": 120.0,
            "activation_floor": 0.1,
        }))
        .unwrap()
        .unwrap();
        assert_eq!(activation, 2.5);
        assert_eq!(config.half_life_secs, 120.0);
        assert_eq!(config.activation_floor, 0.1);
    }

    #[test]
    fn decay_non_positive_half_life_rejected() {
        assert!(parse_decay_params(&json!({ "half_life_secs": 0.0 })).is_err());
        assert!(parse_decay_params(&json!({ "half_life_secs": -1.0 })).is_err());
    }

    #[test]
    fn decay_non_numeric_rejected() {
        assert!(parse_decay_params(&json!({ "half_life_secs": "60" })).is_err());
        assert!(parse_decay_params(&json!({ "half_life_secs": true })).is_err());
        assert!(parse_decay_params(&json!({
            "half_life_secs": 60.0,
            "activation": "1.0",
        }))
        .is_err());
    }

    #[test]
    fn decay_negative_activation_rejected() {
        assert!(parse_decay_params(&json!({
            "half_life_secs": 60.0,
            "activation": -0.5,
        }))
        .is_err());
    }

    #[test]
    fn decay_floor_above_activation_rejected() {
        assert!(parse_decay_params(&json!({
            "half_life_secs": 60.0,
            "activation": 0.5,
            "activation_floor": 0.6,
        }))
        .is_err());
    }
}
