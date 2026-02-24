//! MCP (Model Context Protocol) server for Ket.
//!
//! Exposes Ket operations as MCP tools over stdio JSON-RPC.
//! Tools: ket_put, ket_get, ket_verify, ket_dag_link, ket_dag_lineage,
//!        ket_check_drift, ket_query_cdom, ket_store_reasoning,
//!        ket_create_subtask, ket_get_reasoning, ket_score.

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
            description: "Store content in CAS, return CID".into(),
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
            description: "Retrieve content by CID".into(),
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
            description: "Verify CID integrity".into(),
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
            description: "Create DAG edge (parent -> child node)".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "content": { "type": "string", "description": "Content for the new node" },
                    "kind": { "type": "string", "description": "Node kind: memory, code, reasoning, task, cdom, score, context" },
                    "parents": { "type": "array", "items": { "type": "string" }, "description": "Parent CIDs" },
                    "agent": { "type": "string", "description": "Agent name" }
                },
                "required": ["content", "kind", "agent"]
            }),
        },
        ToolDescriptor {
            name: "ket_dag_lineage".into(),
            description: "Trace lineage of a node up the DAG".into(),
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
            description: "Compare file's current hash to stored CID".into(),
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
            description: "Search code symbols".into(),
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
            description: "Persist agent reasoning as DAG node".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "content": { "type": "string", "description": "Reasoning content" },
                    "agent": { "type": "string", "description": "Agent name" },
                    "parents": { "type": "array", "items": { "type": "string" }, "description": "Parent CIDs" }
                },
                "required": ["content", "agent"]
            }),
        },
        ToolDescriptor {
            name: "ket_create_subtask".into(),
            description: "Delegate work to another agent".into(),
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
            description: "Retrieve prior reasoning for context".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "cid": { "type": "string", "description": "Reasoning node CID" }
                },
                "required": ["cid"]
            }),
        },
        ToolDescriptor {
            name: "ket_score".into(),
            description: "Score an agent output".into(),
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
    ]
}

/// Handle an MCP tool call.
pub fn handle_tool_call(
    tool_name: &str,
    params: &Value,
    cas: &ket_cas::Store,
    db: &ket_sql::DoltDb,
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

            let kind = parse_node_kind(kind_str)?;
            let dag = ket_dag::Dag::new(cas);
            let (node_cid, content_cid) =
                dag.store_with_node(content.as_bytes(), kind, parents, agent)?;

            let node = dag.get_node(&node_cid)?;
            db.insert_dag_node(
                node_cid.as_str(),
                kind_str,
                agent,
                &node.timestamp,
                content_cid.as_str(),
                "",
            )?;

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

            let orch = ket_agent::Orchestrator::new(cas, db);
            let node_cid = orch.store_reasoning(content, agent, parents)?;
            Ok(serde_json::json!({ "node_cid": node_cid.as_str() }))
        }
        "ket_create_subtask" => {
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
                    }))
                }
                Err(_) => {
                    let content = String::from_utf8_lossy(&data).into_owned();
                    Ok(serde_json::json!({ "content": content }))
                }
            }
        }
        "ket_score" => {
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
        _ => Err(McpError::UnknownTool(tool_name.to_string())),
    }
}

/// Handle a JSON-RPC request (MCP protocol).
pub fn handle_jsonrpc(
    request: &JsonRpcRequest,
    cas: &ket_cas::Store,
    db: &ket_sql::DoltDb,
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
pub fn run_stdio_server(cas: &ket_cas::Store, db: &ket_sql::DoltDb) -> Result<(), McpError> {
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
