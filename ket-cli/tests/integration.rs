//! Integration tests for the ket CLI.
//!
//! These test the real binary end-to-end: init, CAS, DAG, MCP JSON-RPC,
//! repair, drift detection, CDOM scanning.

use std::process::Command;
use std::path::PathBuf;

fn ket_bin() -> PathBuf {
    // cargo test builds to target/debug
    let mut path = std::env::current_exe().unwrap();
    path.pop(); // remove test binary name
    path.pop(); // remove deps
    path.push("ket");
    path
}

fn fresh_ket(name: &str) -> (PathBuf, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let ket_dir = dir.path().join(".ket");
    let output = Command::new(ket_bin())
        .args(["--home", ket_dir.to_str().unwrap(), "init"])
        .current_dir(dir.path())
        .output()
        .unwrap();
    assert!(output.status.success(), "init failed for {name}: {}", String::from_utf8_lossy(&output.stderr));
    (ket_dir, dir)
}

fn ket(ket_dir: &PathBuf, args: &[&str]) -> (bool, String, String) {
    let mut full_args = vec!["--home", ket_dir.to_str().unwrap()];
    full_args.extend_from_slice(args);
    let output = Command::new(ket_bin())
        .args(&full_args)
        .output()
        .unwrap();
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

fn ket_json(ket_dir: &PathBuf, args: &[&str]) -> serde_json::Value {
    let mut full_args = vec!["--home", ket_dir.to_str().unwrap(), "--json"];
    full_args.extend_from_slice(args);
    let output = Command::new(ket_bin())
        .args(&full_args)
        .output()
        .unwrap();
    assert!(output.status.success(), "ket {:?} failed: {}", args, String::from_utf8_lossy(&output.stderr));
    serde_json::from_str(&String::from_utf8_lossy(&output.stdout))
        .unwrap_or_else(|e| panic!("JSON parse failed for {:?}: {e}\nstdout: {}", args, String::from_utf8_lossy(&output.stdout)))
}

// --- CAS tests ---

#[test]
fn cas_put_get_roundtrip() {
    let (ket_dir, dir) = fresh_ket("cas-roundtrip");
    let test_file = dir.path().join("test.txt");
    std::fs::write(&test_file, b"hello world").unwrap();

    let result = ket_json(&ket_dir, &["put", test_file.to_str().unwrap()]);
    let cid = result["cid"].as_str().unwrap();
    assert_eq!(cid.len(), 64);

    let get_result = ket_json(&ket_dir, &["get", cid]);
    assert_eq!(get_result["content"].as_str().unwrap(), "hello world");
}

#[test]
fn cas_verify() {
    let (ket_dir, dir) = fresh_ket("cas-verify");
    let test_file = dir.path().join("test.txt");
    std::fs::write(&test_file, b"verify me").unwrap();

    let result = ket_json(&ket_dir, &["put", test_file.to_str().unwrap()]);
    let cid = result["cid"].as_str().unwrap();

    let verify = ket_json(&ket_dir, &["verify", cid]);
    assert!(verify["valid"].as_bool().unwrap());
}

#[test]
fn cas_dedup() {
    let (ket_dir, dir) = fresh_ket("cas-dedup");
    let f1 = dir.path().join("a.txt");
    let f2 = dir.path().join("b.txt");
    std::fs::write(&f1, b"same content").unwrap();
    std::fs::write(&f2, b"same content").unwrap();

    let r1 = ket_json(&ket_dir, &["put", f1.to_str().unwrap()]);
    let r2 = ket_json(&ket_dir, &["put", f2.to_str().unwrap()]);
    assert_eq!(r1["cid"], r2["cid"]);
}

// --- DAG tests ---

#[test]
fn dag_create_and_lineage() {
    let (ket_dir, _dir) = fresh_ket("dag-lineage");

    let root = ket_json(&ket_dir, &["dag", "create", "root content", "--kind", "memory", "--agent", "human"]);
    let root_cid = root["node_cid"].as_str().unwrap().to_string();

    let child = ket_json(&ket_dir, &["dag", "create", "child content", "--kind", "memory", "--agent", "claude", "--parent", &root_cid]);
    let child_cid = child["node_cid"].as_str().unwrap().to_string();

    // Lineage should have 2 nodes
    let lineage = ket_json(&ket_dir, &["dag", "lineage", &child_cid]);
    let lineage_arr = lineage.as_array().unwrap();
    assert_eq!(lineage_arr.len(), 2);
}

#[test]
fn dag_drift_detection() {
    let (ket_dir, dir) = fresh_ket("dag-drift");
    let test_file = dir.path().join("tracked.txt");
    std::fs::write(&test_file, b"original").unwrap();

    let result = ket_json(&ket_dir, &["put", test_file.to_str().unwrap()]);
    let cid = result["cid"].as_str().unwrap();

    // No drift
    let drift = ket_json(&ket_dir, &["dag", "drift", test_file.to_str().unwrap(), cid]);
    assert!(!drift["drifted"].as_bool().unwrap());

    // Modify file
    std::fs::write(&test_file, b"modified").unwrap();
    let drift = ket_json(&ket_dir, &["dag", "drift", test_file.to_str().unwrap(), cid]);
    assert!(drift["drifted"].as_bool().unwrap());
}

// --- MCP JSON-RPC tests ---

#[test]
fn mcp_initialize() {
    let (ket_dir, _dir) = fresh_ket("mcp-init");

    let request = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
    let output = Command::new(ket_bin())
        .args(["--home", ket_dir.to_str().unwrap(), "mcp"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            if let Some(ref mut stdin) = child.stdin {
                writeln!(stdin, "{request}").ok();
            }
            drop(child.stdin.take());
            child.wait_with_output()
        });

    if let Ok(output) = output {
        let stdout = String::from_utf8_lossy(&output.stdout);
        if let Some(line) = stdout.lines().next() {
            let response: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(response["result"]["serverInfo"]["name"], "ket");
            assert_eq!(response["result"]["protocolVersion"], "2024-11-05");
        }
    }
}

#[test]
fn mcp_tools_list() {
    let (ket_dir, _dir) = fresh_ket("mcp-tools");

    let request = r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#;
    let output = Command::new(ket_bin())
        .args(["--home", ket_dir.to_str().unwrap(), "mcp"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            if let Some(ref mut stdin) = child.stdin {
                writeln!(stdin, "{request}").ok();
            }
            drop(child.stdin.take());
            child.wait_with_output()
        });

    if let Ok(output) = output {
        let stdout = String::from_utf8_lossy(&output.stdout);
        if let Some(line) = stdout.lines().next() {
            let response: serde_json::Value = serde_json::from_str(line).unwrap();
            let tools = response["result"]["tools"].as_array().unwrap();
            assert_eq!(tools.len(), 20);

            let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
            assert!(names.contains(&"ket_put"));
            assert!(names.contains(&"ket_get"));
            assert!(names.contains(&"ket_dag_link"));
            assert!(names.contains(&"ket_check_drift"));
            assert!(names.contains(&"ket_score"));
        }
    }
}

#[test]
fn mcp_put_get_roundtrip() {
    let (ket_dir, _dir) = fresh_ket("mcp-put-get");

    // Put
    let put_req = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"ket_put","arguments":{"content":"mcp test content"}}}"#;
    // Get will use the CID from put response

    let output = Command::new(ket_bin())
        .args(["--home", ket_dir.to_str().unwrap(), "mcp"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            if let Some(ref mut stdin) = child.stdin {
                writeln!(stdin, "{put_req}").ok();
            }
            drop(child.stdin.take());
            child.wait_with_output()
        });

    if let Ok(output) = output {
        let stdout = String::from_utf8_lossy(&output.stdout);
        if let Some(line) = stdout.lines().next() {
            let response: serde_json::Value = serde_json::from_str(line).unwrap();
            // MCP returns content as text
            let content_text = response["result"]["content"][0]["text"].as_str().unwrap();
            let inner: serde_json::Value = serde_json::from_str(content_text).unwrap();
            assert!(inner["cid"].as_str().unwrap().len() == 64);
        }
    }
}

// --- Repair test ---

#[test]
fn repair_idempotent() {
    let (ket_dir, _dir) = fresh_ket("repair-idempotent");

    // Create some nodes
    ket_json(&ket_dir, &["dag", "create", "node 1", "--kind", "memory", "--agent", "human"]);
    ket_json(&ket_dir, &["dag", "create", "node 2", "--kind", "code", "--agent", "claude"]);

    if !has_dolt() {
        return;
    }

    // First repair
    let r1 = ket_json(&ket_dir, &["repair"]);
    // Nodes might or might not need syncing depending on dual-write
    let synced1 = r1["synced"].as_u64().unwrap();
    let skipped1 = r1["skipped"].as_u64().unwrap();
    assert!(synced1 + skipped1 >= 2);

    // Second repair should skip all
    let r2 = ket_json(&ket_dir, &["repair"]);
    assert_eq!(r2["synced"].as_u64().unwrap(), 0);
    assert!(r2["skipped"].as_u64().unwrap() >= 2);
}

// --- CDOM test ---

#[test]
fn cdom_parse_and_query() {
    let (ket_dir, dir) = fresh_ket("cdom");
    let py_file = dir.path().join("example.py");
    std::fs::write(&py_file, r#"
class UserProfile:
    def __init__(self, name):
        self.name = name

    def greet(self):
        return f"Hello, {self.name}"

def process_data(items):
    return [x * 2 for x in items]
"#).unwrap();

    let result = ket_json(&ket_dir, &["scan", py_file.to_str().unwrap()]);
    assert!(result["symbols"].as_u64().unwrap() >= 3);

    // Query specific symbol
    let (ok, stdout, _) = ket(&ket_dir, &["cdom", "UserProfile", py_file.to_str().unwrap()]);
    assert!(ok);
    assert!(stdout.contains("UserProfile"));
    assert!(stdout.contains("class"));
}

// --- Log test ---

#[test]
fn log_records_operations() {
    let (ket_dir, dir) = fresh_ket("log");
    let f = dir.path().join("f.txt");
    std::fs::write(&f, b"data").unwrap();

    ket(&ket_dir, &["put", f.to_str().unwrap()]);
    ket(&ket_dir, &["dag", "create", "test", "--kind", "code", "--agent", "human"]);

    let result = ket_json(&ket_dir, &["log", "-n", "10"]);
    let entries = result.as_array().unwrap();
    assert!(entries.len() >= 3); // init + put + dag:create
}

// --- GC tests ---

#[test]
fn gc_identifies_orphans() {
    let (ket_dir, dir) = fresh_ket("gc");

    // Put raw content (not a DAG node) — this becomes an orphan
    let f = dir.path().join("orphan.txt");
    std::fs::write(&f, b"orphan content").unwrap();
    ket(&ket_dir, &["put", f.to_str().unwrap()]);

    // Create a DAG node (references a content blob)
    ket_json(&ket_dir, &["dag", "create", "kept", "--kind", "code", "--agent", "human"]);

    // GC dry run should find the orphan
    let result = ket_json(&ket_dir, &["gc"]);
    assert!(result["unreferenced"].as_u64().unwrap() >= 1);
    assert!(!result["deleted"].as_bool().unwrap());

    // GC with --delete
    let result = ket_json(&ket_dir, &["gc", "--delete"]);
    assert!(result["deleted"].as_bool().unwrap());

    // Second GC should find nothing
    let result = ket_json(&ket_dir, &["gc"]);
    assert_eq!(result["unreferenced"].as_u64().unwrap(), 0);
}

// --- Export/Import tests ---

#[test]
fn export_import_roundtrip() {
    let (ket_dir_a, dir_a) = fresh_ket("export-a");
    let (ket_dir_b, _dir_b) = fresh_ket("export-b");

    // Create a chain in store A
    let root = ket_json(&ket_dir_a, &["dag", "create", "root", "--kind", "memory", "--agent", "human"]);
    let root_cid = root["node_cid"].as_str().unwrap().to_string();

    let child = ket_json(&ket_dir_a, &["dag", "create", "child", "--kind", "reasoning", "--agent", "claude", "--parent", &root_cid]);
    let child_cid = child["node_cid"].as_str().unwrap().to_string();

    // Export from A
    let bundle_path = dir_a.path().join("bundle.json");
    let (ok, _, _) = ket(&ket_dir_a, &["export", &child_cid, "-o", bundle_path.to_str().unwrap()]);
    assert!(ok);

    // Import into B
    let result = ket_json(&ket_dir_b, &["import", bundle_path.to_str().unwrap()]);
    assert!(result["imported_blobs"].as_u64().unwrap() >= 2); // at least node + content

    // Verify the node exists in B
    let lineage = ket_json(&ket_dir_b, &["dag", "lineage", &child_cid]);
    assert_eq!(lineage.as_array().unwrap().len(), 2);
}

// --- Merge tests ---

#[test]
fn merge_creates_multi_parent_node() {
    let (ket_dir, _dir) = fresh_ket("merge");

    // Create two independent branches
    let a = ket_json(&ket_dir, &["dag", "create", "branch A", "--kind", "reasoning", "--agent", "claude"]);
    let a_cid = a["node_cid"].as_str().unwrap().to_string();

    let b = ket_json(&ket_dir, &["dag", "create", "branch B", "--kind", "reasoning", "--agent", "codex"]);
    let b_cid = b["node_cid"].as_str().unwrap().to_string();

    // Merge them
    let merged = ket_json(&ket_dir, &["merge", "synthesis of A and B", "--parents", &a_cid, &b_cid, "--agent", "human"]);
    let merge_cid = merged["node_cid"].as_str().unwrap().to_string();
    assert_eq!(merged["parents"].as_array().unwrap().len(), 2);

    // Lineage should include all 3 nodes
    let lineage = ket_json(&ket_dir, &["dag", "lineage", &merge_cid]);
    assert_eq!(lineage.as_array().unwrap().len(), 3);
}

// --- CAS stats tests ---

#[test]
fn cas_stats_shows_breakdown() {
    let (ket_dir, dir) = fresh_ket("cas-stats");

    // Put some content
    let f = dir.path().join("f.txt");
    std::fs::write(&f, b"stats test").unwrap();
    ket(&ket_dir, &["put", f.to_str().unwrap()]);
    ket_json(&ket_dir, &["dag", "create", "node", "--kind", "code", "--agent", "human"]);

    let result = ket_json(&ket_dir, &["cas-stats"]);
    assert!(result["total_blobs"].as_u64().unwrap() >= 3); // orphan + dag node + content
    assert!(result["dag_nodes"].as_u64().unwrap() >= 1);
    assert!(result["content_blobs"].as_u64().unwrap() >= 1);
}

// --- DOT output test ---

#[test]
fn dot_outputs_graphviz() {
    let (ket_dir, _dir) = fresh_ket("dot");

    let root = ket_json(&ket_dir, &["dag", "create", "root node", "--kind", "memory", "--agent", "human"]);
    let root_cid = root["node_cid"].as_str().unwrap().to_string();
    ket_json(&ket_dir, &["dag", "create", "child node", "--kind", "code", "--agent", "claude", "--parent", &root_cid]);

    let (ok, stdout, _) = ket(&ket_dir, &["dot"]);
    assert!(ok);
    assert!(stdout.contains("digraph ket"));
    assert!(stdout.contains("rankdir=BT"));
    assert!(stdout.contains("->"));
}

// --- Search test ---

#[test]
fn search_finds_content() {
    let (ket_dir, dir) = fresh_ket("search");

    let f = dir.path().join("searchable.txt");
    std::fs::write(&f, b"the quick brown fox jumps over the lazy dog").unwrap();
    ket(&ket_dir, &["put", f.to_str().unwrap()]);

    let result = ket_json(&ket_dir, &["search", "quick brown"]);
    let results = result.as_array().unwrap();
    assert!(!results.is_empty());
    assert!(results[0]["matches"][0]["text"].as_str().unwrap().contains("quick brown"));
}

// --- Snapshot test ---

#[test]
fn snapshot_create_and_verify() {
    let (ket_dir, _dir) = fresh_ket("snapshot");

    ket_json(&ket_dir, &["dag", "create", "node A", "--kind", "memory", "--agent", "human"]);
    ket_json(&ket_dir, &["dag", "create", "node B", "--kind", "code", "--agent", "claude"]);

    // Create snapshot
    let result = ket_json(&ket_dir, &["snapshot", "create", "v1"]);
    assert!(result["dag_nodes"].as_u64().unwrap() >= 2);

    // List snapshots
    let result = ket_json(&ket_dir, &["snapshot", "ls"]);
    let snaps = result.as_array().unwrap();
    assert_eq!(snaps.len(), 1);
    assert_eq!(snaps[0]["name"], "v1");

    // Verify snapshot
    let result = ket_json(&ket_dir, &["snapshot", "verify", "v1"]);
    assert!(result["ok"].as_bool().unwrap());
}

// --- helpers ---

fn has_dolt() -> bool {
    Command::new("dolt").arg("version").output().is_ok()
}
