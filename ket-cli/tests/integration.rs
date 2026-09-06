//! Integration tests for the ket CLI.
//!
//! These test the real binary end-to-end: init, CAS, DAG, MCP JSON-RPC,
//! repair, drift detection, CDOM scanning.

use std::path::{Path, PathBuf};
use std::process::Command;

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
    assert!(
        output.status.success(),
        "init failed for {name}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    (ket_dir, dir)
}

fn ket(ket_dir: &Path, args: &[&str]) -> (bool, String, String) {
    let mut full_args = vec!["--home", ket_dir.to_str().unwrap()];
    full_args.extend_from_slice(args);
    let output = Command::new(ket_bin()).args(&full_args).output().unwrap();
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

fn ket_json(ket_dir: &Path, args: &[&str]) -> serde_json::Value {
    let mut full_args = vec!["--home", ket_dir.to_str().unwrap(), "--json"];
    full_args.extend_from_slice(args);
    let output = Command::new(ket_bin()).args(&full_args).output().unwrap();
    assert!(
        output.status.success(),
        "ket {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_str(&String::from_utf8_lossy(&output.stdout)).unwrap_or_else(|e| {
        panic!(
            "JSON parse failed for {:?}: {e}\nstdout: {}",
            args,
            String::from_utf8_lossy(&output.stdout)
        )
    })
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

    let root = ket_json(
        &ket_dir,
        &[
            "dag",
            "create",
            "root content",
            "--kind",
            "memory",
            "--agent",
            "human",
        ],
    );
    let root_cid = root["node_cid"].as_str().unwrap().to_string();

    let child = ket_json(
        &ket_dir,
        &[
            "dag",
            "create",
            "child content",
            "--kind",
            "memory",
            "--agent",
            "claude",
            "--parent",
            &root_cid,
        ],
    );
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
    let drift = ket_json(
        &ket_dir,
        &["dag", "drift", test_file.to_str().unwrap(), cid],
    );
    assert!(!drift["drifted"].as_bool().unwrap());

    // Modify file
    std::fs::write(&test_file, b"modified").unwrap();
    let drift = ket_json(
        &ket_dir,
        &["dag", "drift", test_file.to_str().unwrap(), cid],
    );
    assert!(drift["drifted"].as_bool().unwrap());
}

#[test]
fn drift_exit_code_follows_drift_check_contract() {
    // `ket drift` is the shell gate agents run before reasoning on tracked
    // context (`ket drift && agent ...`), so its exit status must carry the
    // verdict: 0 clean, 1 drifted or missing. Tracking needs Dolt.
    if !has_dolt() {
        return;
    }
    let (ket_dir, dir) = fresh_ket("drift-exit-code");
    let test_file = dir.path().join("tracked.txt");
    std::fs::write(&test_file, b"original").unwrap();
    let path = test_file.to_str().unwrap();

    let (ok, _, err) = ket(&ket_dir, &["track", "add", path, "--agent", "test"]);
    assert!(ok, "track add failed: {err}");

    let (ok, out, _) = ket(&ket_dir, &["drift"]);
    assert!(ok, "clean tracked file must exit 0: {out}");

    std::fs::write(&test_file, b"modified").unwrap();
    let (ok, out, _) = ket(&ket_dir, &["drift"]);
    assert!(!ok, "drifted tracked file must exit non-zero: {out}");
    assert!(out.contains("DRIFTED"), "report names the drift: {out}");

    std::fs::remove_file(&test_file).unwrap();
    let (ok, out, _) = ket(&ket_dir, &["drift"]);
    assert!(!ok, "missing tracked file must exit non-zero: {out}");
    assert!(
        out.contains("MISSING"),
        "report names the missing file: {out}"
    );
}

/// A typed edge created from the CLI must be sealed in the node, not just
/// written to the projection. Before this test, `--edge-kind grounds` went
/// to `dag_edges` only, so verify-projection diverged on the very next call
/// — the bottom-left cell of DESIGN.md's partition.
#[test]
fn typed_edge_is_sealed_in_node_and_projection_agrees() {
    let (ket_dir, _dir) = fresh_ket("typed-edge-sealed");
    let a = ket_json(
        &ket_dir,
        &[
            "dag",
            "create",
            "measurement",
            "--kind",
            "memory",
            "--agent",
            "human",
        ],
    );
    let a = a["node_cid"].as_str().unwrap();
    let b = ket_json(
        &ket_dir,
        &[
            "dag",
            "create",
            "hypothesis",
            "--kind",
            "reasoning",
            "--agent",
            "claude",
            "--parent",
            a,
            "--edge-kind",
            "proposes",
        ],
    );
    let b = b["node_cid"].as_str().unwrap();

    // Sealed in CAS: the node itself carries the kind.
    let node = ket_json(&ket_dir, &["dag", "show", b]);
    assert_eq!(
        node["parent_kinds"][0], "proposes",
        "node bytes carry the edge kind"
    );

    // Rendered from the node: every format names the kind.
    let (ok, mermaid, _) = ket(&ket_dir, &["graph", "--format", "mermaid"]);
    assert!(ok);
    assert!(
        mermaid.contains("-.->|proposes|"),
        "mermaid styles proposes: {mermaid}"
    );
    assert!(
        mermaid.contains("<br/>reasoning · claude<br/>hypothesis"),
        "label has kind, agent, preview: {mermaid}"
    );
    let (ok, dot, _) = ket(&ket_dir, &["dot"]);
    assert!(ok, "'ket dot' alias still works");
    assert!(
        dot.contains("[label=\"proposes\", style=dashed]"),
        "dot styles proposes: {dot}"
    );
    // Look nodes up by content, not position: the graph orders by timestamp
    // and a clock step between two process launches must not fail this.
    let graph = ket_json(&ket_dir, &["graph", "--format", "json"]);
    assert_eq!(graph["edges"].as_array().unwrap().len(), 1);
    assert_eq!(graph["edges"][0]["kind"], "proposes");
    assert_eq!(graph["edges"][0]["child"], b);
    assert_eq!(graph["edges"][0]["parent"], a);
    let labels: Vec<&str> = graph["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["label"].as_str().unwrap())
        .collect();
    assert!(labels.contains(&"measurement"), "{labels:?}");
    assert!(labels.contains(&"hypothesis"), "{labels:?}");

    if !has_dolt() {
        return;
    }
    // Projected from the node: the SQL row agrees, so verify is clean —
    // and stays clean after a rebuild (idempotent replay).
    let (ok, out, _) = ket(&ket_dir, &["verify-projection"]);
    assert!(
        ok,
        "verify-projection must be clean right after a typed create: {out}"
    );
    ket_json(&ket_dir, &["rebuild-projection"]);
    let (ok, out, _) = ket(&ket_dir, &["verify-projection"]);
    assert!(ok, "still clean after rebuild: {out}");
}

/// A verdict node confirms a claim and is grounded by evidence in one write:
/// `--parent <cid>:<kind>` gives each parent its own kind, and the projection
/// agrees. Resolution edges are ordinary typed parents (DESIGN.md, L2 decided).
#[test]
fn per_parent_edge_kinds_seal_and_project() {
    let (ket_dir, _dir) = fresh_ket("per-parent-kinds");
    let mk = |content: &str, kind: &str| -> String {
        ket_json(&ket_dir, &["dag", "create", content, "--kind", kind])["node_cid"]
            .as_str()
            .unwrap()
            .to_string()
    };
    let claim = mk("claim: no path traversal", "reasoning");
    let evidence = mk("$ grep -rn '\\.\\.' src/ -> 0 hits", "memory");
    let verdict = ket_json(
        &ket_dir,
        &[
            "dag",
            "create",
            "CONFIRMED",
            "--kind",
            "reasoning",
            "--agent",
            "verify",
            "--parent",
            &format!("{claim}:confirms"),
            "--parent",
            &format!("{evidence}:grounds"),
        ],
    );
    let verdict = verdict["node_cid"].as_str().unwrap();
    let node = ket_json(&ket_dir, &["dag", "show", verdict]);
    assert_eq!(
        node["parent_kinds"],
        serde_json::json!(["confirms", "grounds"])
    );

    let corrected = ket_json(
        &ket_dir,
        &[
            "dag",
            "create",
            "claim: no path traversal in put(); get() unchecked",
            "--parent",
            &format!("{claim}:supersedes"),
        ],
    );
    let corrected = corrected["node_cid"].as_str().unwrap();

    let (ok, mermaid, _) = ket(&ket_dir, &["graph", "--format", "mermaid"]);
    assert!(ok);
    assert!(mermaid.contains("-->|confirms|"), "{mermaid}");
    assert!(mermaid.contains("==>|grounds|"), "{mermaid}");
    assert!(mermaid.contains("--o|supersedes|"), "{mermaid}");

    let (ok, _, err) = ket(
        &ket_dir,
        &["dag", "create", "x", "--parent", &format!("{claim}:bogus")],
    );
    assert!(!ok, "unknown kind is rejected, not defaulted");
    assert!(err.contains("Unknown edge kind"), "{err}");

    if !has_dolt() {
        return;
    }
    let (ok, out, _) = ket(&ket_dir, &["verify-projection"]);
    assert!(ok, "mixed-kind parents must project cleanly: {out}");
    let rows = ket(&ket_dir, &["sql", &format!(
        "select edge_kind from dag_edges where child_cid in ('{verdict}','{corrected}') order by edge_kind"
    )]).1;
    assert!(
        rows.contains("confirms") && rows.contains("grounds") && rows.contains("supersedes"),
        "{rows}"
    );
}

/// Merge nodes go through the same sealed path, with the same flag.
#[test]
fn merge_typed_edges_projection_agrees() {
    let (ket_dir, _dir) = fresh_ket("merge-typed");
    let mk = |content: &str| -> String {
        ket_json(&ket_dir, &["dag", "create", content, "--kind", "memory"])["node_cid"]
            .as_str()
            .unwrap()
            .to_string()
    };
    let (a, b) = (mk("A"), mk("B"));
    let m = ket_json(
        &ket_dir,
        &[
            "merge",
            "synthesis",
            "--parents",
            &a,
            &b,
            "--edge-kind",
            "grounds",
        ],
    );
    let m = m["node_cid"].as_str().unwrap();
    let node = ket_json(&ket_dir, &["dag", "show", m]);
    assert_eq!(
        node["parent_kinds"],
        serde_json::json!(["grounds", "grounds"])
    );

    if !has_dolt() {
        return;
    }
    let (ok, out, _) = ket(&ket_dir, &["verify-projection"]);
    assert!(ok, "merge with typed edges must project cleanly: {out}");
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
            assert_eq!(tools.len(), 19);

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
    ket_json(
        &ket_dir,
        &[
            "dag", "create", "node 1", "--kind", "memory", "--agent", "human",
        ],
    );
    ket_json(
        &ket_dir,
        &[
            "dag", "create", "node 2", "--kind", "code", "--agent", "claude",
        ],
    );

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
    std::fs::write(
        &py_file,
        r#"
class UserProfile:
    def __init__(self, name):
        self.name = name

    def greet(self):
        return f"Hello, {self.name}"

def process_data(items):
    return [x * 2 for x in items]
"#,
    )
    .unwrap();

    let result = ket_json(&ket_dir, &["scan", py_file.to_str().unwrap()]);
    assert!(result["symbols"].as_u64().unwrap() >= 3);

    // Query specific symbol
    let (ok, stdout, _) = ket(
        &ket_dir,
        &["cdom", "UserProfile", py_file.to_str().unwrap()],
    );
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
    ket(
        &ket_dir,
        &[
            "dag", "create", "test", "--kind", "code", "--agent", "human",
        ],
    );

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
    ket_json(
        &ket_dir,
        &[
            "dag", "create", "kept", "--kind", "code", "--agent", "human",
        ],
    );

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
    let root = ket_json(
        &ket_dir_a,
        &[
            "dag", "create", "root", "--kind", "memory", "--agent", "human",
        ],
    );
    let root_cid = root["node_cid"].as_str().unwrap().to_string();

    let child = ket_json(
        &ket_dir_a,
        &[
            "dag",
            "create",
            "child",
            "--kind",
            "reasoning",
            "--agent",
            "claude",
            "--parent",
            &root_cid,
        ],
    );
    let child_cid = child["node_cid"].as_str().unwrap().to_string();

    // Export from A
    let bundle_path = dir_a.path().join("bundle.json");
    let (ok, _, _) = ket(
        &ket_dir_a,
        &["export", &child_cid, "-o", bundle_path.to_str().unwrap()],
    );
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
    let a = ket_json(
        &ket_dir,
        &[
            "dag",
            "create",
            "branch A",
            "--kind",
            "reasoning",
            "--agent",
            "claude",
        ],
    );
    let a_cid = a["node_cid"].as_str().unwrap().to_string();

    let b = ket_json(
        &ket_dir,
        &[
            "dag",
            "create",
            "branch B",
            "--kind",
            "reasoning",
            "--agent",
            "codex",
        ],
    );
    let b_cid = b["node_cid"].as_str().unwrap().to_string();

    // Merge them
    let merged = ket_json(
        &ket_dir,
        &[
            "merge",
            "synthesis of A and B",
            "--parents",
            &a_cid,
            &b_cid,
            "--agent",
            "human",
        ],
    );
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
    ket_json(
        &ket_dir,
        &[
            "dag", "create", "node", "--kind", "code", "--agent", "human",
        ],
    );

    let result = ket_json(&ket_dir, &["cas-stats"]);
    assert!(result["total_blobs"].as_u64().unwrap() >= 3); // orphan + dag node + content
    assert!(result["dag_nodes"].as_u64().unwrap() >= 1);
    assert!(result["content_blobs"].as_u64().unwrap() >= 1);
}

// --- DOT output test ---

#[test]
fn dot_outputs_graphviz() {
    let (ket_dir, _dir) = fresh_ket("dot");

    let root = ket_json(
        &ket_dir,
        &[
            "dag",
            "create",
            "root node",
            "--kind",
            "memory",
            "--agent",
            "human",
        ],
    );
    let root_cid = root["node_cid"].as_str().unwrap().to_string();
    ket_json(
        &ket_dir,
        &[
            "dag",
            "create",
            "child node",
            "--kind",
            "code",
            "--agent",
            "claude",
            "--parent",
            &root_cid,
        ],
    );

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
    assert!(results[0]["matches"][0]["text"]
        .as_str()
        .unwrap()
        .contains("quick brown"));
}

// --- Snapshot test ---

#[test]
fn snapshot_create_and_verify() {
    let (ket_dir, _dir) = fresh_ket("snapshot");

    ket_json(
        &ket_dir,
        &[
            "dag", "create", "node A", "--kind", "memory", "--agent", "human",
        ],
    );
    ket_json(
        &ket_dir,
        &[
            "dag", "create", "node B", "--kind", "code", "--agent", "claude",
        ],
    );

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

// --- Verify + rebuild projection (L3 audit contract) ---

/// The projection self-audit gate: after `repair`, the substrate and the
/// projection must agree. Fails loudly on non-clean exit codes so a regression
/// in the L3 mirror (verify_projection) or in node-sourced edges surfaces in CI
/// rather than at runtime.
#[test]
fn verify_projection_after_repair_is_clean() {
    let (ket_dir, _dir) = fresh_ket("verify-projection-clean");

    // Seed the substrate with a small mixed-kind DAG. `repair` gives us a
    // populated projection to audit against; without it, verify would be a
    // trivial "empty vs empty" tautology.
    ket_json(
        &ket_dir,
        &[
            "dag", "create", "root A", "--kind", "memory", "--agent", "human",
        ],
    );
    ket_json(
        &ket_dir,
        &[
            "dag", "create", "root B", "--kind", "memory", "--agent", "human",
        ],
    );

    if !has_dolt() {
        return;
    }

    ket_json(&ket_dir, &["repair"]);

    let (ok, stdout, stderr) = ket(&ket_dir, &["--json", "verify-projection"]);
    assert!(
        ok,
        "verify-projection exit code must be 0 after repair; stdout={stdout} stderr={stderr}"
    );

    let json: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("verify-projection JSON parse: {e}; stdout={stdout}"));
    assert_eq!(json["clean"], serde_json::Value::Bool(true));
    assert_eq!(json["missing"], serde_json::json!(0));
    assert_eq!(json["extra"], serde_json::json!(0));
    assert_eq!(json["mismatched"], serde_json::json!(0));
}

/// The heal contract: after `rebuild-projection`, verify must clear.
/// Also asserts idempotence — a second rebuild wipes and rewrites the same
/// edges, so the projection is bit-identical.
#[test]
fn rebuild_projection_heals_and_is_idempotent() {
    let (ket_dir, _dir) = fresh_ket("rebuild-projection-heals");

    ket_json(
        &ket_dir,
        &[
            "dag", "create", "root", "--kind", "memory", "--agent", "human",
        ],
    );

    if !has_dolt() {
        return;
    }

    ket_json(&ket_dir, &["repair"]);

    let r1 = ket_json(&ket_dir, &["rebuild-projection"]);
    let written1 = r1["edges_written"].as_u64().unwrap();

    // Verify must clear.
    let (ok, _, _) = ket(&ket_dir, &["verify-projection"]);
    assert!(ok, "verify-projection non-clean after rebuild");

    // Idempotent — the second rebuild purges what the first wrote and re-writes
    // the same set. purged and written both equal the same count.
    let r2 = ket_json(&ket_dir, &["rebuild-projection"]);
    assert_eq!(r2["edges_written"].as_u64().unwrap(), written1);
    assert_eq!(r2["edges_purged"].as_u64().unwrap(), written1);
}

// --- helpers ---

fn has_dolt() -> bool {
    Command::new("dolt").arg("version").output().is_ok()
}

// --- Review fixes: parents are validated before sealing; renderers escape ---

fn create_node(ket_dir: &Path, content: &str, extra: &[&str]) -> String {
    let mut args = vec!["dag", "create", content];
    args.extend_from_slice(extra);
    ket_json(ket_dir, &args)["node_cid"]
        .as_str()
        .unwrap()
        .to_string()
}

#[test]
fn malformed_or_dangling_parents_are_rejected_before_sealing() {
    let (ket_dir, _tmp) = fresh_ket("bad-parents");
    let missing = "0".repeat(64);
    for (parent, why) in [
        ("a€€€€", "non-ascii"),
        ("", "empty"),
        (":grounds", "empty with kind"),
        ("deadbeef", "short"),
        (missing.as_str(), "well-formed but absent"),
    ] {
        let (ok, _, err) = ket(&ket_dir, &["dag", "create", "x", "--parent", parent]);
        assert!(!ok, "{why} parent {parent:?} must be rejected");
        assert!(
            err.contains("Malformed parent") || err.contains("Parent not found"),
            "{why}: {err}"
        );
    }
    // Nothing was sealed, so the whole-store graph still renders.
    let (ok, out, _) = ket(&ket_dir, &["graph", "--format", "mermaid"]);
    assert!(ok, "graph renders on a store with only good nodes");
    assert!(out.starts_with("graph BT"));
    // merge gets the same checks.
    let a = create_node(&ket_dir, "a", &[]);
    let (ok, _, err) = ket(&ket_dir, &["merge", "m", "--parents", &a, "deadbeef"]);
    assert!(!ok && err.contains("Malformed parent"), "{err}");
}

#[test]
fn merge_accepts_per_parent_edge_kinds() {
    let (ket_dir, _tmp) = fresh_ket("merge-typed");
    let a = create_node(&ket_dir, "measurement", &[]);
    let b = create_node(&ket_dir, "prior claim", &[]);
    let spec = format!("{a}:grounds");
    let m = ket_json(
        &ket_dir,
        &[
            "merge",
            "synthesis",
            "--parents",
            &spec,
            &b,
            "--edge-kind",
            "supersedes",
        ],
    )["node_cid"]
        .as_str()
        .unwrap()
        .to_string();
    let node = ket_json(&ket_dir, &["dag", "show", &m]);
    let parents: Vec<&str> = node["parents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p.as_str().unwrap())
        .collect();
    assert_eq!(
        parents,
        vec![a.as_str(), b.as_str()],
        "the :kind suffix is not part of the CID"
    );
    assert_eq!(node["parent_kinds"][0], "grounds");
    assert_eq!(node["parent_kinds"][1], "supersedes");
}

#[test]
fn content_can_come_from_a_file_or_stdin_or_start_with_a_dash() {
    let (ket_dir, tmp) = fresh_ket("content-file");
    let big = "x".repeat(300 * 1024); // past argv's single-argument limit
    let path = tmp.path().join("big.txt");
    std::fs::write(&path, &big).unwrap();
    let v = ket_json(
        &ket_dir,
        &["dag", "create", "--content-file", path.to_str().unwrap()],
    );
    let (_, got, _) = ket(&ket_dir, &["get", v["content_cid"].as_str().unwrap()]);
    assert_eq!(got.len(), big.len());

    // `--` lets content that looks like a flag through (a markdown bullet).
    let v = ket_json(&ket_dir, &["dag", "create", "--", "- ran ls .github"]);
    let (_, got, _) = ket(&ket_dir, &["get", v["content_cid"].as_str().unwrap()]);
    assert_eq!(got, "- ran ls .github");

    // stdin
    use std::io::Write;
    let mut child = Command::new(ket_bin())
        .args([
            "--home",
            ket_dir.to_str().unwrap(),
            "--json",
            "dag",
            "create",
            "--content-file",
            "-",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"from stdin")
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success());
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let (_, got, _) = ket(&ket_dir, &["get", v["content_cid"].as_str().unwrap()]);
    assert_eq!(got, "from stdin");

    // Both at once is a usage error, neither is too.
    let (ok, _, _) = ket(&ket_dir, &["dag", "create", "a", "--content-file", "-"]);
    assert!(!ok);
    let (ok, _, _) = ket(&ket_dir, &["dag", "create"]);
    assert!(!ok);
}

#[test]
fn graph_renderers_escape_agent_names_and_labels() {
    let (ket_dir, _tmp) = fresh_ket("graph-escape");
    create_node(
        &ket_dir,
        "{\"title\":\"first\\nsecond \\\"quoted\\\" <b>#1</b> | [x]\"}",
        &["--agent", "ag\"ent]|", "--kind", "context"],
    );
    let (ok, dot, _) = ket(&ket_dir, &["graph", "--format", "dot"]);
    assert!(ok);
    let node_line = dot.lines().find(|l| l.contains("[label=")).unwrap();
    assert!(
        node_line.contains("ag\\\"ent]|"),
        "quote in agent is escaped: {node_line}"
    );
    assert!(
        node_line.contains("first second"),
        "newline in a JSON title collapses to one line: {node_line}"
    );
    assert!(node_line.contains("\\\"quoted\\\""), "{node_line}");

    let (ok, mmd, _) = ket(&ket_dir, &["graph", "--format", "mermaid"]);
    assert!(ok);
    let node_line = mmd.lines().find(|l| l.contains("[\"")).unwrap();
    for raw in ["<b>", "</b>", "]|", "ag\"ent"] {
        assert!(
            !node_line.contains(raw),
            "{raw:?} must not appear raw: {node_line}"
        );
    }
    for esc in ["#quot;", "#lt;b#gt;", "#35;1", "#124;", "#91;x#93;"] {
        assert!(node_line.contains(esc), "{esc} expected: {node_line}");
    }
    // The opening quote is the only unescaped one on the line: `id["` ... `"]`.
    assert_eq!(node_line.matches('"').count(), 2, "{node_line}");
}

#[test]
#[cfg(unix)]
fn drift_exit_2_when_a_tracked_file_cannot_be_read() {
    use std::os::unix::fs::PermissionsExt;
    if !has_dolt() {
        return;
    }
    extern "C" {
        fn geteuid() -> u32;
    }
    if unsafe { geteuid() } == 0 {
        return; // root reads anything; the case cannot be produced
    }
    let (ket_dir, tmp) = fresh_ket("drift-unreadable");
    let f = tmp.path().join("secret.rs");
    std::fs::write(&f, "fn main() {}").unwrap();
    let (ok, _, err) = ket(
        &ket_dir,
        &["track", "add", f.to_str().unwrap(), "--agent", "t"],
    );
    assert!(ok, "{err}");
    std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o000)).unwrap();
    let status = Command::new(ket_bin())
        .args(["--home", ket_dir.to_str().unwrap(), "drift"])
        .output()
        .unwrap();
    std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(
        status.status.code(),
        Some(2),
        "present-but-unreadable is 'cannot check', not 'missing'"
    );
}
