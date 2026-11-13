//! CLI/MCP parity against the same CAS fixture, without requiring Dolt.
use ket_cas::{Cid, Store};
use ket_dag::{Dag, DagNode, EdgeKind, NodeKind};
use serde_json::{json, Value};
use std::process::Command;

fn node(cas: &Store, label: &str, parents: Vec<(Cid, EdgeKind)>) -> Cid {
    let content = cas.put(label.as_bytes()).unwrap();
    Dag::new(cas)
        .put_node(&DagNode::new_typed(
            NodeKind::Reasoning,
            parents,
            content,
            "test",
        ))
        .unwrap()
}

#[test]
fn invalidated_cli_mcp_parity_and_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join(".ket");
    let cas = Store::init(&home.join("cas")).unwrap();
    let seed = node(&cas, "retracted", vec![]);
    let other = node(&cas, "unaffected", vec![]);
    let invalid = node(
        &cas,
        "fully dependent",
        vec![(seed.clone(), EdgeKind::Grounds)],
    );
    let weak = node(
        &cas,
        "partly dependent",
        vec![
            (seed.clone(), EdgeKind::Grounds),
            (other, EdgeKind::Derives),
        ],
    );
    let later = node(
        &cas,
        "depends on weakened",
        vec![(weak.clone(), EdgeKind::Derives)],
    );
    let refuted = node(
        &cas,
        "recorded refutation target",
        vec![(seed.clone(), EdgeKind::Grounds)],
    );
    node(&cas, "notice", vec![(refuted.clone(), EdgeKind::Refutes)]);

    for (args, params) in [
        (vec![], json!({"cid": seed.as_str()})),
        (
            vec!["--with-recorded-refutations", "--include-intact"],
            json!({"cid": seed.as_str(), "with_recorded_refutations": true, "include_intact": true}),
        ),
        (
            vec!["--max-nodes", "1"],
            json!({"cid": seed.as_str(), "max_nodes": 1}),
        ),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_ket"))
            .arg("--home")
            .arg(&home)
            .args(["--json", "dag", "invalidated", seed.as_str()])
            .args(&args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let cli: Value = serde_json::from_slice(&output.stdout).unwrap();
        let mcp = ket_mcp::handle_tool_call("ket_invalidated", &params, &cas, None).unwrap();
        assert_eq!(cli, mcp);
        if args.is_empty() {
            let status = |cid: &Cid| {
                cli["nodes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|n| n["cid"] == cid.as_str())
                    .map(|n| n["status"].as_str().unwrap())
            };
            assert_eq!(status(&invalid), Some("invalidated"));
            assert_eq!(status(&weak), Some("weakened"));
            assert_eq!(status(&later), None);
            assert_eq!(cli["complete"], true);
        }
    }
    // A replacement stops expansion at the seed, and does not masquerade as
    // a complete negative result when dependents were held back.
    node(
        &cas,
        "replacement",
        vec![(seed.clone(), EdgeKind::Supersedes)],
    );
    let report = ket_mcp::handle_tool_call(
        "ket_invalidated",
        &json!({"cid": seed.as_str()}),
        &cas,
        None,
    )
    .unwrap();
    assert_eq!(report["complete"], false);
    assert_eq!(report["nodes"].as_array().unwrap().len(), 1);
    assert!(!report["blocked"].as_array().unwrap().is_empty());
}

#[test]
fn invalidated_rejects_bad_arguments_and_absent_seed() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join(".ket");
    let cas = Store::init(&home.join("cas")).unwrap();
    let cid = node(&cas, "seed", vec![]);
    for params in [
        json!({}),
        json!({"cid": "../../bad"}),
        json!({"cid": cid.as_str(), "include_intact": "false"}),
        json!({"cid": cid.as_str(), "with_recorded_refutations": 1}),
        json!({"cid": cid.as_str(), "max_nodes": -1}),
        json!({"cid": cid.as_str(), "max_nodes": 1.5}),
        json!({"cid": cid.as_str(), "max_nodes": null}),
        json!({"cid": cid.as_str(), "extra": true}),
    ] {
        assert!(
            matches!(
                ket_mcp::handle_tool_call("ket_invalidated", &params, &cas, None),
                Err(ket_mcp::McpError::InvalidParams(_))
            ),
            "{params}"
        );
    }
    let missing = "0".repeat(64);
    assert!(
        ket_mcp::handle_tool_call("ket_invalidated", &json!({"cid": missing}), &cas, None).is_err()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_ket"))
        .arg("--home")
        .arg(home)
        .args(["dag", "invalidated", &missing])
        .output()
        .unwrap();
    assert!(!output.status.success());
}
