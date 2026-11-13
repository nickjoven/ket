//! `ket dag label` and `ket dag consistent` against a CAS fixture, without Dolt.
use ket_cas::{Cid, Store};
use ket_dag::{Dag, DagNode, EdgeKind, NodeKind};
use serde_json::Value;
use std::path::Path;
use std::process::{Command, Output};

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

fn ket(home: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ket"))
        .arg("--home")
        .arg(home)
        .args(args)
        .output()
        .unwrap()
}

fn json(output: Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn label_of<'a>(report: &'a Value, cid: &Cid) -> Option<&'a str> {
    report["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["cid"] == cid.as_str())
        .map(|n| n["label"].as_str().unwrap())
}

#[test]
fn label_reports_defeat_reinstatement_and_the_view_it_was_asked_about() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join(".ket");
    let cas = Store::init(&home.join("cas")).unwrap();
    let claim = node(&cas, "claim", vec![]);
    let built_on = node(
        &cas,
        "built on it",
        vec![(claim.clone(), EdgeKind::Derives)],
    );
    let notice = node(&cas, "retraction", vec![(claim.clone(), EdgeKind::Refutes)]);

    // The view of the dependent alone does not hold the retraction.
    let alone = json(ket(&home, &["dag", "label", built_on.as_str()]));
    assert_eq!(alone["schema"], "ket.labelling/v1");
    assert_eq!(alone["scope"], "closure");
    assert_eq!(label_of(&alone, &claim), Some("in"));
    assert_eq!(label_of(&alone, &notice), None);

    // With the retraction in view, the claim and what rests on it are out.
    let both = json(ket(
        &home,
        &["dag", "label", built_on.as_str(), notice.as_str()],
    ));
    assert_eq!(label_of(&both, &claim), Some("out"));
    assert_eq!(label_of(&both, &built_on), Some("out"));
    assert_eq!(label_of(&both, &notice), Some("in"));
    assert_eq!(both["counts"]["out"], 2);
    assert_eq!(both["complete"], true);

    // Retract the retraction: the whole store has the claim back in.
    let withdrawn = node(
        &cas,
        "retraction withdrawn",
        vec![(notice.clone(), EdgeKind::Refutes)],
    );
    let all = json(ket(&home, &["dag", "label", "--all"]));
    assert_eq!(all["scope"], "store");
    assert_eq!(label_of(&all, &claim), Some("in"));
    assert_eq!(label_of(&all, &built_on), Some("in"));
    assert_eq!(label_of(&all, &notice), Some("out"));
    assert_eq!(label_of(&all, &withdrawn), Some("in"));
}

#[test]
fn consistent_names_the_nodes_two_views_disagree_on() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join(".ket");
    let cas = Store::init(&home.join("cas")).unwrap();
    let claim = node(&cas, "claim", vec![]);
    let built_on = node(
        &cas,
        "built on it",
        vec![(claim.clone(), EdgeKind::Derives)],
    );
    let also = node(
        &cas,
        "also built on it",
        vec![(claim.clone(), EdgeKind::Derives)],
    );
    let notice = node(&cas, "retraction", vec![(claim.clone(), EdgeKind::Refutes)]);

    let agree = json(ket(
        &home,
        &[
            "dag",
            "consistent",
            "--a",
            built_on.as_str(),
            "--b",
            also.as_str(),
        ],
    ));
    assert_eq!(agree["schema"], "ket.consistency/v1");
    assert_eq!(agree["consistent"], true);
    assert_eq!(agree["shared_nodes"], 1);
    assert_eq!(agree["flips"].as_array().unwrap().len(), 0);

    let disagree = json(ket(
        &home,
        &[
            "dag",
            "consistent",
            "--a",
            built_on.as_str(),
            "--a",
            also.as_str(),
            "--b",
            notice.as_str(),
        ],
    ));
    assert_eq!(disagree["consistent"], false);
    let flips = disagree["flips"].as_array().unwrap();
    assert_eq!(flips.len(), 3, "{flips:?}");
    for flip in flips {
        assert_eq!(flip["side"], "a");
        assert_eq!(flip["before"], "in");
        assert_eq!(flip["after"], "out");
        // Every witness ends at the retraction the first view did not hold.
        assert_eq!(
            flip["because"].as_array().unwrap().last().unwrap(),
            notice.as_str()
        );
    }
}

#[test]
fn bad_arguments_and_absent_roots_fail() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join(".ket");
    let cas = Store::init(&home.join("cas")).unwrap();
    let cid = node(&cas, "a", vec![]);
    let absent = "0".repeat(64);
    for args in [
        vec!["dag", "label"],
        vec!["dag", "label", "not-a-cid"],
        vec!["dag", "label", absent.as_str()],
        vec!["dag", "label", "--all", cid.as_str()],
        vec!["dag", "consistent", "--a", cid.as_str()],
        vec![
            "dag",
            "consistent",
            "--a",
            cid.as_str(),
            "--b",
            absent.as_str(),
        ],
        vec!["dag", "consistent", "--a", "../x", "--b", cid.as_str()],
        vec!["dag", "label", cid.as_str(), "--support", "some"],
        vec!["dag", "label", cid.as_str(), "--ignore-attacks-by"],
    ] {
        let output = ket(&home, &args);
        assert!(!output.status.success(), "{args:?} should fail");
    }
}

#[test]
fn the_policy_flags_change_the_reading_and_the_report_records_them() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join(".ket");
    let cas = Store::init(&home.join("cas")).unwrap();
    let cited = node(&cas, "cited", vec![]);
    let also_cited = node(&cas, "also cited", vec![]);
    let paper = node(
        &cas,
        "paper",
        vec![
            (cited.clone(), EdgeKind::Grounds),
            (also_cited, EdgeKind::Grounds),
        ],
    );
    let notice = node(&cas, "retraction", vec![(cited.clone(), EdgeKind::Refutes)]);
    let content = cas.put(b"build failed").unwrap();
    let auto = Dag::new(&cas)
        .put_node(&DagNode::new_typed(
            NodeKind::Score,
            vec![(paper.clone(), EdgeKind::Refutes)],
            content,
            "auto:compile",
        ))
        .unwrap();
    let roots = [paper.as_str(), notice.as_str(), auto.as_str()];

    let default = json(ket(&home, &[&["dag", "label"][..], &roots].concat()));
    assert_eq!(default["policy"]["support"], "all");
    assert_eq!(label_of(&default, &paper), Some("out"));

    // One retracted citation among two no longer takes the paper down, but
    // the auto-scorer's refutation still does.
    let any = json(ket(
        &home,
        &[&["dag", "label", "--support", "any"][..], &roots].concat(),
    ));
    assert_eq!(any["policy"]["support"], "any");
    assert_eq!(label_of(&any, &cited), Some("out"));
    assert_eq!(label_of(&any, &paper), Some("out"));

    let both = json(ket(
        &home,
        &[
            &[
                "dag",
                "label",
                "--support",
                "any",
                "--ignore-attacks-by",
                "auto:compile",
            ][..],
            &roots,
        ]
        .concat(),
    ));
    assert_eq!(label_of(&both, &paper), Some("in"));
    assert_eq!(both["policy"]["ignore_attacks_by"][0], "auto:compile");
    assert_ne!(both["policy"]["cid"], any["policy"]["cid"]);
    let paper_row = both["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["cid"] == paper.as_str())
        .unwrap();
    assert_eq!(paper_row["attackers_ignored"][0], auto.as_str());

    // The same policy flags apply to consistent.
    let c = json(ket(
        &home,
        &[
            "dag",
            "consistent",
            "--a",
            paper.as_str(),
            "--b",
            notice.as_str(),
            "--support",
            "any",
        ],
    ));
    assert_eq!(c["policy"]["support"], "any");
    assert_eq!(c["consistent"], false);
}
