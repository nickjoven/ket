#!/usr/bin/env python3
"""Replay synthetic ket24 acceptance cases using Python stdlib and a built ket.

Run from the repository root:
    cargo build -p ket-cli
    python3 docs/retraction-poc/acceptance_report.py \
        --ket target/debug/ket --output /tmp/ket24-acceptance

The output directory must be empty. No network, Dolt, or installed Python
packages are needed. Fixed timestamps and JSON encoding make node CIDs stable.
Generated Markdown and acceptance-fixtures JSON are portable; work/ contains
disposable CAS stores, and provenance.json records the local executable hash.
"""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def encode(value):
    return (json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode()


def pretty(value):
    return json.dumps(value, ensure_ascii=False, indent=2) + "\n"


def require(condition, detail):
    if not condition:
        raise RuntimeError(detail)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ket", type=Path, required=True, help="Path to the built ket executable")
    parser.add_argument("--output", type=Path, required=True, help="Empty directory for generated artifacts")
    parser.add_argument("--check-against", type=Path, help="Compare generated acceptance-fixtures JSON with this directory")
    args = parser.parse_args()
    ket = args.ket.resolve(strict=True)
    executable_hash = hashlib.sha256(ket.read_bytes()).hexdigest()
    args.output.mkdir(parents=True, exist_ok=True)
    require(not any(args.output.iterdir()), "output directory must be empty")
    output = args.output.resolve()
    evidence = output / "acceptance-fixtures"
    evidence.mkdir()
    fixtures = []

    def run(home, *argv):
        result = subprocess.run(
            [str(ket), "--home", str(home), "--json", *argv],
            check=True, text=True, capture_output=True,
        )
        return json.loads(result.stdout)

    def fixture(name):
        root = output / "work" / name
        home = root / ".ket"
        (home / "cas").mkdir(parents=True)
        inputs = root / "inputs"
        inputs.mkdir()
        nodes = {}

        def put(filename, value):
            path = inputs / filename
            path.write_bytes(encode(value))
            cid = run(home, "put", str(path))["cid"]
            require(run(home, "verify", cid)["valid"], f"CID verification failed: {filename}")
            return cid

        def node(label, parents=()):
            content = put(label + ".content.json", {"schema_version": 1, "synthetic": True, "label": label})
            payload = {
                "kind": "reasoning", "parents": [parent for parent, _ in parents],
                "output_cid": content, "agent": "ket24-independent-fixture",
                "timestamp": "2026-11-04T00:00:00+00:00", "meta": [],
            }
            if any(kind != "derives" for _, kind in parents):
                payload["parent_kinds"] = [kind for _, kind in parents]
            # Store fixed, valid DAG bytes directly. CLI `dag create` uses a
            # wall-clock timestamp and rejects intentionally missing parents.
            cid = put(label + ".node.json", payload)
            nodes[label] = cid
            return cid

        return home, nodes, node, put

    home, nodes, node, _ = fixture("support-loss")
    seed = node("retracted-ground")
    other = node("independent-ground")
    left = node("left", [(seed, "grounds")])
    right = node("right", [(seed, "derives")])
    node("diamond", [(left, "derives"), (right, "derives")])
    weakened = node("partly-supported", [(seed, "grounds"), (other, "grounds")])
    node("child-of-weakened", [(weakened, "derives")])
    old = node("superseded", [(seed, "grounds")])
    node("behind-boundary", [(old, "grounds")])
    replacement = node("replacement", [(old, "supersedes")])
    node("notice", [(seed, "refutes")])
    node("proposal", [(seed, "proposes")])
    report = run(home, "dag", "invalidated", seed, "--with-recorded-refutations", "--include-intact")
    expected = {
        "retracted-ground": "refuted", "left": "invalidated", "right": "invalidated",
        "diamond": "invalidated", "partly-supported": "weakened", "child-of-weakened": "intact",
        "superseded": "invalidated",
    }
    require(report["replacement_boundaries"] == [{"superseded": old, "by": replacement}], "replacement boundary missing")
    require(report["blocked"] == [{"at": old, "child": nodes["behind-boundary"]}], "held-back child not recorded")
    fixtures.append(("support-loss", home, nodes, seed, report, expected,
                     ["independent-ground", "behind-boundary", "replacement", "notice", "proposal"], {}))

    home, nodes, node, _ = fixture("missing-support")
    seed = node("retracted-ground")
    missing = "0" * 64
    unknown = node("incomplete-paper", [(seed, "grounds"), (missing, "grounds")])
    node("child-of-unknown", [(unknown, "derives")])
    report = run(home, "dag", "invalidated", seed, "--include-intact")
    expected = {"retracted-ground": "refuted", "incomplete-paper": "unknown", "child-of-unknown": "unknown"}
    require(missing in report["missing"], "missing support CID not reported")
    fixtures.append(("missing-support", home, nodes, seed, report, expected, [], {"absent": missing}))

    home, nodes, node, put = fixture("external-reference")
    seed = node("retracted-ground")
    external = put("external-reference.json", {
        "schema_version": 1, "synthetic": True,
        "external_reference": "https://example.invalid/outside-sample", "status": "unresolved",
    })
    unknown = node("paper-with-external-reference", [(seed, "grounds"), (external, "grounds")])
    node("child-of-unresolved", [(unknown, "derives")])
    report = run(home, "dag", "invalidated", seed, "--include-intact")
    expected = {"retracted-ground": "refuted", "paper-with-external-reference": "unknown", "child-of-unresolved": "unknown"}
    require(external in report["missing"], "non-DAG external reference must remain unresolved")
    fixtures.append(("external-reference", home, nodes, seed, report, expected, [], {"non_dag_blob": external}))

    table = [
        "# Synthetic acceptance report", "",
        "**Invented fixtures only.** These rows demonstrate graph mechanics; they are not a paper dataset or findings about scientific results.", "",
        "| Case | Downstream node | Status | Depth | Lost / total support | Witness path (labels) |",
        "|---|---|---|---:|---:|---|",
    ]
    summaries = []
    for name, home, nodes, seed, report, expected, absent, external in fixtures:
        by_cid = {result["cid"]: result for result in report["nodes"]}
        labels = {cid: label for label, cid in nodes.items()}
        for label, status in expected.items():
            require(by_cid[nodes[label]]["status"] == status, f"{name}: wrong status for {label}")
        for label in absent:
            require(nodes[label] not in by_cid, f"{name}: unexpected downstream node {label}")
        require(report["schema"] == "ket.invalidated/v1", "unexpected result schema")
        require(report["complete"] is False, f"{name}: incomplete evidence must be explicit")
        for result in report["nodes"]:
            cid = result["cid"]
            path = result["witness_path"]
            require(path[0] == seed and path[-1] == cid, f"{name}: invalid witness endpoints")
            require(len(path) == result["depth"] + 1, f"{name}: wrong depth")
            for parent, child in zip(path, path[1:]):
                raw = json.loads((home / "cas" / child).read_bytes())
                kinds = raw.get("parent_kinds", ["derives"] * len(raw["parents"]))
                require(any(p == parent and k in ("grounds", "derives") for p, k in zip(raw["parents"], kinds)),
                        f"{name}: witness edge is not CAS support")
            raw = json.loads((home / "cas" / cid).read_bytes())
            kinds = raw.get("parent_kinds", ["derives"] * len(raw["parents"]))
            support = {p for p, k in zip(raw["parents"], kinds) if k in ("grounds", "derives")}
            require(result["support_total"] == len(support), f"{name}: wrong support count")
            require(set(result["support_lost"]) <= support, f"{name}: lost parent is not support")
            if result["status"] == "invalidated":
                require(bool(support) and set(result["support_lost"]) == support, f"{name}: invalidated without all support lost")
            if cid != seed:
                table.append(f"| {name} | {labels[cid]} | {result['status']} | {result['depth']} | "
                             f"{len(result['support_lost'])}/{result['support_total']} | "
                             + " → ".join(labels[step] for step in path) + " |")
        manifest = {
            "schema_version": 1, "synthetic": True, "seed": seed, "nodes": nodes,
            "expected": expected, "expected_absent": absent, "external_references": external,
        }
        (evidence / f"{name}-fixture.json").write_text(pretty(manifest))
        (evidence / f"{name}-report.json").write_text(pretty(report))
        summaries.append({
            "case": name, "seed": seed, "status_assertions": len(expected),
            "absence_assertions": len(absent), "report_sha256": hashlib.sha256(encode(report)).hexdigest(),
            "complete": report["complete"],
        })
    summary = {"schema_version": 1, "synthetic": True, "cases": summaries}
    (evidence / "summary.json").write_text(pretty(summary))
    table += [
        "", "All three runs have `complete: false`: support-loss stops at a replacement; missing-support lacks a parent; external-reference preserves unresolved support as a raw non-DAG blob.",
        "The replacement and the child held behind it are absent from the first table, with their CIDs retained in `replacement_boundaries` and `blocked`.",
        "", "Full node CIDs, support-parent lists, witness paths, and completeness reasons:", "",
        "- [Support-loss fixture](acceptance-fixtures/support-loss-fixture.json) and [report](acceptance-fixtures/support-loss-report.json)",
        "- [Missing-support fixture](acceptance-fixtures/missing-support-fixture.json) and [report](acceptance-fixtures/missing-support-report.json)",
        "- [External-reference fixture](acceptance-fixtures/external-reference-fixture.json) and [report](acceptance-fixtures/external-reference-report.json)",
        "- [Assertion counts and report hashes](acceptance-fixtures/summary.json)",
        "", "The script checks 13 expected statuses and 5 expected absences. It verifies stored CIDs, every witness edge and depth, support counts, and the full support set for each invalidated node.",
        "A witness path proves one route; all-parent loss requires the support lists. A paper's citations only approximate support, so these mechanics cannot establish that a paper is false.",
        "", "Reproduce from the repository root (choose a new or empty output directory):", "",
        "```sh", "cargo build -p ket-cli",
        "python3 docs/retraction-poc/acceptance_report.py \\",
        "  --ket target/debug/ket --output /tmp/ket24-acceptance \\",
        "  --check-against docs/retraction-poc/acceptance-fixtures", "```", "",
        "The command needs only Python's standard library and the built CLI. It performs no network calls and starts no services. Node timestamps are fixed; `provenance.json` records the local binary hash separately from portable fixture evidence. `work/` contains disposable CAS stores and is not checked in.", "",
    ]
    (output / "synthetic-acceptance.md").write_text("\n".join(table))
    require(hashlib.sha256(ket.read_bytes()).hexdigest() == executable_hash, "ket executable changed during replay; run again against a stable build")
    (output / "provenance.json").write_text(pretty({"schema_version": 1, "ket_binary_sha256": executable_hash}))
    if args.check_against:
        expected_files = sorted(path.name for path in evidence.glob("*.json"))
        require(expected_files == sorted(path.name for path in args.check_against.glob("*.json")), "fixture file set differs")
        for name in expected_files:
            require((evidence / name).read_bytes() == (args.check_against / name).read_bytes(), f"fixture differs: {name}")
    print(pretty(summary), end="")


if __name__ == "__main__":
    main()
