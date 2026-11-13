#!/usr/bin/env python3
"""Offline source-level diagnostics for the frozen ket24 POC policy.

This is not independent scientific validation or an AND/OR justification model.
It shares captured citation edges and declared cycle cuts with the POC, but
uses source IDs and synchronous fixed-point rules rather than CAS or Rust's
topological evaluator. It performs no network calls and needs only stdlib.
"""
import argparse
from collections import Counter, deque
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SEEDS = ("https://openalex.org/W2160675481", "https://openalex.org/W2026087575")
TARGET = "https://openalex.org/W2156452911"
STATES = {"refuted", "invalidated", "weakened", "unknown", "intact"}


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encode(value):
    return (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode()


def load_capture(path):
    raw = (path / "manifest.json").read_bytes()
    manifest = json.loads(raw)
    names = [source["file"] for source in manifest["sources"]]
    consumed = {"openalex-cohort.json", *("openalex-" + seed.rsplit("/", 1)[1] + ".json" for seed in SEEDS),
                *("crossref-" + seed.rsplit("/", 1)[1] + "-updates.json" for seed in SEEDS)}
    require(len(names) == len(set(names)) and consumed <= set(names), "unchecked or duplicated consumed source")
    for source in manifest["sources"]:
        file = path / source["file"]
        require(file.resolve().is_relative_to(path.resolve()), "source path escapes capture")
        content = file.read_bytes()
        require(len(content) == source["bytes"] and sha(content) == source["sha256"], "capture integrity mismatch")
    cohort = json.loads((path / "openalex-cohort.json").read_bytes())
    papers = {item["id"]: item for item in cohort["results"]}
    require(len(papers) == len(cohort["results"]) == cohort["meta"]["count"], "cohort pagination or duplicate gap")
    require(all(set(SEEDS) <= set(item["referenced_works"]) for item in papers.values()), "cites-both condition fails")
    cohort_ids = set(papers)
    for seed in SEEDS:
        item = json.loads((path / ("openalex-" + seed.rsplit("/", 1)[1] + ".json")).read_bytes())
        require(item["id"] == seed, "seed identity mismatch")
        papers[seed] = item
    return papers, cohort_ids, sha(raw)


def graph_from_sources(papers, cuts, drop_external=False):
    graph = {}
    for child, item in papers.items():
        parents = set()
        for parent in set(item.get("referenced_works", [])):
            if (child, parent) in cuts:
                parents.add("gap:cycle:" + parent)
            elif parent in papers:
                parents.add(parent)
            elif not drop_external:
                parents.add("gap:external:" + parent)
        graph[child] = parents
    return graph


def discover(graph, initial):
    children = {node: set() for node in graph}
    for child, parents in graph.items():
        for parent in parents:
            if parent in graph:
                children[parent].add(child)
    found, queue = set(initial), deque(sorted(initial))
    while queue:
        for child in sorted(children[queue.popleft()] - found):
            found.add(child)
            queue.append(child)
    return found


def evaluate(graph, initial, recorded=(), consider_recorded=True):
    """Frozen all-support-lost policy, with any unknown taking precedence.

    Outside-closure parents are undetermined when any ancestry is a gap or
    intersects the closure. That matches the POC's conservative scope rule;
    this harness does not silently treat outside recorded retractions as lost.
    There are no supersedes boundaries in the paper dataset being compared.
    """
    closure = discover(graph, initial)
    refuted = set(initial) | (closure & set(recorded) if consider_recorded else set())
    uncertain = {node for node, parents in graph.items() if any(p not in graph for p in parents)}
    # Declarative reverse propagation proves which external co-parents cannot
    # be established unaffected; it does not consult engine output or CIDs.
    while True:
        expanded = uncertain | {node for node, parents in graph.items() if parents & (uncertain | closure)}
        if expanded == uncertain:
            break
        uncertain = expanded
    status = {node: "refuted" if node in refuted else "intact" for node in closure}
    for iteration in range(len(closure) + 2):
        result = {}
        for node in sorted(closure):
            parents = graph[node]
            lost = {p for p in parents if p in refuted or status.get(p) in {"refuted", "invalidated"}}
            unknown = any(p not in graph or status.get(p) == "unknown" or (p not in closure and p in uncertain) for p in parents)
            result[node] = ("refuted" if node in refuted else "unknown" if unknown else
                            "invalidated" if parents and lost == parents else "weakened" if lost else "intact")
        if result == status:
            return result
        status = result
    raise ValueError("fixed-point diagnostic did not converge")


def counts(status, include=None):
    return dict(sorted(Counter(value for key, value in status.items() if include is None or key in include).items()))


def compare(actual, expected):
    mismatch = {key: {"oracle": actual.get(key), "engine": expected.get(key)}
                for key in sorted(set(actual) | set(expected)) if actual.get(key) != expected.get(key)}
    require(not mismatch, "source-level oracle disagrees with engine: " + json.dumps(mismatch))


def self_test():
    graph = {"seed": set(), "other": set(), "full": {"seed"}, "partial": {"seed", "other"},
             "unknown": {"seed", "gap:external:x"}, "after_unknown": {"unknown"}, "independent": {"other"}}
    got = evaluate(graph, {"seed"})
    require(got == {"seed": "refuted", "full": "invalidated", "partial": "weakened", "unknown": "unknown", "after_unknown": "unknown"},
            "negative-control fixture policy mismatch")
    require("independent" not in got, "unrelated branch entered closure")
    try:
        compare(got, {**got, "partial": "invalidated"})
    except ValueError:
        pass
    else:
        raise ValueError("deliberate oracle mismatch was not detected")
    require(evaluate({"a": set(), "b": {"a"}, "c": {"a", "b"}}, {"b"}, {"a"})["c"] == "weakened",
            "outside-closure recorded refutation leaked into query")
    require(evaluate({"a": set(), "b": {"a"}, "c": {"a", "b"}}, {"a", "b"})["c"] == "invalidated",
            "explicit joint-refutation scope control failed")


def dfs_cuts(papers, order):
    done, active, cuts = set(), set(), set()
    def visit(node):
        if node in done:
            return
        active.add(node)
        for parent in sorted(set(papers[node].get("referenced_works", [])) & set(papers)):
            if parent in active:
                cuts.add((node, parent))
            elif parent not in done:
                visit(parent)
        active.remove(node)
        done.add(node)
    for node in order:
        visit(node)
    return cuts


def run(capture, reports):
    self_test()
    papers, cohort_ids, manifest_sha = load_capture(capture)
    map_raw = (reports / "graph-map.json").read_bytes()
    mapping = json.loads(map_raw)
    require(mapping["capture_manifest_sha256"] == manifest_sha, "report/capture identity mismatch")
    require(mapping["model"] == "conservative", "immutable baseline must be conservative")
    inverse = {cid: source for source, cid in mapping["paper_cids"].items()}
    cuts = {(cut["child"], cut["parent"]) for cut in mapping["cycle_cuts"]}
    graph = graph_from_sources(papers, cuts)
    recorded = set()
    for seed in SEEDS:
        original_doi = papers[seed]["doi"].removeprefix("https://doi.org/").lower()
        updates = json.loads((capture / ("crossref-" + seed.rsplit("/", 1)[1] + "-updates.json")).read_bytes())["message"]["items"]
        if any(assertion.get("DOI", "").lower() == original_doi and assertion.get("type") == "retraction"
               and assertion.get("source") == "retraction-watch" for item in updates for assertion in item.get("update-to", [])):
            recorded.add(seed)
    require(recorded == {notice["seed"] for notice in mapping["notices"]}, "recorded refutations disagree with captured Crossref assertions")
    require(recorded == set(SEEDS), "unexpected recorded refutations")
    scenarios, comparisons, original = {}, [], {}
    for seed in SEEDS:
        tag = seed.rsplit("/", 1)[1]
        raw = (reports / ("report-" + tag + ".json")).read_bytes()
        report = json.loads(raw)
        expected = {inverse[node["cid"]]: node["status"] for node in report["nodes"]}
        require(set(expected.values()) <= STATES and report["scope"] == "with_recorded_refutations", "baseline query contract changed")
        oracle = evaluate(graph, {seed}, recorded)
        compare(oracle, expected)
        original[seed] = oracle
        comparisons.append({"seed": seed, "report_sha256": sha(raw), "compared_paper_statuses": len(expected), "mismatches": 0})
        scenarios[tag + ":closure_recorded"] = {"all_reached": counts(oracle), "cohort_only": counts(oracle, cohort_ids)}
        scenarios[tag + ":seed_only"] = {"all_reached": counts(evaluate(graph, {seed}, consider_recorded=False))}
    joint = evaluate(graph, set(SEEDS), recorded)
    scenarios["explicit_joint_seeds"] = {"all_reached": counts(joint), "cohort_only": counts(joint, cohort_ids)}
    require(graph[TARGET] == set(SEEDS), "sole invalidation support set changed; reassess fragility control")
    added = {node: set(parents) for node, parents in graph.items()}
    added[TARGET].add("gap:external:one-additional-reference")
    removed = {node: set(parents) for node, parents in graph.items()}
    removed[TARGET].remove(SEEDS[0])
    controls = {
        "target": TARGET,
        "captured_reference_count": len(graph[TARGET]),
        "science_query_current": original[SEEDS[0]][TARGET],
        "science_query_add_one_unknown_reference": evaluate(added, {SEEDS[0]}, recorded)[TARGET],
        "pnas_query_current": original[SEEDS[1]][TARGET],
        "pnas_query_remove_science_reference": evaluate(removed, {SEEDS[1]}, recorded)[TARGET],
        "science_query_remove_science_reference": evaluate(removed, {SEEDS[0]}, recorded)[TARGET],
        "scope_note": "Controls alter the source graph, not captured evidence or certified engine output. Removing one already-lost parent does not by itself rescue an all-support-lost node.",
    }
    require(controls["science_query_current"] == "invalidated" and controls["science_query_add_one_unknown_reference"] == "unknown", "missingness fragility control changed")
    require(controls["pnas_query_current"] == "unknown" and controls["pnas_query_remove_science_reference"] == "invalidated", "scope/reference-removal control changed")
    orders = [sorted(papers), sorted(papers, reverse=True)]
    for seed in range(10):
        # Hash-derived permutations do not depend on a runtime's PRNG/shuffle.
        orders.append(sorted(papers, key=lambda node: sha((str(seed) + "\0" + node).encode())))
    distinct_cuts, changed = set(), []
    for number, order in enumerate(orders):
        trial_cuts = dfs_cuts(papers, order)
        distinct_cuts.add(tuple(sorted(trial_cuts)))
        trial = graph_from_sources(papers, trial_cuts)
        for seed in SEEDS:
            got = evaluate(trial, {seed}, recorded)
            differences = sorted(node for node in set(got) | set(original[seed]) if got.get(node) != original[seed].get(node))
            if differences:
                changed.append({"order": number, "seed": seed, "changed_source_ids": differences})
    return {
        "schema_version": 1,
        "diagnostic": "source-level model checks, not independent scientific validation",
        "policy": "legacy all-support-lost, any-unknown precedence, no empty-support invalidation; grounds and derives treated alike; no supersedes boundaries in this paper dataset",
        "shared_inputs": "Pinned captured citations and declared cycle cuts are shared with the POC. Python synchronous fixed point and source IDs are independent of CAS construction/Rust topological traversal. Agreement validates this implementation/model pair, not scientific truth or novelty.",
        "capture_manifest_sha256": manifest_sha, "graph_map_sha256": sha(map_raw),
        "paper_count": len(papers), "selected_cohort_count": len(cohort_ids),
        "selection": {"all_selected_papers_directly_cite_both_seeds": True, "independently_isolates_multihop_impact": False,
                      "reason": "All cohort papers have a one-hop support path from either seed by selection. Longer paths exist, but the reported depth-1 reachability does not isolate multihop impact from direct seed citations."},
        "baseline_comparisons": comparisons, "scenarios": scenarios,
        "missingness_controls": controls,
        "cycle_order_diagnostic": {"deterministic_orders_tested": len(orders), "distinct_cut_sets": len(distinct_cuts),
                                    "status_differences": changed, "scope": "Conservative source-ID verdicts on this dataset only; no general invariance theorem or CID invariance claimed."},
        "self_tests": ["one lost parent plus intact parent remains weakened", "unknown overrides partial loss", "unknown propagates", "unrelated branch excluded", "deliberately wrong oracle comparison rejected", "outside recorded seed not silently applied", "explicit joint seed scope changes verdict"],
        "unvalidated": ["citation intent and whether listed citations are necessary/alternative support", "human correctness labels and empirical precision/recall", "all cohort retractions", "general AND/OR justification semantics", "global or simultaneous refutation support in the public CLI", "performance superiority or algorithmic novelty"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", type=Path, default=ROOT / "fixtures/retraction-poc/verified")
    parser.add_argument("--reports", type=Path, default=ROOT / "docs/retraction-poc/paper-results")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check-against", type=Path)
    args = parser.parse_args()
    raw = encode(run(args.capture, args.reports))
    if args.check_against:
        require(args.check_against.read_bytes() == raw, "diagnostic snapshot mismatch")
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(raw)
    else:
        print(raw.decode(), end="")


if __name__ == "__main__":
    main()
