#!/usr/bin/env python3
"""Render real-paper ket24 results from an offline replay, without network.

From the repository root, run scripts/retraction-poc/replay.py first, then:
    python3 docs/retraction-poc/render_papers.py \
        --replay /tmp/ket24-paper-replay \
        --capture fixtures/retraction-poc/verified \
        --output /tmp/ket24-paper-report

Only paper-report.md and paper-results/*.json are portable report artifacts.
The output directory must be empty. Source response hashes and graph identity
are checked before rendering; complete=false is retained visibly in the table.
"""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path


SEEDS = ("W2160675481", "W2026087575")
SHORT_LABELS = {"W2160675481": "XMRV / Science (2009)", "W2026087575": "MLV / PNAS (2010)"}


def pretty(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def require(condition, detail):
    if not condition:
        raise RuntimeError(detail)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay", type=Path, required=True)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest_bytes = (args.capture / "manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    for source in manifest["sources"]:
        raw = (args.capture / source["file"]).read_bytes()
        require(digest(raw) == source["sha256"], "capture hash mismatch: " + source["file"])
        require(len(raw) == source["bytes"], "capture byte count mismatch: " + source["file"])
    map_bytes = (args.replay / "map.json").read_bytes()
    graph = json.loads(map_bytes)
    require(graph["model"] == "conservative", "primary report requires the conservative replay, not a sensitivity model")
    require(graph["capture_manifest_sha256"] == digest(manifest_bytes), "replay was built from a different capture")
    cohort = json.loads((args.capture / "openalex-cohort.json").read_bytes())
    seed_ids = {"https://openalex.org/" + seed for seed in SEEDS}
    require(all(seed_ids <= set(work.get("referenced_works", [])) for work in cohort["results"]),
            "cohort does not satisfy the stated cites-both sampling condition")
    works = {work["id"]: work for work in cohort["results"]}
    for seed in SEEDS:
        work = json.loads((args.capture / f"openalex-{seed}.json").read_bytes())
        works[work["id"]] = work
    require(len(works) == graph["paper_count"], "paper count differs from the capture")
    require(set(works) == set(graph["paper_cids"]), "paper identities differ from replay mapping")
    by_cid = {cid: source_id for source_id, cid in graph["paper_cids"].items()}
    args.output.mkdir(parents=True, exist_ok=True)
    require(not any(args.output.iterdir()), "output directory must be empty")
    evidence = args.output / "paper-results"
    evidence.mkdir()
    results = []
    table = [
        "# Retraction propagation: XMRV / chronic fatigue syndrome", "",
        "This captured co-citation sample tests how a provenance graph responds when each retracted seed is treated as lost support. Citation edges approximate support; these flags are not findings that a paper's conclusions are false.", "",
        "| Retracted seed | Downstream papers reached | Invalidated | Weakened | Unknown | Other refuted | Depth range | Complete? |",
        "|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for seed in SEEDS:
        source_id = "https://openalex.org/" + seed
        seed_cid = graph["paper_cids"][source_id]
        report = json.loads((args.replay / f"report-{seed}.json").read_bytes())
        require(report["schema"] == "ket.invalidated/v1", "unexpected propagation schema")
        require(report["seed"] == seed_cid, "report seed differs from graph mapping")
        require(report["scope"] == "with_recorded_refutations", "report must opt in to recorded in-closure refutations")
        require(len({node["cid"] for node in report["nodes"]}) == len(report["nodes"]), "duplicate report nodes")
        status_by_cid = {node["cid"]: node["status"] for node in report["nodes"]}
        require(status_by_cid.get(seed_cid) == "refuted", "explicit seed must be refuted")
        downstream = [node for node in report["nodes"] if node["cid"] != seed_cid]
        for result in report["nodes"]:
            require(result["cid"] in by_cid, "report contains a non-paper support descendant")
            witness = result["witness_path"]
            require(witness[0] == seed_cid and witness[-1] == result["cid"], "invalid witness endpoints")
            require(len(witness) == result["depth"] + 1, "wrong witness depth")
            for parent, child in zip(witness, witness[1:]):
                raw_node = json.loads((args.replay / ".ket" / "cas" / child).read_bytes())
                kinds = raw_node.get("parent_kinds", ["derives"] * len(raw_node["parents"]))
                require(any(p == parent and kind in ("grounds", "derives") for p, kind in zip(raw_node["parents"], kinds)), "witness edge is not CAS support")
            raw_node = json.loads((args.replay / ".ket" / "cas" / result["cid"]).read_bytes())
            kinds = raw_node.get("parent_kinds", ["derives"] * len(raw_node["parents"]))
            support = {parent for parent, kind in zip(raw_node["parents"], kinds) if kind in ("grounds", "derives")}
            require(result["support_total"] == len(support), "report support count differs from CAS")
            require(set(result["support_lost"]) <= support, "lost parent is not support")
            if result["status"] == "invalidated":
                require(bool(support) and set(result["support_lost"]) == support, "invalidation lacks all-support-loss proof")
        counts = dict(sorted(Counter(node["status"] for node in downstream).items()))
        depths = [node["depth"] for node in downstream]
        # Show one example per available verdict before repeating statuses;
        # the main table still reports every node, including unknowns.
        priorities = {"invalidated": 0, "weakened": 1, "unknown": 2, "refuted": 3, "intact": 4}
        selected = []
        for status in sorted(counts, key=lambda key: priorities.get(key, 5)):
            selected.append(min((node for node in downstream if node["status"] == status),
                                key=lambda node: (node["depth"], node["cid"])))
        selected = selected[:2]
        examples = []
        for node in selected:
            work = works[by_cid[node["cid"]]]
            examples.append({"source_id": work["id"], "title": work.get("title"), "doi": work.get("doi"),
                             "cid": node["cid"], "status": node["status"], "depth": node["depth"],
                             "support_total": node["support_total"], "support_lost": node["support_lost"],
                             "witness_path": node["witness_path"]})
        notices = [notice for notice in graph["notices"] if notice["seed"] == source_id]
        require(bool(notices), "seed lacks captured retraction evidence")
        require(any(assertion.get("type") == "retraction" and assertion.get("source") == "retraction-watch"
                    for notice in notices for assertion in notice["assertions"]), "seed lacks a Retraction Watch assertion")
        for notice in notices:
            raw_notice = json.loads((args.replay / ".ket" / "cas" / notice["cid"]).read_bytes())
            kinds = raw_notice.get("parent_kinds", ["derives"] * len(raw_notice["parents"]))
            require(any(parent == seed_cid and kind == "refutes" for parent, kind in zip(raw_notice["parents"], kinds)),
                    "notice has no CAS refutes link to its seed")
            captured_notices = json.loads((args.capture / f"crossref-{seed}-updates.json").read_bytes())["message"]["items"]
            require(any(item.get("DOI") == notice["notice_doi"]
                        and all(assertion in item.get("update-to", []) for assertion in notice["assertions"])
                        for item in captured_notices), "retraction assertion differs from captured Crossref evidence")
        row = {
            "seed": source_id, "seed_cid": seed_cid, "title": works[source_id]["title"], "doi": works[source_id].get("doi"),
            "downstream_papers": len(downstream), "statuses": counts,
            "depth_min": min(depths) if depths else None, "depth_max": max(depths) if depths else None,
            "complete": report["complete"], "frontier_count": len(report["frontier"]), "missing_count": len(report["missing"]),
            "undiscovered_count": len(report["undiscovered"]), "blocked_count": len(report["blocked"]),
            "report_sha256": digest(pretty(report).encode()), "examples": examples, "notices": notices,
            "recorded_refuted_papers": [{"source_id": by_cid[node["cid"]], "cid": node["cid"]}
                                         for node in downstream if node["status"] == "refuted"],
        }
        results.append(row)
        depth_range = f"{row['depth_min']}–{row['depth_max']}" if depths else "—"
        seed_link = f"[{SHORT_LABELS[seed]}]({row['doi']})"
        table.append(f"| {seed_link} | {len(downstream)} | {counts.get('invalidated', 0)} | {counts.get('weakened', 0)} | {counts.get('unknown', 0)} | {counts.get('refuted', 0)} | {depth_range} | {'yes' if report['complete'] else '**no**'} |")
        (evidence / f"report-{seed}.json").write_bytes(pretty(report).encode("utf-8"))
    summary = {
        "schema_version": 1, "model": "conservative", "capture_manifest_sha256": digest(manifest_bytes),
        "replay_map_sha256": digest(map_bytes), "captured_at": manifest["captured_at"],
        "cohort_papers": len(cohort["results"]), "total_papers": graph["paper_count"],
        "graph_counts": graph["counts"], "unresolved_unique": len(graph["unresolved_cids"]),
        "cycle_cuts": graph["cycle_cuts"], "sources": manifest["sources"], "seeds": results,
    }
    (evidence / "summary.json").write_bytes(pretty(summary).encode("utf-8"))
    (evidence / "graph-map.json").write_bytes(map_bytes)
    require(any(item["source_id"] == "https://openalex.org/" + SEEDS[1]
                for item in results[0]["recorded_refuted_papers"]),
            "Science closure no longer reaches the recorded PNAS refutation; update the scope explanation")
    require(not any(item["source_id"] == "https://openalex.org/" + SEEDS[0]
                    for item in results[1]["recorded_refuted_papers"]),
            "reverse-query scope changed; update the explanation")
    rw_records = sorted({str(assertion["record-id"]) for row in results for notice in row["notices"]
                         for assertion in notice["assertions"]
                         if assertion.get("type") == "retraction" and assertion.get("source") == "retraction-watch"})
    table += ["", "Counts exclude the queried seed. `Other refuted` counts downstream papers with recorded refutation notices inside that seed's closure; intact counts, all CIDs, and complete status details remain in the JSON."]
    for row in results:
        seed = row["seed"].rsplit("/", 1)[-1]
        reasons = f"missing/non-DAG support {row['missing_count']}; undiscovered support parents {row['undiscovered_count']}; held-back children {row['blocked_count']}; frontier {row['frontier_count']}"
        table += ["", f"**{SHORT_LABELS[seed]}:** {reasons}. [Full CID report](paper-results/report-{seed}.json)."]
        if row["examples"]:
            example = row["examples"][0]
            table.append(f"Example: [{example['title']}]({example['doi'] or example['source_id']}) — `{example['status']}`, depth {example['depth']}, CID `{example['cid']}`. Its full seed-to-paper CID path and lost-parent list are in the [summary](paper-results/summary.json).")
            if example["status"] == "invalidated":
                table.append(f"This node has only {example['support_total']} captured support links; all are lost under the citation model. That is a property of captured references, not a verdict on the paper's conclusions.")
    table += ["", "The rows are not simultaneous-two-seed queries. The Science-seed closure also reaches the PNAS seed and its recorded retraction; the reverse query does not automatically refute the Science ancestor. This scope difference explains why the verdict counts can differ."]
    table += [
        "", "## Scope and evidence", "",
        f"Captured **{manifest['captured_at']}**: {len(cohort['results'])} papers selected because they cite **both** seeds, plus the two seed records ({graph['paper_count']} papers total). This is a selected co-citation cohort, not a random sample or a complete subfield. The keyless OpenAlex list query fit on one page: `cites:W2160675481,cites:W2026087575`, `per_page=200`, `sort=publication_date:asc`.",
        f"The [capture manifest](../../fixtures/retraction-poc/verified/manifest.json) SHA-256 is `{digest(manifest_bytes)}`. [Summary](paper-results/summary.json) preserves each exact request URL, response byte count, and SHA-256; [graph map](paper-results/graph-map.json) pins source IDs to CIDs. Offline reproduction uses captured bytes, not a fresh live query.",
        "", f"Retraction verification is limited to these two seeds: the captured Crossref update records include `type=retraction`, `source=retraction-watch`, record IDs **{'**, **'.join(rw_records)}**. Expression-of-concern and correction records are preserved separately and do not become refutations. OpenAlex `is_retracted` flags for the captured records are retained, but other cohort papers do not receive a comprehensive Crossref/Retraction Watch check.",
        f"The conservative graph keeps {graph['counts']['external_edges']} out-of-sample citation edges as unresolved raw evidence and records {len(graph['cycle_cuts'])} cycle-cut edges. These are not intact, support-free paper nodes. [Graph map](paper-results/graph-map.json) preserves their source IDs, CIDs, and reasons; citation cycles need this disclosed conversion because CID-parent identity forms a DAG.",
        "Each run explicitly refutes one seed. Recorded refutations are considered only within that seed's downstream closure; a refuted co-parent outside the closure is not automatically treated as lost. No simultaneous-two-seed analysis is implied.",
        "", "## Interpretation", "",
        "A citation may be background, methodological, supportive, or critical. The graph has no citation-intent classification or extracted claims. Unknown support is preserved; `complete: false` must not be read as proof that no other papers are affected. An empty invalidated or weakened count is an honest result under this model. Synthetic acceptance examples are [separate](synthetic-acceptance.md).",
        "A witness path proves reachability, not all-support loss. The full reports list every lost/missing/unresolved support parent; the renderer checks witness edges and all-support-loss assertions against the replay CAS.",
        "", "## Reproduce offline", "", "```sh", "cargo build -p ket-cli",
        "python3 scripts/retraction-poc/replay.py \\",
        "  --capture fixtures/retraction-poc/verified --ket target/debug/ket \\",
        "  --out /tmp/ket24-paper-replay",
        "python3 docs/retraction-poc/render_papers.py \\",
        "  --capture fixtures/retraction-poc/verified --replay /tmp/ket24-paper-replay \\",
        "  --output /tmp/ket24-paper-report", "```", "",
        "Choose fresh output directories. Python uses only the standard library. No network, API key, or Dolt service is required. `paper-report.md` and `paper-results/*.json` should reproduce byte-for-byte.", "",
    ]
    (args.output / "paper-report.md").write_bytes("\n".join(table).encode("utf-8"))
    print(pretty({"schema_version": 1, "capture_manifest_sha256": summary["capture_manifest_sha256"],
                  "model": summary["model"], "seeds": [{key: row[key] for key in ("seed", "downstream_papers", "statuses", "complete")} for row in results]}), end="")


if __name__ == "__main__":
    main()
