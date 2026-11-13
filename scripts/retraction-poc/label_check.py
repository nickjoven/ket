"""Check the labelling against ket.invalidated/v1 on a replayed capture.

Run after replay.py. Labels the whole replayed store with `--support any`
(one standing citation is enough, the conservative reading of a citation
graph) and requires the nodes it marks `out` to be exactly the nodes the
replay's v1 reports mark refuted or invalidated.

    python3 scripts/retraction-poc/label_check.py \
        --replay /tmp/ket24-replay --ket ./target/debug/ket
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--replay", type=Path, required=True)
parser.add_argument("--ket", required=True)
args = parser.parse_args()

raw = subprocess.run(
    [args.ket, "--home", str(args.replay / ".ket"), "dag", "label", "--all", "--support", "any"],
    capture_output=True,
    check=True,
).stdout
labels = json.loads(raw)
if labels["policy"]["support"] != "any":
    sys.exit("label report was not computed under support: any")
out = {n["cid"] for n in labels["nodes"] if n["label"] == "out"}

reports = sorted(args.replay.glob("report-*.json"))
if not reports:
    sys.exit("no v1 reports in the replay directory")
lost = set()
for path in reports:
    report = json.loads(path.read_text())
    if report["schema"] != "ket.invalidated/v1":
        sys.exit(f"{path.name}: unexpected schema {report['schema']}")
    lost |= {n["cid"] for n in report["nodes"] if n["status"] in ("refuted", "invalidated")}

summary = {
    "policy_cid": labels["policy"]["cid"],
    "counts": labels["counts"],
    "out": len(out),
    "v1_refuted_or_invalidated": len(lost),
    "only_in_label": sorted(out - lost),
    "only_in_v1": sorted(lost - out),
}
print(json.dumps(summary, indent=2))
if out != lost:
    sys.exit("labelling under support: any disagrees with v1 on what falls")
