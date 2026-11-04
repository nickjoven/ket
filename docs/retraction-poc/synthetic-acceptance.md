# Synthetic acceptance report

**Invented fixtures only.** These rows demonstrate graph mechanics; they are not a paper dataset or findings about scientific results.

| Case | Downstream node | Status | Depth | Lost / total support | Witness path (labels) |
|---|---|---|---:|---:|---|
| support-loss | superseded | invalidated | 1 | 1/1 | retracted-ground → superseded |
| support-loss | right | invalidated | 1 | 1/1 | retracted-ground → right |
| support-loss | left | invalidated | 1 | 1/1 | retracted-ground → left |
| support-loss | partly-supported | weakened | 1 | 1/2 | retracted-ground → partly-supported |
| support-loss | child-of-weakened | intact | 2 | 0/1 | retracted-ground → partly-supported → child-of-weakened |
| support-loss | diamond | invalidated | 2 | 2/2 | retracted-ground → right → diamond |
| missing-support | incomplete-paper | unknown | 1 | 1/2 | retracted-ground → incomplete-paper |
| missing-support | child-of-unknown | unknown | 2 | 0/1 | retracted-ground → incomplete-paper → child-of-unknown |
| external-reference | paper-with-external-reference | unknown | 1 | 1/2 | retracted-ground → paper-with-external-reference |
| external-reference | child-of-unresolved | unknown | 2 | 0/1 | retracted-ground → paper-with-external-reference → child-of-unresolved |

All three runs have `complete: false`: support-loss stops at a replacement; missing-support lacks a parent; external-reference preserves unresolved support as a raw non-DAG blob.
The replacement and the child held behind it are absent from the first table, with their CIDs retained in `replacement_boundaries` and `blocked`.

Full node CIDs, support-parent lists, witness paths, and completeness reasons:

- [Support-loss fixture](acceptance-fixtures/support-loss-fixture.json) and [report](acceptance-fixtures/support-loss-report.json)
- [Missing-support fixture](acceptance-fixtures/missing-support-fixture.json) and [report](acceptance-fixtures/missing-support-report.json)
- [External-reference fixture](acceptance-fixtures/external-reference-fixture.json) and [report](acceptance-fixtures/external-reference-report.json)
- [Assertion counts and report hashes](acceptance-fixtures/summary.json)

The script checks 13 expected statuses and 5 expected absences. It verifies stored CIDs, every witness edge and depth, support counts, and the full support set for each invalidated node.
A witness path proves one route; all-parent loss requires the support lists. A paper's citations only approximate support, so these mechanics cannot establish that a paper is false.

Reproduce from the repository root (choose a new or empty output directory):

```sh
cargo build -p ket-cli
python3 docs/retraction-poc/acceptance_report.py \
  --ket target/debug/ket --output /tmp/ket24-acceptance \
  --check-against docs/retraction-poc/acceptance-fixtures
```

The command needs only Python's standard library and the built CLI. It performs no network calls and starts no services. Node timestamps are fixed; `provenance.json` records the local binary hash separately from portable fixture evidence. `work/` contains disposable CAS stores and is not checked in.
