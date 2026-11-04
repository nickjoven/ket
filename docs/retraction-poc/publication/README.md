# Publication iteration index

Starting point: local POC commit `bdc3b71b76a29b2fd2ed3b2b579253a604d1e207`.
Working target: a systems/reproducibility paper, pending the author's preference.
This directory records preparation and falsifiable evaluation criteria; it is not
a claim that the artifact is publication-ready or has passed external review.

| Artifact | Purpose |
|---|---|
| [Protocol](protocol.md) | Claims, methods, acceptance gates, and next iterations |
| [References](references.json) | Bounded primary-source prior-art and licensing screen |
| [Evaluation](evaluation.json) | Reproducible source-level diagnostic and sensitivity controls |
| [Existing case study](../paper-report.md) | Frozen 177-paper POC and its limits |
| [Synthetic acceptance](../synthetic-acceptance.md) | Implementation mechanics and offline replay |

The experimental diagnostic is separate from both the Rust implementation and
the frozen case-study outputs. Its use of the same captured graph must not be
described as independent validation of scientific support relationships.

Run the offline diagnostic from the repository root:

```sh
python3 scripts/retraction-poc/publication_check.py \
  --output /tmp/ket24-publication.json --check-against docs/retraction-poc/publication/evaluation.json
```
