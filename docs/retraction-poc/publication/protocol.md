# Toward a publishable support-loss artifact

Status: iteration 1; no publication-readiness or novelty claim.
POC baseline: `bdc3b71b76a29b2fd2ed3b2b579253a604d1e207`.
Working target: systems/reproducibility. An empirical scientometrics paper would
require a different sampling design and annotated scientific-support evidence.

## Candidate contribution and exclusions

Investigate whether versioned, content-addressed impact reports make dependency
assessments easier to verify under incomplete data and projection drift. This
is a candidate contribution to test, not an established advance.

Do not claim that ordinary knowledge graphs cannot traverse dependencies,
withdraw support, or preserve provenance. Truth maintenance, incremental view
maintenance, provenance semirings, PROV, and Trusty URIs are relevant prior art;
see [the bounded source screen](references.json). A credible paper must identify
and measure a useful difference from these approaches, or present the work as a
reproducible implementation/case study without an algorithmic novelty claim.

The current POC establishes runnable software and reproducible computations.
It does not establish scientific falsity, citation intent, causal consequences
of retraction, population prevalence, or superior performance.

## Semantic gate: decide before extending the study

The existing algorithm is a **flat support-exhaustion heuristic**:

- Only `grounds` and `derives` parents count.
- An explicit or in-scope recorded refutation marks a node refuted.
- Missing or unresolved support takes precedence over partial-loss verdicts.
- A nonempty support set is exhausted when every support parent is lost.
- Partial support loss is weakened and does not itself propagate loss.
- A replacement halts expansion, with withheld results reported as incomplete.

This is not yet a general theory of logical entailment. A flat parent list
cannot distinguish a conjunctive justification `A AND B` from alternative
justifications `A OR B`, or represent `(A AND B) OR C`. Losing one necessary
premise can defeat a conjunction even though another premise survives. The
meaning of `derives` must therefore be reconciled with the exhaustion rule.

Before an empirical extension, either formally retain and name the restricted
heuristic, or specify justification groups/hyperedges and their truth tables.
State what loss means: withdrawal of modeled support is not proof of negation.
Do not silently change the existing schema or regenerate the frozen POC.

Define query scope independently of semantics: one hypothetical seed,
closure-local recorded refutations, or simultaneous known refutations. The
current CLI implements the first two. Its asymmetric seed queries must not be
presented as a simultaneous global-refutation result. Define replacement,
reinstatement, conflicting evidence, evidence authority, and as-of time to the
extent required by the selected claim.

## Iteration 1: falsify optimistic interpretations of the POC

Run an offline diagnostic from the pinned source records, sharing inputs and
cycle-cut decisions but not the Rust evaluator or CAS node builder. Compare
per-paper statuses with the frozen CLI reports under the same stated rules.
Use a synchronous fixed-point reference computation and document outside-scope
and unknown-parent handling. This checks implementation/model agreement, not
whether citations constitute genuine scientific support.

Measure co-citation selection and path lengths. Perturb the sole modeled
invalidation's support set by removing a captured edge or adding an unresolved
parent; distinguish these source-level counterfactuals from Rust API tests.
Compare seed-local and simultaneous-refutation scenarios without overwriting
the original result. Preserve exact input hashes and deterministic output.
Report failures and changed verdicts as findings, not reasons to tune the cohort.

Acceptance: no unexplained baseline disagreements; diagnostic output regenerates
exactly; deliberately changed expected output is rejected; all reported controls
have explicit assumptions and their scope of validation is visible.

Measured outcome ([full deterministic evidence](evaluation.json)):

| Diagnostic | Observed result | What it does not establish |
|---|---|---|
| Source-ID fixed point versus frozen Rust reports | 353 of 353 paper statuses agree | Scientific validity or a general correctness proof |
| Co-citation selection | All 175 selected papers cite both seeds directly | Empirical isolation of multihop impact |
| Science query without recorded refutations | 176 downstream unknown, no modeled invalidation | A failure of the opt-in recorded-refutation implementation |
| Sole positive plus one unresolved support reference | `invalidated` becomes `unknown` | That such an extra citation actually exists |
| PNAS query with the Science reference removed from that paper | `unknown` becomes `invalidated` | That the captured reference should be removed |
| Explicit joint seed scope in the source diagnostic | 1 invalidated and 174 unknown among the 175 selected papers | A new simultaneous-seed CLI capability |
| Twelve deterministic DFS root orders | Four cut sets, no source-ID verdict changes here | General cut-order or CID invariance |

The diagnostic's incorrect-expected-verdict control, corrupted-capture check and
altered-output-snapshot check fail as intended. The implementation/model agreement
gate for this frozen cohort is met. The novelty, semantics, scientific-validity,
performance, external-reproduction and release gates remain open.

## Subsequent experiments and acceptance gates

| Gate | Required evidence before claiming it |
|---|---|
| Semantics | Written rules and truth tables; AND/OR ambiguity resolved; explicit seed/time/replacement policy; counterexamples included |
| Correctness | Independent semantics-matched SQL/Datalog or reference evaluator; no unexplained verdict disagreement; property/metamorphic cases and proof obligations |
| Certificate checking | Verifier separate from traversal; check all needed support, not merely a witness path; detect mutations to input bytes, verdicts, paths and omitted resolution evidence |
| Fair comparison | Matched semantics, data and completeness policy; exposure/reachability baseline labeled separately; compare hash-addressed provenance rather than hashes versus an artificially unsigned competitor |
| Robustness | Prespecified missing-edge/node and stale-index controls; multiple seed scopes; replacement and cycle-conversion sensitivity; retain unknowns and failures |
| Empirical validity | Case selection independent of outcome; a genuinely multihop cohort; retraction verification coverage/missingness ledger; temporal/as-of policy |
| Scientific-support claims | Citation contexts or claim-level justifications; a documented annotation rubric, independent human assessment and agreement/uncertainty reporting |
| Performance | Pinned hardware/software, multiple sizes/shapes, repetitions and dispersion; separate build/query/verification time; memory and storage overhead; no superiority claim from one run |
| Reproduction | Offline one-command workflow in a clean environment; dependency/data manifests; another person/team reproduces main claims |
| Release | Data rights/attribution inventory, stable archival package and identifiers, author-approved release, instructions that match the final artifact |

The current cohort selects papers that cite both seeds. Its shortest paths are
therefore direct by construction; it cannot demonstrate empirical multihop
propagation. Its one modeled invalidation is particularly sensitive to the
captured reference set. Retain the cohort as an illustrative case study, not as
an outcome-selected evaluation set or an estimate of downstream scientific harm.

Do not choose a precision/recall threshold after inspecting results. Define an
annotation protocol and primary endpoints first; then report denominators,
uncertainty and failures. If valid support labels cannot be obtained, keep claims
at the systems/model level rather than inventing scientific ground truth.

## Artifact and data release gate

ACM's artifact policy distinguishes a runnable, documented artifact from results
validated by an independent person/team. The current fresh-agent and fresh-store
checks are internal QA, not an ACM badge or independent third-party reproduction.
Use the policy as a checklist; venue requirements and acceptance remain unverified.

OpenAlex metadata is CC0. Crossref distinguishes bibliographic/Retraction Watch
metadata from abstracts whose rights remain with publishers/authors. The frozen
POC includes two abstract fields:

| File under `fixtures/retraction-poc/verified/` | JSON pointer | Characters |
|---|---|---:|
| `crossref-W2026087575-original.json` | `/message/abstract` | 1829 |
| `crossref-W2160675481-original.json` | `/message/abstract` | 1870 |

These fields are not used in the propagation computation. Before publishing a
redistribution artifact, omit them in an explicitly documented metadata projection
or establish their reuse terms. Preserve original response hashes, transformation
records, and derived-file hashes; do not label a transformed file an unchanged
HTTP response or silently overwrite the historical manifest. Check the actual
release package and source history, not only the current generated report.
This inventory identifies unresolved release work, not a legal clearance.

## Proposed next iteration

Review the measured diagnostic and choose the paper's central claim. Resolve
support semantics and seed scope first. Then implement a genuinely independent
matched baseline/certificate verifier and a controlled multihop benchmark.
Expand scientific data or commission annotation only after that protocol is
fixed. Public push, archival deposit, submission, and outreach require explicit
authorization and a reviewable final artifact.

If matched prior-art baselines leave no substantive useful difference, withdraw
the novelty claim and choose an honest technical/software report or a different
research question. A larger dataset or more impressive-looking counts alone do
not resolve that outcome.
