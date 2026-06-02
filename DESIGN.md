# ket — design north-star

This note states the invariants ket is *for*. It refines the architecture in
[*A Content-Addressed Adaptive Knowledge Substrate for Distributed Epistemic
Coordination*](https://github.com/nickjoven/jfk-dsa/blob/main/joven_knowledge_substrate.md)
(Joven, 2026) into a single decision rule and a small set of tests that any
change to the substrate must pass.

It is also the first artifact subject to its own discipline: written once,
sealed, and **superseded rather than re-argued in context**.

## The test

> Throw away everything except the CAS blobs and the append-only log. Can you
> rebuild every other artifact — the SQL projection, the graph, the indexes —
> **bit-identically**?

If yes, the system is self-healing and self-auditing *by construction*. If no,
whatever you couldn't rebuild is the liability. Every design question reduces to
keeping the answer **yes**.

This operationalizes two properties:

- **Self-audit** — identity is a function of content, so wrongness is detectable
  without an oracle. Primary data audits by *re-hash* (`verify_cas`); derived
  data audits by *re-derive-and-diff*.
- **Self-heal** — derived data is regenerable from the authoritative source
  below it. (Authored/primary data does not regenerate; it heals by redundancy
  and audits by hash. Only *derived* layers self-heal.)

## The partition

Every datum is exactly one of two things. Anything that is neither is the
silent-drift class — the failure mode this substrate exists to prevent.

| | Audits by | Heals by | |
|---|---|---|---|
| **Content-addressed primary** | re-hash | redundancy | ✅ |
| **Fully derived** | re-derive + diff | regeneration | ✅ |
| Primary but **not** content-addressed | *nothing* | *nothing* | ☠️ |
| Derived but **not** regenerable from the sealed source | *fake* | *fake* | ☠️ |

The bottom two rows look authoritative and have no integrity anchor. The
project's incident history (SHA-256/BLAKE3 corruption; the `weinberg_angle`
misclassification in a downstream consumer) is bottom-left.

## The two-question rule

Before any datum enters the system, answer both:

1. **If I delete this, how is it re-derived?** No answer → it must be
   content-addressed primary, not a mutable cell.
2. **What do I diff it against to know it's right?** No answer → it isn't
   substrate yet, it's a guess.

Fail either and the **design** is wrong, not the data. Convenience-of-write is
uncorrelated with reconstructibility; choosing a home by where it's easy to
write is how antithetical artifacts get made.

## Layer roles

| Layer | Crate(s) | Owns | Invariant |
|---|---|---|---|
| L0 Content | `ket-cas` | bytes ↔ CID | CID = BLAKE3(bytes) |
| L1 Structure | `ket-dag` | provenance graph (DagNodes are themselves CAS blobs) | structure is content-addressed |
| L2 Epistemics | *(see open choice)* | meaning over structure: edge kind, saturation, status | one identity discipline |
| L3 Projection | `ket-sql` / Dolt | queryable views | **holds nothing not reconstructible from L0–L2** |
| L4 Surface | `ket-mcp`, consumers | the verbs agents call | thin; no concern starts here |
| L5 Domain | downstream repos | documents and domain semantics | substrate-agnostic |

The **append-only log** is the source of truth for *events*. The **CAS/DAG** is
the addressed substrate. **Dolt is a projection only** — written *from* the
substrate, never written *to* as truth. The projection owes two operations it
does not yet have:

- `verify_projection` — replay L1/L2 → rebuild the SQL tables → diff. The L3
  mirror of `verify_cas` (self-audit).
- `rebuild_projection` — replay → Dolt (self-heal). Once it exists, losing Dolt
  is a non-event and a tampered Dolt is detectable.

## Resolution is a logged event, never an overwrite

The log is append-only and multi-voice, so it can hold two assertions about the
same object. Which is canonical "now" is a **fold** over the log, not a storage
fact. Storage is solved; the fold is the residual freedom — and its *shape* is
constrained: resolution must be an **explicit, content-addressed, reversible
canonicalization event** (itself a DAG node, with an agent and provenance),
never a silent overwrite. The only genuine choice left is the *policy* that
proposes those events (last-writer / evidence-weighted / vote / lattice-merge).

`INSERT IGNORE`-style write-once edges are a violation of this: they make
correction impossible and have no supersession path. An edge's kind must be
correctable through the same lineage machinery as everything else.

## Document as artifact, prose as projection

Prose has no canonical form, so its byte-CID is meaningless as a meaning-address;
its semantics must be *inferred*; and you cannot `verify` that one paragraph
grounds another. Run that through the test and **prose fails it** — therefore
prose cannot be the source of record. The source is the **machine artifact**
(typed, schema-validated, canonicalizable — `ket-cdom` + the schema tools);
prose is a *rendering*, exactly as Dolt is a rendering for querying.

This does not eliminate semantic ambiguity — it **relocates** it, out of
per-document interpretation (a large, smeared, re-inferred surface) and into
**schema design** (a small, explicit, versioned surface). That is the correct
trade: ambiguity should live at one deliberate, supersedable boundary.

With a *really* deterministic canonicalizer, the CID addresses **meaning**, not
encoding: cosmetic edits leave the CID fixed; a semantic change *is* a CID change
*is* a log event. "Did the meaning drift?" becomes a mechanical check.

## The manageability ladder

Where a problem lives determines how much must be *held* and how its failures
recover:

| Lives in | Held by | Fails by | Recovery |
|---|---|---|---|
| **Held opinion / context** | whoever has context now | forgetting | none (silent, dies at compaction) |
| **Architecture** | the whole system | local violation | expensive |
| **Language / schema** | one versioned artifact | schema drift | re-version, re-canonicalize |
| **Mechanical** | no one | bug | fix the function, recompute |

Push concerns *down* this ladder. The move is content-addressing applied to
judgment: content-addressing took "which version?" out of held convention and
made it a hash; the same move takes "what does this mean / what does it ground?"
out of held opinion and makes it canonical form + typed edges. **Never hold what
you can address.** The leverage point is the line between *deciding* (designing
the schema — bounded, one-time, versioned) and *computing* (canonicalize,
type-check, detect conflict — no deciding). Shove as much as possible across that
line.

## Conflict is a typed diagnostic

With append-only writes (no storage conflict), deterministic canonicalization
(no encoding conflict), and typed edges (no inference conflict), a residual
conflict can only be one of three things — and its *kind localizes the failing
layer*:

| Conflict shape | What failed | Severity |
|---|---|---|
| Two voices, same canonical CID, contradicting claim/edge-type | **Epistemic** — genuine disagreement, surfaced honestly | Healthy — the system is working |
| Dissolves under schema-version alignment | **Ontology / schema drift** | Expected — fix at the one place trouble is meant to live |
| Things that *should* canonicalize identically hash differently | **Canonicalizer non-determinism** — self-audit itself is broken | Highest — substrate bug |

The goal was never zero conflict — it was zero **silent** conflict. Drift is
disagreement that *couldn't surface* because there was no canonical object to
disagree about. The real failure is a system with **no way to represent** "two
voices disagree about CID X." The absence of the conflict channel is the bug;
expected disagreement is the substrate finally telling the truth.

## Recursive closure

The schema/language is the concentrated residue of opinion — the one thing
someone must still hold, and therefore the highest-stakes, most ossification-prone
artifact in the system. The temptation after mechanizing everything is to treat
the schema as settled. That is the next drift. So the discipline applies
**recursively to the language itself**: a schema is just another CAS blob —
logged, superseded, conflict-typed, projection-rendered — like every other
artifact. No layer escapes, including the one that defines the layers.

## What this implies for the current code (open targets)

These are design targets, not yet-done work. Live improvements are incremental
and must not be mistaken for the endpoint.

- **Declare Dolt projection-only** and add `verify_projection` / `rebuild_projection`.
  Today `edge_kind` is written into `dag_edges` as a primary value with no CAS
  source — bottom-left of the partition. (See the k-stack `ket_store` and
  `ket_store_reasoning` write paths; they currently feed the projection, which is
  fine *as a projection writer* and wrong *as the source*.)
- **Decide L2 identity for edges** — edge kind as in-node typed parents
  (`Vec<(Cid, EdgeKind)>`, consistent with how `saturation`/`activation` are kept
  in the node) **vs.** edge kind as a separate content-addressed annotation node.
  Both pass the test; the SQL-column-as-source does not. This is the one open
  design choice; everything else follows from it.
- **Project graphs from sealed blobs**, CID-pinned via the log, not by regex over
  a mutable working tree. A graph derived from the tree heals to "whatever the
  tree says," which is a fake heal.
