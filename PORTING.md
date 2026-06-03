# Porting the knowledge-substrate discipline to another substrate

This is the portable form of the discipline proven in the **harmonics** corpus
backed by **ket**. It is written so a *different* repository, over a *different*
substrate, can adopt the same anti-drift, content-addressed knowledge practice
without inheriting any ket- or harmonics-specific detail.

`DESIGN.md` says *why* a substrate must be the way it is. This says *how* to
instantiate the discipline over any substrate that meets a small contract.
harmonics + ket are the **reference instantiation**, not the only one.

> The benefit was never perfecting one repo's substrate. It is understanding the
> pattern well enough to hand it to the next one. This file is that hand-off.

---

## 1. The substrate contract

The discipline transfers to any substrate that provides these five capabilities.
Each row is abstract; the ket realization is the worked example, not the
requirement.

| Capability | What it must do | ket realization |
|---|---|---|
| **Address** | Map content → a stable ID that is a function of the content (so equal content ⇒ equal ID, and a changed byte ⇒ a changed ID). | BLAKE3 CID |
| **Seal log** | An **append-only** record of `name → ID` events, last-wins, never rewritten. This is the bridge between a stable *concept name* and its current *content version*. | `.ket/log` |
| **Retrieve + lineage** | Given an ID, return the content; given a concept, return its canonical head and supersession history. | `ket_get`, `ket_lineage` |
| **Projection** | A derived, queryable view that holds **nothing not reconstructible** from Address + Log. | the Dolt `dag_edges`/`dag_nodes` tables |
| **Verify + Rebuild** | *Verify*: re-derive the projection from the addressed content and diff (self-audit). *Rebuild*: replay it (self-heal). | `verify_cas`, `verify_projection` / `rebuild_projection` |

If your substrate offers these, the rest of this file applies unchanged. If it
offers only Address + Log (e.g. plain git over content-hashed files), you get
the audit half; add a Projection only when you need fast queries over the graph.

**The one test that subsumes the contract** (from `DESIGN.md`): *throw away
everything except the addressed content and the append-only log — can you rebuild
every other artifact bit-identically?* A substrate passes the contract iff the
answer is yes.

---

## 2. The discipline (agent operating rules)

These are substrate-agnostic. harmonics ships them as a project `CLAUDE.md`; copy
that file's structure and only refill its "current substrate" section with your
substrate's tool names.

- **Knowledge is a cache of the substrate.** Treat every value, statement, or
  classification you "know" as a cache entry with a *key* (the named concept), a
  *resolution* (how fine a claim), and a *freshness* (read this session, or
  recalled). Never the source of truth.
- **Verify before asserting** when any hold: not read this session; taken on
  assumption; load-bearing (feeds a commit, an edit, a downstream answer); or the
  session snapshot is non-clean. Read at the resolution you intend to assert.
- **Cache what you verify**, then reference the address — don't reproduce content
  from memory. Re-seal on edit before reuse.
- **Stale vs fabricated**: a *stale* entry drifted since you read it; a
  *fabricated* one was never read. Both are silent. The protocol exists to make
  them loud.

---

## 3. The invariants to carry (from `DESIGN.md`)

Substrate-neutral; they are the reason the discipline works.

- **The partition.** Every datum is *content-addressed primary* (audits by
  re-hash, heals by redundancy) or *fully derived* (audits by re-derive-and-diff,
  heals by regeneration). Anything that is neither is the silent-drift class —
  hunt it down.
- **The two-question rule** for admitting any datum: *if I delete this, how is it
  re-derived?* (no answer ⇒ must be primary) and *what do I diff it against?* (no
  answer ⇒ it's a guess, not substrate).
- **Resolution is a logged event, never an overwrite.** Corrections supersede;
  they don't clobber.
- **Document as artifact, prose as projection.** Prose can't self-audit, so it
  can't be the source of record; where you can, make the typed artifact primary
  and render prose from it.
- **Conflict is a typed diagnostic** — epistemic / schema-drift /
  canonicalizer-nondeterminism. The goal is zero *silent* conflict, not zero
  conflict.
- **Push concerns down the manageability ladder**: held-opinion → architecture →
  language → mechanical. Never hold what you can address.

---

## 4. Adoption checklist

1. **Name the corpus.** List the named concepts and their source files. These are
   your keys.
2. **Pick or build a substrate** that meets §1. At minimum: Address + Seal log.
3. **Drop in the discipline.** Copy harmonics' substrate-agnostic `CLAUDE.md`;
   refill only its "current substrate" section with your substrate's retrieve /
   integrity / seal / spine tools. Keep your *domain* registry (your equivalent
   of a scorecard / classification) separate from the substrate mechanics — they
   transfer independently.
4. **Stand up audit tooling**, cheapest-first, each a standalone check with
   `0 = clean / 1 = violation / 2 = env-error`:
   - **integrity** — every sealed entry's content still hashes to its ID;
   - **drift** — working-tree content matches its last-sealed ID (gate a curated
     *spine*; leave the rest *retrieval-tier*, integrity-checked but un-gated);
   - **projection** — the derived view re-derives from the substrate and agrees
     (the `verify`/`rebuild` pair).
   Wire questionable-until-complete checks as **advisory** (non-gating) and report
   a **coverage number**; promote to gating only when coverage reaches 100%.
5. **Make identity content-addressed, not nominal.** Where artifacts reference
   each other, resolve names → IDs through the seal log so the reference graph is
   a *projection of sealed content*, and audit that projection's coverage.
6. **Run the two-question rule** on every new datum, in review and at write time.

---

## 5. Worked reference: harmonics + ket

| Abstract role | Realization |
|---|---|
| Corpus | `sync_cost/derivations/*.md` (physics derivations) |
| Address | BLAKE3 CID under `.ket/cas/` |
| Seal log | `.ket/log` (`put \| <path> -> <cid>`) |
| Retrieve / lineage | `ket_get` / `ket_search` / `ket_lineage` |
| Projection | Dolt `dag_edges` (typed epistemic edges) |
| Verify / Rebuild | `verify_cas`; `verify_projection` / `rebuild_projection` |
| Audit suite | nine `scripts/drift/*.py` checks (integrity, drift, orphans, acyclicity, sealed-projection, …) |
| Reference graph | `docs/derivation-graph.json`, nodes CID-pinned via the seal log |
| Spine vs tier | `scripts/drift/enforced_paths.txt` gates a curated set; the rest is retrieval-tier |

**The honest state is a legitimate adoption point.** harmonics today is ~31%
sealed-projection coverage with the sealed-projection and acyclicity checks
*advisory*, not gating. That is correct for that repo: you do not need 100%
coverage or a gating wall to get the benefit. Coverage is a **ratchet** — a
number you watch go up — not a precondition. Adopt at any coverage; tighten as
the corpus stabilizes.

---

## 6. What transfers, and what does not

- **Transfers:** the contract (§1), the discipline (§2), the invariants (§3), the
  checklist (§4), and the *shape* of the audit suite.
- **Does not transfer — and must not be copied:** the domain vocabulary
  (harmonics' Class 1–5 numerology, K=1 vs K<1 regimes, Survives/Floor/Eliminated
  status, the physics itself). That is the corpus's own meaning, sitting *on* the
  substrate, not *part* of it. A new substrate brings its own domain registry; it
  inherits only the mechanics of keeping that registry honest.

The test of a clean port is the same one this repo's CLAUDE.md was rewritten to
pass: **you can remove every trace of the reference substrate without losing the
discipline.** If your adopted `CLAUDE.md` still reads correctly after you strip
the "current substrate" section, the port is sound.
