# qkl1 interface contract, version 1

Status: coordinator-review candidate. This commit provides the complete interface;
the coordinator's bus freeze identifies the adopted commit. No code in this
directory starts Lean, numerical solvers, training, services or network access.

## Inventory and migration

All schemas use JSON Schema 2020-12, integer `schema_version: 1`, closed objects,
and the `qkl.* /v1` names listed below (without the space). Schema identifiers
under `https://schemas.ket.local/qkl1/` are identifiers, not HTTP dependencies.
Resolve them from the files supplied with this contract.

| Schema | Producer / consumer |
|---|---|
| `input-manifest.schema.json` | Semantic input identity before execution |
| `run-manifest.schema.json` | Lemma adapter's sealed run and all role bindings |
| `quod-evidence.schema.json` | Quod adapter's **synthetic** gate fixture |
| `calibration.schema.json` | Lemma adapter's synthetic known-answer gate |
| `assessment.schema.json` | New contextual eligibility judgment, never an overwrite |
| `attempt-event.schema.json` | Durable fixture execution/journal boundary |
| `projection-receipt.schema.json` | Separate CAS receipt after explicit SQL checkpoint |
| `required-input-policy.schema.json` | Exact pilot composition policy |
| `common.schema.json` | Shared CID, context, binding and check definitions |

`fixtures/*.json` are exact shape examples. Their repeated-digit CIDs and Dolt
hash are **symbolic** and are not assertions that artifacts exist. Adapters must
create real CAS artifacts and verify real hashes. A schema test does not perform
CAS, gate, filesystem durability or SQL verification. Every v1 document has
`fixture: true`; general real-domain admission requires a separately reviewed
contract. This pilot must not present a simulated pass as a Lean proof.

No earlier qkl manifest was released. V1 changes no existing ket24 schemas or
lemma ledger files. Additive optional fields need documented defaults; removing,
renaming or changing the meaning of a required field requires a new version and
explicit migration. Do not silently reinterpret archived manifests.

## Canonical bytes and identities

For this Python-only fixture, encode JSON as UTF-8 using
`json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
allow_nan=False)` with **no trailing newline**. Store exact bytes with `ket put`.
All CAS CIDs are lowercase 64-character BLAKE3 hex strings. This encoding is a
pilot convention, not a claim to implement all of RFC 8785. Non-JSON model
checkpoints remain opaque byte artifacts; never deserialize an untrusted pickle.

Publish a `qkl.input-manifest/v1` first. It contains code, policy, environment,
context and semantic input roles, excluding the output roles `metrics` and
`calibration`. Its CID is the `logical_run_key`. A run ID names the human-facing
logical run but does not replace that key. Same code and pinned inputs have the
same semantic identity; attempt IDs and durations are operational history.
Every attempt event's `input_manifest_cid` equals its `logical_run_key`; it
does not point to the later completed run manifest.
Create each step key by putting canonical
`{"schema_version":1,"logical_run_key":K,"step":STEP,"code_cid":C}`,
where `STEP` is one of `quod_fixture`, `evaluate`, `calibrate`, `seal`, `project`.
Record that returned CID as `step_key`. These objects are simple step-key records,
not function-result memoization without verified input identities.

The context contains exact `mathlib_pin`, tokenizer, split manifest, corpus
manifest and gate implementation CIDs. Store its canonical bytes before the
manifest. `context_cid` must resolve to exactly that context, and code/environment
roles must agree with the input manifest's fields. `policy_cid` resolves to a
`qkl.required-input-policy/v1` document. Recompute these identities at assessment;
matching labels or Git SHAs are not a substitute for verified CAS bytes.

## Role bindings and DAG publication

The run manifest's `roles` has all eleven named keys. Values are arrays:
`corpus_shards` accepts one or more entries; other roles require exactly one.
Each entry has `cid`, `node_cid`, `required`, `kind`, `unavailable_reason`.
For the first six-row fixture all eleven roles are required and every binding
has `required: true`; the policy lists those roles explicitly. Optional-role
policy is outside this freeze. Use a disclosed synthetic artifact for a fixture
checkpoint/tokenizer, not a null input silently treated as optional.
`kind` describes the input CID (`blob` or `dag_node`). Null/unavailable roles
remain declared with a reason; an empty array or omitted required role is not
a way to evade eligibility checking. The source producer's run ID and source
pin belong in its verified content rather than being invented by the consumer.

Known raw blobs are wrapped in deterministic DAG nodes. Put this exact shape
using canonical JSON bytes (do not call wall-clock `ket dag create` on retry):

```json
{"agent":"qkl1","kind":"memory","meta":[["input_kind","blob"],["role","config"],["role_index","0"]],"output_cid":"<input CID>","parents":[],"timestamp":"2026-11-04T00:00:00Z"}
```

For a `dag_node` input, use `input_kind=dag_node`, `parents=[input CID]` and
`parent_kinds=["grounds"]`; the wrapper's output still points to the exact input
bytes. Its role/index metadata makes the link typed and inspectable. Verify the
input hash, wrapper hash, wrapper output and role metadata before trusting it.
Missing expected input bytes may be recorded with a null wrapper; they cannot
be called verified. Source blobs may have multiple role wrappers, each derived
from deterministic metadata. Lexically sort metadata pairs.

After wrappers exist, seal the run manifest content, then put a deterministic
run DAG node with `kind=score`, `agent=qkl1`, output equal to the manifest CID,
parents equal to all available wrapper CIDs sorted/deduplicated, all kinds
`grounds`, fixed timestamp above, and `meta=[["role","run_manifest"]]`.
An assessment similarly points to its assessment content and has the manifest
node as a `grounds` parent, `kind=score`, `agent=qkl1`, fixed timestamp, and
`meta=[["role","assessment"]]`. Return its content and node CIDs separately.
Do not put a node's own CID in its content. Requiredness comes from the manifest
and policy, **not** from the grounds edge kind or ket24's exhaustion heuristic.

Adapter call interface (keyword-only operational paths may be `str` or `Path`):

```python
seal_manifest(manifest, *, ket_executable, ket_home)
# -> {"manifest_cid": CID, "manifest_node_cid": CID}
assess(manifest_cid, *, requested_context, requested_policy_cid,
       execution, ket_executable, ket_home)
# -> {"assessment_cid": CID, "assessment_node_cid": CID, "assessment": OBJECT}
```

`manifest_cid` names the JSON content, not its DAG node. Resolve/verify the node
by sealed receipt or exact deterministic reconstruction; never select a node
by a timestamp/most-recent heuristic. Module names remain the assigned paths:
lemma `src/lemma/run_manifest.py`; quod `tools/ket_pilot/` supplies the producer
fixture and may expose its own CLI around `qkl.quod-evidence/v1`. The shared
interface between repos is the schema and these sealed CIDs, not a Torch import.

## Required-input and context truth table

Every manifest binding has exactly one assessment check identified by `(role,
index)`. The check must repeat that binding's CID, wrapper and required flag.
Required `quod_evidence` and `calibration` inputs each supply exactly one gate.
The assessor verifies actual bytes and the declared fixture's known-answer
computation; reading an untrusted string `gate_verdict=pass` is insufficient.
Quod checks' contexts must match their emitted evidence context. A calibration
must bind the metric/known-answer computation used by this fixture.
Quod evidence explicitly names `corpus.manifest_cid` and ordered
`corpus.shard_cids`; these must match the corpus actually consumed in the run
manifest. Emit shard bytes and their corpus manifest before the gate-evidence
receipt, so neither must refer to a future evidence CID. The receipt then binds
that corpus and its gate checks without a hash cycle. Aggregate its required
checks with fail/unknown/pass precedence; contradictory aggregate labels are
invalid. A gate for some different corpus cannot validate this run.

Gate assertions count only when their evidence CID matches the declared role,
its bytes and wrapper are verified, and its context matches the manifest's
**original** context. A wrong-context or unverifiable assertion is `unknown`,
including a wrong-context string that says `fail`. Aggregate applicable gates:
any fail -> domain `fail`; otherwise any unknown -> `unknown`; otherwise `pass`.

Apply the following **ordered** eligibility rules:

| First applicable condition | Eligibility |
|---|---|
| Any verified, applicable required gate fails | `blocked` |
| Any required input is missing/corrupt/unverified, or applicable gate unknown | `unknown` |
| Requested context or policy differs from the manifest's | `needs_revalidation` |
| Execution is not completed | `unknown` |
| All preceding conditions cleared | `eligible` |

An old **pass** does not become a pass for a new gate implementation, corpus,
split or library pin: preserve the old domain judgment and emit
`needs_revalidation`. An applicable old **failure** remains terminal for that
historical failed run, including after an operator asks about a new context;
asking a new question cannot rescue failed evidence. A genuinely new evaluation
has new inputs/context/run evidence and receives its own assessment. A gate
whose evidence never matched the original run is instead unknown, not a valid
old failure. These cases are covered by the reference truth-table checks.

Keep four axes explicit: execution, input integrity, original-context domain
gate and contextual eligibility. Retain metric values and historical assessments
unchanged. A pin change is not a proof that an old theorem was false, and a
model score is not an authoritative replacement for quod's gates.

## Durable attempts and crash windows

The fixture's authoritative attempt journal must publish a durable `started`
event before a side-effect attempt. Sequence and previous-event CID describe
the retained history; a missing/altered event, source or input commitment stops
recovery. Use one writer and the existing adapter's persistence guarantees.

The sealed run's `attempt_journal_head_cid` names its retained execution
evidence; an assessment repeats it as `execution_evidence_cid`. A completed
execution needs a verified acknowledged outcome in that history. Check that
the outcome binds the metric/calibration CIDs being sealed. Do not trust the
`execution` function argument or manifest string without that evidence.
The journal refers to the input manifest and output artifacts, never to a
future run manifest; the final manifest can therefore bind the journal head
without a circular identity. A provider effect with uncertain completion has
no acknowledged result until it is reconciled.

1. After start but before outcome: preserve interrupted state. With no effect
   attempted, a new attempt may compute; otherwise check/reconcile effect state.
2. After result bytes survive but before acknowledgement: recover the retained
   outcome CID, verify identity and reuse it without invoking the function again.
   Verify stored bytes against the CID; preserve orphan/temporary evidence for
   inspection, and never silently delete or claim completion from a partial file.
3. After CAS publication before SQL projection: reconstruct exact nodes from
   fixed bytes and publish idempotently. A successful CLI exit is insufficient:
   projection can fail with a warning. Call `verify-projection` and reconcile.

Wall-clock `ket dag create` is not idempotent because its timestamp affects the
CID. An adapter may use a durable saved outcome identity or the fixed canonical
nodes above, but must not mint a new timestamped node on every retry.
Terminal fixture/calibration/domain failures remain failures after restart.
Adapter-specific BDF/GPU state is outside this pilot and must not be approximated
by replaying just a wrapper function. No live-run fault injection is authorized.

Fake provider interface for recovery review:

```python
FakeProvider(store_dir, idempotency: bool)
send(request_id, idempotency_key_or_none, payload) -> effect_id
reconcile(request_id) -> {"status": "found"|"absent", "effect_id": str|None}
```

Log every request separately from every logical effect. Hooks are `before_send`
and `after_effect_before_reply`. With idempotency enabled, repeated same-key
requests yield one logical effect; changing payload under the same key fails.
Without provider idempotency, unknown completion becomes
`reconciliation_required`; never retry blindly or automatically call it success.
Reconciliation found -> retain/verify the effect and its outcome; absent -> a new
explicit attempt may execute. Record all transitions. This is no claim of
exactly-once execution in arbitrary external systems.

## Versioned SQL receipt

CAS artifacts precede SQL publication. The projection must contain the expected
manifest node/output CID and wrapper-parent edges. Verify, explicitly create a
Dolt history checkpoint, read the actual commit hash and query that snapshot.
The receipt names that commit and CAS verification evidence. A transaction
`COMMIT` or successful `dag create` is not a Dolt checkpoint. The hash32 format
in this pilot is a 32-character Dolt reference, not a Git SHA or CAS CID.

The separate receipt avoids a hash cycle: do not rewrite the run manifest to
include a commit that already contains that manifest CID. One receipt can be a
new immutable CAS artifact with run/verification parent wrappers. A later SQL
repair preserves prior AS OF snapshots and creates new operational evidence.
Use tested table-scoped `SELECT ... FROM dag_nodes AS OF '<hash>'` syntax;
do not append AS OF blindly after a WHERE/ORDER BY clause. Observation/commit
times and domain effective dates remain separate. Existing SQLite recovery
journals and durable solver adapters stay authoritative for their own state.

## Checks, boundaries and rollback

Run `python3 docs/durable-pilot/check_contract.py`. It requires the installed
`jsonschema` package, performs no installation/network/compute,
validates all schema/examples and checks policy negatives. Structural examples
carry symbolic IDs; adapters/recovery tests still must prove real CAS retention,
idempotent publication, source drift rejection, SQL snapshots and resumed
semantic results. Fresh-agent review is internal QA, not external validation.

This change is docs/schemas/fixture checks only. No old metric, corpus, model,
registration, live checkpoint, service, core ket source or lemma ledger changes.
Rollback is abandoning/reverting this task's local commit, never editing prior
evidence. No push, release or target execution is authorized by this contract.
