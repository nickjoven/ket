#!/usr/bin/env python3
"""Offline qkl1 schema/examples and reference truth-table checks.

Uses the already-installed jsonschema package; installs nothing. These checks
do not resolve symbolic example CIDs or replace adapter CAS/domain verification.
"""
import copy
import json
from pathlib import Path
import unittest

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parent
URI = 'https://schemas.ket.local/qkl1/'
SCHEMAS = {path.name: json.loads(path.read_text()) for path in ROOT.glob('*.schema.json')}
def local_schema(value):
    """Expand only this contract's shared definitions; never fetch a URI."""
    if isinstance(value, list):
        return [local_schema(item) for item in value]
    if not isinstance(value, dict):
        return value
    if '$ref' in value:
        prefix = URI + 'common.schema.json#/$defs/'
        if not value['$ref'].startswith(prefix) or len(value) != 1:
            raise ValueError('unsupported/nonlocal schema reference')
        return local_schema(SCHEMAS['common.schema.json']['$defs'][value['$ref'][len(prefix):]])
    return {key: local_schema(item) for key, item in value.items()}


def fixture(name):
    return json.loads((ROOT / 'fixtures' / (name + '.json')).read_text())


def validate(name, value):
    Draft202012Validator(local_schema(SCHEMAS[name + '.schema.json'])).validate(value)


def aggregate(verdicts):
    return 'fail' if 'fail' in verdicts else 'unknown' if 'unknown' in verdicts else 'pass'


def check_quod_evidence(manifest, evidence):
    validate('quod-evidence', evidence)
    if evidence['context_cid'] != manifest['context_cid'] or any(
            check['context_cid'] != evidence['context_cid'] for check in evidence['checks']):
        raise ValueError('quod evidence context mismatch')
    if evidence['corpus']['manifest_cid'] != manifest['roles']['corpus_manifest'][0]['cid'] or evidence['corpus']['shard_cids'] != [
            binding['cid'] for binding in manifest['roles']['corpus_shards']]:
        raise ValueError('quod gate is not bound to the consumed corpus')
    if evidence['gate_verdict'] != aggregate([check['verdict'] for check in evidence['checks']]):
        raise ValueError('quod aggregate gate contradicts required checks')


def expected_assessment(manifest, assessment):
    """Reference policy for a verified manifest, not a production assessor.

    Adapters must first hash/parse actual CAS artifacts and evaluate their
    declared synthetic gate. This function tests policy composition only.
    """
    checks = {}
    for check in assessment['inputs']:
        key = (check['role'], check['index'])
        if key in checks:
            raise ValueError('duplicate input check')
        checks[key] = check
    expected = {(role, i): binding for role, bindings in manifest['roles'].items()
                for i, binding in enumerate(bindings)}
    if set(checks) != set(expected):
        raise ValueError('every binding must have exactly one input check')
    for key, binding in expected.items():
        if any(checks[key][field] != binding[field] for field in ['cid', 'node_cid', 'required']):
            raise ValueError('assessment does not bind the manifest input identity')
        if checks[key]['status'] == 'verified' and (binding['cid'] is None or binding['node_cid'] is None):
            raise ValueError('absent binding cannot be verified')
    gates = {gate['gate']: gate for gate in assessment['gates']}
    if len(gates) != 2 or set(gates) != {'quod', 'calibration'}:
        raise ValueError('exactly one quod and one calibration gate required')
    judgments = []
    for name, role in [('quod', 'quod_evidence'), ('calibration', 'calibration')]:
        gate, source = gates[name], expected[(role, 0)]
        valid = (gate['required'] and source['required']
                 and gate['evidence_cid'] == source['cid']
                 and gate['evidence_cid'] is not None
                 and gate['context_cid'] == manifest['context_cid']
                 and gate['integrity'] == checks[(role, 0)]['status'] == 'verified')
        judgments.append(gate['verdict'] if valid else 'unknown')
    domain = aggregate(judgments)
    missing = any(binding['required'] and checks[key]['status'] != 'verified'
                  for key, binding in expected.items())
    changed = (assessment['requested_context'] != manifest['context']
               or assessment['requested_context_cid'] != manifest['context_cid']
               or assessment['policy_cid'] != manifest['policy_cid'])
    eligibility = ('blocked' if domain == 'fail' else
                   'unknown' if missing or domain == 'unknown' else
                   'unknown' if assessment['execution'] != 'completed' else
                   'needs_revalidation' if changed else 'eligible')
    return domain, eligibility, changed


def check_assessment(manifest, assessment):
    validate('run-manifest', manifest)
    validate('assessment', assessment)
    for key in ['run_id', 'logical_run_key']:
        if assessment[key] != manifest[key]:
            raise ValueError('assessment/run identity mismatch')
    if assessment['manifest_policy_cid'] != manifest['policy_cid'] or assessment['original_context_cid'] != manifest['context_cid']:
        raise ValueError('assessment has wrong original context or policy')
    if assessment['execution_evidence_cid'] != manifest['attempt_journal_head_cid']:
        raise ValueError('assessment has wrong execution evidence')
    domain, eligibility, changed = expected_assessment(manifest, assessment)
    if (assessment['domain_gate'], assessment['eligibility'], assessment['context_changed']) != (domain, eligibility, changed):
        raise ValueError('assessment violates required-input policy')
    if eligibility != 'eligible' and not assessment['reasons']:
        raise ValueError('noneligible assessment must explain its reason')


class ContractChecks(unittest.TestCase):
    def test_all_schema_documents_and_positive_examples(self):
        for filename, schema in SCHEMAS.items():
            Draft202012Validator.check_schema(schema)
            if filename != 'common.schema.json':
                name = filename.removesuffix('.schema.json')
                validate(name, fixture(name))

    def test_eligible_example(self):
        check_assessment(fixture('run-manifest'), fixture('assessment'))
        check_quod_evidence(fixture('run-manifest'), fixture('quod-evidence'))

    def test_quod_gate_must_bind_consumed_corpus(self):
        q = fixture('quod-evidence')
        q['corpus']['manifest_cid'] = 'e' * 64
        with self.assertRaisesRegex(ValueError, 'consumed corpus'):
            check_quod_evidence(fixture('run-manifest'), q)

    def test_quod_failed_check_cannot_aggregate_as_pass(self):
        q = fixture('quod-evidence')
        q['checks'][0]['verdict'] = 'fail'
        with self.assertRaisesRegex(ValueError, 'contradicts'):
            check_quod_evidence(fixture('run-manifest'), q)

    def test_policy_precedence_controls(self):
        manifest = fixture('run-manifest')
        cases = [
            ('one_failed_required_gate', 'fail', 'blocked'),
            ('missing_metrics', 'pass', 'unknown'),
            ('corrupt_checkpoint', 'pass', 'unknown'),
            ('unknown_gate', 'unknown', 'unknown'),
            ('changed_split', 'pass', 'needs_revalidation'),
            ('changed_policy', 'pass', 'needs_revalidation'),
            ('interrupted', 'pass', 'unknown'),
            ('failed_gate_with_missing_metrics', 'fail', 'blocked'),
            ('stale_gate_context', 'unknown', 'unknown'),
            ('old_pass_new_context', 'pass', 'needs_revalidation'),
            ('old_fail_new_context', 'fail', 'blocked'),
            ('interrupted_changed_context', 'pass', 'unknown'),
            ('failed_changed_context', 'pass', 'unknown'),
            ('interrupted_changed_context_failed_gate', 'fail', 'blocked'),
            ('failed_changed_context_failed_gate', 'fail', 'blocked'),
        ]
        for name, domain, eligibility in cases:
            with self.subTest(name=name):
                a = fixture('assessment')
                if name in ['one_failed_required_gate', 'failed_gate_with_missing_metrics', 'old_fail_new_context',
                            'interrupted_changed_context_failed_gate', 'failed_changed_context_failed_gate']:
                    a['gates'][0]['verdict'] = 'fail'
                if name in ['missing_metrics', 'failed_gate_with_missing_metrics', 'corrupt_checkpoint']:
                    role = 'checkpoint' if name == 'corrupt_checkpoint' else 'metrics'
                    next(c for c in a['inputs'] if c['role'] == role)['status'] = 'corrupt' if role == 'checkpoint' else 'missing'
                if name == 'unknown_gate':
                    a['gates'][1]['verdict'] = 'unknown'
                if name in ['changed_split', 'old_pass_new_context', 'old_fail_new_context',
                            'interrupted_changed_context', 'failed_changed_context',
                            'interrupted_changed_context_failed_gate', 'failed_changed_context_failed_gate']:
                    a['requested_context']['split_manifest_cid'] = 'e' * 64
                    a['requested_context_cid'] = 'f' * 64
                if name == 'changed_policy':
                    a['policy_cid'] = 'e' * 64
                if name in ['interrupted', 'interrupted_changed_context', 'interrupted_changed_context_failed_gate']:
                    a['execution'] = 'interrupted'
                if name in ['failed_changed_context', 'failed_changed_context_failed_gate']:
                    a['execution'] = 'failed'
                if name == 'stale_gate_context':
                    a['gates'][0]['context_cid'] = 'f' * 64
                    a['gates'][0]['verdict'] = 'fail'
                a['domain_gate'], a['eligibility'], a['context_changed'] = expected_assessment(manifest, a)
                a['reasons'] = [name]
                self.assertEqual((a['domain_gate'], a['eligibility']), (domain, eligibility))
                check_assessment(manifest, a)

    def test_policy_schema_and_fixture_use_the_frozen_precedence(self):
        expected = ['required_gate_fail', 'required_unknown', 'execution_not_completed',
                    'context_or_policy_changed', 'eligible']
        self.assertEqual(fixture('required-input-policy')['precedence'], expected)
        self.assertEqual(SCHEMAS['required-input-policy.schema.json']['properties']['precedence']['const'], expected)
        validate('required-input-policy', fixture('required-input-policy'))

    def test_false_gate_is_not_rescued_by_execution_success(self):
        m, a = fixture('run-manifest'), fixture('assessment')
        a['gates'][0]['verdict'] = 'fail'
        with self.assertRaisesRegex(ValueError, 'violates'):
            check_assessment(m, a)

    def test_missing_required_binding_check_rejected(self):
        m, a = fixture('run-manifest'), fixture('assessment')
        a['inputs'][-1] = copy.deepcopy(a['inputs'][0])
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            check_assessment(m, a)

    def test_gate_evidence_identity_cannot_be_swapped(self):
        m, a = fixture('run-manifest'), fixture('assessment')
        a['gates'][0]['evidence_cid'] = 'f' * 64
        with self.assertRaisesRegex(ValueError, 'violates'):
            check_assessment(m, a)

    def test_null_required_source_cannot_claim_verified(self):
        m, a = fixture('run-manifest'), fixture('assessment')
        m['roles']['metrics'][0].update(cid=None, node_cid=None, unavailable_reason='fixture omitted metrics')
        c = next(c for c in a['inputs'] if c['role'] == 'metrics')
        c.update(cid=None, node_cid=None)
        with self.assertRaisesRegex(ValueError, 'absent'):
            check_assessment(m, a)

    def test_schema_rejects_silent_role_omission(self):
        m = fixture('run-manifest')
        del m['roles']['metrics']
        with self.assertRaises(Exception):
            validate('run-manifest', m)

    def test_required_metric_role_cannot_be_marked_optional(self):
        m = fixture('run-manifest')
        m['roles']['metrics'][0]['required'] = False
        with self.assertRaises(Exception):
            validate('run-manifest', m)

    def test_execution_evidence_cannot_be_swapped(self):
        m, a = fixture('run-manifest'), fixture('assessment')
        a['execution_evidence_cid'] = 'e' * 64
        with self.assertRaisesRegex(ValueError, 'execution evidence'):
            check_assessment(m, a)

    def test_schema_requires_unavailable_reason(self):
        m = fixture('run-manifest')
        m['roles']['metrics'][0]['cid'] = None
        with self.assertRaises(Exception):
            validate('run-manifest', m)

    def test_corrupt_hex_identity_rejected(self):
        m = fixture('run-manifest')
        m['policy_cid'] = '../untrusted'
        with self.assertRaises(Exception):
            validate('run-manifest', m)

    def test_git_sha_is_not_a_dolt_snapshot_hash(self):
        r = fixture('projection-receipt')
        r['dolt_commit'] = 'a' * 40
        with self.assertRaises(Exception):
            validate('projection-receipt', r)

    def test_ack_requires_retained_outcome(self):
        event = fixture('attempt-event')
        event['event'] = 'acknowledged'
        with self.assertRaises(Exception):
            validate('attempt-event', event)

    def test_fixture_cannot_be_promoted_to_real_domain_evidence(self):
        q = fixture('quod-evidence')
        q['fixture'] = False
        with self.assertRaises(Exception):
            validate('quod-evidence', q)


if __name__ == '__main__':
    unittest.main(verbosity=2)
