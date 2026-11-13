#!/usr/bin/env python3
"""Regression tests for #25. Requires Unix, installed Dolt, and a built ket.

python3 scripts/test_dag_recovery.py --ket /path/to/ket
All fault injection runs in disposable stores; no installed binary is changed.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time
import unittest

SHIM = '''#!/bin/sh
case "$*" in
 *"INSERT IGNORE INTO dag_nodes"*)
  case "$KET_TEST_MODE" in
   block) : > "$KET_TEST_MARK"; exec sleep 120 ;;
   fail) echo 'injected projection failure' >&2; exit 1 ;;
  esac ;;
esac
exec "$KET_TEST_DOLT" "$@"
'''


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix='ket-issue25-')
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.home = self.root / '.ket'
        shim = self.root / 'bin'
        shim.mkdir()
        (shim / 'dolt').write_text(SHIM)
        (shim / 'dolt').chmod(0o755)
        self.env = dict(os.environ, KET_TEST_DOLT=DOLT,
                        KET_TEST_MARK=str(self.root / 'reached'),
                        PATH=str(shim) + os.pathsep + os.environ.get('PATH', ''))
        self.run_ket('init')

    def run_ket(self, *args, mode='', success=True):
        r = subprocess.run([KET, '--home', str(self.home), *args],
                           env=dict(self.env, KET_TEST_MODE=mode),
                           capture_output=True, text=True, timeout=60)
        self.assertEqual(r.returncode == 0, success, (args, r.stdout, r.stderr))
        return r

    def create(self, **kw):
        return json.loads(self.run_ket('--json', 'dag', 'create', 'payload', **kw).stdout)

    def recover(self, cid, **kw):
        return json.loads(self.run_ket('--json', 'dag', 'recover', cid, **kw).stdout)

    def count_nodes(self):
        return len(json.loads(self.run_ket('--json', 'dag', 'ls').stdout))

    def events(self, name):
        parts = [line.split(' | ', 2) for line in (self.home / 'log').read_text().splitlines()]
        return [p[2] for p in parts if len(p) == 3 and p[1] == name]

    def sql(self, statement):
        return subprocess.run([DOLT, 'sql', '-q', statement], cwd=self.home / 'ket.db',
                              capture_output=True, text=True, check=True, timeout=30).stdout

    def test_partial_failure_recovery_and_distinct_new_create(self):
        head = self.sql("SELECT DOLT_HASHOF('HEAD') AS head")
        r = self.create(mode='fail', success=False)
        cid = r['node_cid']
        self.assertEqual((r['status'], r['projection'], r['retained']), ('partial', 'failed', True))
        self.assertEqual(r['recovery']['node_cid'], cid)
        self.assertEqual(self.events('dag:create:intent'), [cid])
        self.assertEqual(self.events('dag:create'), [cid])
        self.run_ket('verify-projection', success=False)
        for _ in range(2):
            self.assertEqual(self.recover(cid)['projection'], 'synced')
        self.assertEqual(self.count_nodes(), 1)
        self.run_ket('verify-projection')
        self.assertEqual(head, self.sql("SELECT DOLT_HASHOF('HEAD') AS head"))
        self.assertNotEqual(self.create()['node_cid'], cid)
        self.assertEqual(self.count_nodes(), 2)

    def test_sigkill_records_exact_cid_before_projection(self):
        p = subprocess.Popen([KET, '--home', str(self.home), '--json', 'dag', 'create', 'payload'],
                             env=dict(self.env, KET_TEST_MODE='block'), start_new_session=True,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 30
            while not Path(self.env['KET_TEST_MARK']).exists():
                self.assertIsNone(p.poll(), 'exited before projection boundary')
                self.assertLess(time.monotonic(), deadline, 'projection boundary not reached')
                time.sleep(.02)
            cid, = self.events('dag:create')
            self.assertEqual(self.events('dag:create:intent'), [cid])
        finally:
            if p.poll() is None:
                os.killpg(p.pid, signal.SIGKILL)
            p.communicate(timeout=10)
        self.assertEqual(p.returncode, -signal.SIGKILL)
        self.run_ket('verify-projection', success=False)
        self.assertEqual(self.recover(cid)['node_cid'], cid)
        self.assertEqual(self.count_nodes(), 1)
        self.run_ket('verify-projection')

    def test_log_failure_before_publication(self):
        (self.home / 'log').unlink()
        (self.home / 'log').mkdir()
        self.run_ket('dag', 'create', 'payload', success=False)
        self.assertEqual(self.count_nodes(), 0)

    def test_recovery_log_failure_is_partial(self):
        cid = self.create()['node_cid']
        (self.home / 'log').unlink()
        (self.home / 'log').mkdir()
        r = self.recover(cid, success=False)
        self.assertEqual((r['status'], r['projection'], r['retained']), ('partial', 'not_attempted', True))
        self.assertEqual(self.count_nodes(), 1)

    def test_configured_db_failure_cannot_be_skipped(self):
        cid = self.create()['node_cid']
        shutil.rmtree(self.home / 'ket.db' / '.dolt')
        self.assertEqual(self.recover(cid, success=False)['projection'], 'failed')

    def test_cas_only_mode(self):
        shutil.rmtree(self.home / 'ket.db')
        r = self.create()
        self.assertEqual(r['projection'], 'not_configured')
        self.assertEqual(self.recover(r['node_cid'])['projection'], 'not_configured')
        self.assertEqual(self.count_nodes(), 1)

    def test_corrupt_node_dependency_and_malformed_cid_rejected(self):
        r = self.create()
        content = self.home / 'cas' / r['content_cid']
        original = content.read_bytes()
        content.write_bytes(b'corrupt')
        self.run_ket('dag', 'recover', r['node_cid'], success=False)
        content.write_bytes(original)
        (self.home / 'cas' / r['node_cid']).write_bytes(b'{}')
        self.run_ket('dag', 'recover', r['node_cid'], success=False)
        self.run_ket('dag', 'recover', '../log', success=False)

    def test_conflicting_sql_row_requires_repair(self):
        cid = self.create()['node_cid']
        self.sql(f"UPDATE dag_nodes SET agent='wrong' WHERE cid='{cid}'")
        r = self.recover(cid, success=False)
        self.assertEqual(r['projection'], 'failed')
        self.assertIn('repair', r['error'])
        self.run_ket('repair')
        self.assertEqual(self.recover(cid)['projection'], 'synced')
        self.assertEqual(self.count_nodes(), 1)
        self.run_ket('verify-projection')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ket', required=True)
    args, remaining = parser.parse_known_args()
    KET = str(Path(args.ket).resolve())
    DOLT = shutil.which('dolt')
    if not DOLT:
        parser.error('installed Dolt required')
    unittest.main(argv=[__file__, *remaining])
