"""AVG-09: the test-only AfterCommitFault wrapper itself (pure Python)."""

import shutil
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from coin_average import batch_writer
from fault_injection import AfterCommitFault, InjectedFault
from test_coin_average import FakeBatch, FakeConnection, exchange


class AfterCommitFaultTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix='avg09-fault-'))
        self.record = str(self.tmp / 'faults.jsonl')
        self.connections = []

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def connect(self):
        self.connections.append(FakeConnection())
        return self.connections[-1]

    def writer(self, **options):
        fault = AfterCommitFault(self.record, **options)
        return fault, fault.wrap(batch_writer(fault.watch(self.connect)))

    def events(self, fault):
        return [(r['batch_id'], r['event']) for r in fault.records()]

    def test_fails_after_the_nth_commit_once_even_across_restarts(self):
        rows = FakeBatch([exchange('UPBIT', 1, '1')])
        fault, write = self.writer(fail_after=2)
        write(FakeBatch([]), 0)  # nothing to write: no commit, not counted
        write(rows, 1)
        with self.assertRaisesRegex(InjectedFault, 'batch 2'):
            write(rows, 2)
        self.assertEqual([c.events[-2:] for c in self.connections], [['commit', 'close']] * 2)
        _, restarted = self.writer(fail_after=2)  # new process, same record file
        restarted(rows, 2)
        restarted(rows, 3)
        self.assertEqual(self.events(fault), [
            (0, 'start'), (1, 'start'), (1, 'committed'), (2, 'start'), (2, 'committed'), (2, 'inject'),
            (2, 'start'), (2, 'committed'), (3, 'start'), (3, 'committed'),
        ])

    def test_a_later_fail_after_value_injects_again_on_the_same_record(self):
        rows = FakeBatch([exchange('UPBIT', 1, '1')])
        _, first = self.writer(fail_after=1)
        with self.assertRaises(InjectedFault):
            first(rows, 0)
        fault, second = self.writer(fail_after=3)
        second(rows, 1)
        with self.assertRaises(InjectedFault):
            second(rows, 2)
        self.assertEqual([(r['batch_id'], r['fail_after']) for r in fault.records() if r['event'] == 'inject'],
                         [(0, 1), (2, 3)])

    def test_committed_record_lists_the_upserted_keys(self):
        fault, write = self.writer()
        write(FakeBatch([exchange('UPBIT', 1, '1'), exchange('UPBIT', 1, '2', coin='ETH')]), 0)
        committed, = [r for r in fault.records() if r['event'] == 'committed']
        self.assertEqual(committed['keys'], [['BTC', 1793318460000], ['ETH', 1793318460000]])
        self.assertIsInstance(committed['wall_ms'], int)

    def test_failed_write_is_not_recorded_as_committed(self):
        fault, write = self.writer(fail_after=1)
        with self.assertRaises(ValueError):
            write(FakeBatch([exchange('UPBIT', 0, '1')]), 0)
        self.assertEqual(self.events(fault), [(0, 'start')])

    def test_pause_mode_waits_instead_of_raising(self):
        fault, write = self.writer(fail_after=1, mode='pause', pause_seconds=0)
        write(FakeBatch([exchange('UPBIT', 1, '1')]), 5)
        self.assertEqual(self.events(fault), [(5, 'start'), (5, 'committed'), (5, 'inject')])


if __name__ == '__main__':
    unittest.main()
