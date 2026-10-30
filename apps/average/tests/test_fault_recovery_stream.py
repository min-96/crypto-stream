"""AVG-09: failure after MySQL COMMIT → restart on the same checkpoint → same batch re-run.

Uses the production start_query with the test-only AfterCommitFault wrapper and a recording
JDBC connection. Needs pyspark; run inside the Spark image with ../run-spark-tests.sh.
"""

import shutil
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

try:
    from pyspark.sql import SparkSession
except ImportError:  # pragma: no cover - host Python has no pyspark
    SparkSession = None

if SparkSession is not None:
    from coin_average import batch_writer
    from stream_to_mysql import start_query
    from ticker_input import FIELDS

from fault_injection import AfterCommitFault
from test_coin_average import FakeConnection
from test_exchange_window import END_MS, MINUTE_MS, ticker


@unittest.skipIf(SparkSession is None, 'pyspark not available')
class AfterCommitRecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[1]').appName('avg09-recovery-test')
                     .config('spark.sql.session.timeZone', 'Asia/Seoul')
                     .config('spark.ui.enabled', 'false')
                     .config('spark.sql.shuffle.partitions', '1').getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix='avg09-'))
        self.source = self.tmp / 'source'
        self.source.mkdir()
        self.files = 0
        self.connections = []

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def connect(self):
        self.connections.append(FakeConnection())
        return self.connections[-1]

    def start(self, fault):
        """A new query on the same source and checkpoint, like a restarted Spark process."""
        stream = self.spark.readStream.option('maxFilesPerTrigger', 1).text(str(self.source))
        return start_query(stream, fault.wrap(batch_writer(fault.watch(self.connect))),
                           str(self.tmp / 'checkpoint'))

    def add_batch(self, *lines):
        self.files += 1
        staging = self.tmp / f'{self.files:04d}.json'
        staging.write_text('\n'.join(lines) + '\n')
        staging.rename(self.source / staging.name)

    def committed(self, fault):
        return [r['batch_id'] for r in fault.records() if r['event'] == 'committed']

    def test_exchanges_from_separate_batches_survive_a_failure_after_commit(self):
        fault = AfterCommitFault(str(self.tmp / 'faults.jsonl'), fail_after=1)
        query = self.start(fault)
        try:
            # One window [MINUTE_MS, END_MS), each exchange's data in a different micro-batch.
            self.add_batch(ticker(MINUTE_MS + 1000, value='100', market='UPBIT'))
            query.processAllAvailable()
            self.add_batch(ticker(MINUTE_MS + 2000, value='110', market='BITHUMB'))
            query.processAllAvailable()
            self.add_batch(ticker(MINUTE_MS + 3000, value='102', market='UPBIT'))
            query.processAllAvailable()
            self.assertEqual(self.connections, [])  # window still open
            self.add_batch(ticker(END_MS + 10000, value='100', market='UPBIT'))  # watermark reaches END_MS
            with self.assertRaisesRegex(Exception, 'injected failure after COMMIT'):
                query.processAllAvailable()
        finally:
            query.stop()

        injected, = [r['batch_id'] for r in fault.records() if r['event'] == 'inject']
        self.assertEqual(self.committed(fault), [injected])
        expected = [('BTC', END_MS, *['105.500000000000000000'] * len(FIELDS))]  # (101 + 110) / 2
        first, = self.connections
        self.assertEqual([p for _, p in first.executed], expected)
        self.assertEqual(first.events[-2:], ['commit', 'close'])

        restarted = self.start(AfterCommitFault(str(self.tmp / 'faults.jsonl'), fail_after=1))
        try:
            restarted.processAllAvailable()
        finally:
            restarted.stop()
        # Spark's commit log never recorded the batch, so the same batch_id runs again and
        # UPSERTs the same (name, time) with the same values; the fault is not re-injected.
        self.assertEqual(self.committed(fault), [injected, injected])
        self.assertEqual(len(self.connections), 2)
        self.assertEqual([p for _, p in self.connections[1].executed], expected)

    def test_record_only_mode_never_fails(self):
        fault = AfterCommitFault(str(self.tmp / 'faults.jsonl'))
        query = self.start(fault)
        try:
            self.add_batch(ticker(MINUTE_MS, value='1'))
            query.processAllAvailable()
            self.add_batch(ticker(END_MS + 10000, value='1'))
            query.processAllAvailable()
        finally:
            query.stop()
        self.assertEqual(len(self.committed(fault)), 1)
        self.assertFalse(any(r['event'] == 'inject' for r in fault.records()))


if __name__ == '__main__':
    unittest.main()
