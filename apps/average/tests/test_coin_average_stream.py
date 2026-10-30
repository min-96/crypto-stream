"""AVG-08: AVG-07 stream → foreachBatch second stage, with a recording JDBC connection.

Needs pyspark; run inside the Spark image with ../run-spark-tests.sh.
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
    from exchange_window import aggregate_exchange_window
    from ticker_input import FIELDS, parse_preprocessed_ticker

from test_coin_average import FakeConnection
from test_exchange_window import END_MS, MINUTE_MS, ticker


@unittest.skipIf(SparkSession is None, 'pyspark not available')
class CoinAverageStreamTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[1]').appName('avg08-test')
                     .config('spark.sql.session.timeZone', 'Asia/Seoul')
                     .config('spark.ui.enabled', 'false')
                     .config('spark.sql.shuffle.partitions', '1').getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix='avg08-'))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_stream(self, batches, run='run'):
        """Feed one file per micro-batch through AVG-07 and the AVG-08 writer; return the connections."""
        source = self.tmp / run / 'source'
        source.mkdir(parents=True)
        connections = []

        def connect():
            connections.append(FakeConnection())
            return connections[-1]

        stream = self.spark.readStream.option('maxFilesPerTrigger', 1).text(str(source))
        query = (aggregate_exchange_window(parse_preprocessed_ticker(stream)).writeStream
                 .outputMode('append').foreachBatch(batch_writer(connect))
                 .option('checkpointLocation', str(self.tmp / run / 'checkpoint')).start())
        try:
            for number, lines in enumerate(batches, 1):
                path = source / f'{number:04d}.json'
                staging = self.tmp / run / path.name
                staging.write_text('\n'.join(lines) + '\n')
                staging.rename(path)
                query.processAllAvailable()
        finally:
            query.stop()
        return connections

    def written(self, connections):
        return [params for connection in connections for _, params in connection.executed]

    def test_closed_window_becomes_one_upsert_per_coin(self):
        batches = [
            [ticker(MINUTE_MS + i, value='100') for i in range(100)]
            + [ticker(MINUTE_MS + 5, value='110', market='BITHUMB'),
               ticker(MINUTE_MS + 1, value='100', coin='ETH'), ticker(MINUTE_MS + 2, value='100', coin='ETH'),
               ticker(MINUTE_MS + 3, value='101', coin='ETH')],
            [ticker(END_MS + 9999)],  # watermark still before END_MS: nothing to write
            [ticker(END_MS + 10000)],
        ]
        connections = self.run_stream(batches)
        self.assertEqual(len(connections), 1)
        self.assertEqual(self.written(connections), [
            ('BTC', END_MS, *['105.000000000000000000'] * len(FIELDS)),
            ('ETH', END_MS, *['100.333333333333333333'] * len(FIELDS)),
        ])
        self.assertEqual(connections[0].events[-2:], ['commit', 'close'])

    def test_more_than_16_integer_digits_survive_the_stream(self):
        big = '12345678901234567890.123456789012345678'
        connections = self.run_stream([[ticker(MINUTE_MS, trade_price=big), ticker(MINUTE_MS + 1, trade_price=big),
                                        ticker(MINUTE_MS + 2, trade_price='-' + big)],
                                       [ticker(END_MS + 10000)]])
        params, = self.written(connections)
        self.assertEqual(params[2 + FIELDS.index('trade_price')], '4115226300411522630.041152263004115226')

    def test_rerunning_the_same_input_writes_the_same_keys_and_values(self):
        batches = [[ticker(MINUTE_MS + i, value=f'{i}.123456789012345678', market=market)
                    for i in range(7) for market in ('UPBIT', 'BITHUMB')],
                   [ticker(END_MS + 3, value='3', coin='XRP')],
                   [ticker(END_MS + 70000)]]
        first = self.written(self.run_stream(batches, 'first'))
        second = self.written(self.run_stream(batches, 'second'))
        self.assertEqual([p[:2] for p in first], [('BTC', END_MS), ('XRP', END_MS + 60000)])
        self.assertEqual(first, second)


if __name__ == '__main__':
    unittest.main()
