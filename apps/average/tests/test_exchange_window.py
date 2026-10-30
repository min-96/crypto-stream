"""AVG-07: first-stage streaming aggregation per (1-minute window, exchange, coin) (ADR-024).

Needs pyspark; run inside the Spark image with ../run-spark-tests.sh.
"""

import json
import shutil
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import DecimalType, LongType
except ImportError:  # pragma: no cover - host Python has no pyspark
    SparkSession = None

if SparkSession is not None:
    from exchange_window import aggregate_exchange_window
    from ticker_input import DECIMAL_TYPE, FIELDS, parse_preprocessed_ticker

MINUTE_MS = 1793318400000  # 2026-10-29T00:00:00Z, a whole UTC minute
END_MS = MINUTE_MS + 60000  # `time` of the window [MINUTE_MS, END_MS)


def ticker(ms, value='100', market='UPBIT', coin='BTC', **fields):
    data = {field: value for field in FIELDS}
    data.update(fields)
    return json.dumps({'market': market, 'timestamp': ms / 1000, 'coin_symbol': coin, 'data': data})


@unittest.skipIf(SparkSession is None, 'pyspark not available')
class ExchangeWindowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[1]').appName('avg07-test')
                     .config('spark.sql.session.timeZone', 'Asia/Seoul')
                     .config('spark.ui.enabled', 'false')
                     .config('spark.sql.shuffle.partitions', '1').getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix='avg07-'))
        self.source = self.tmp / 'source'
        self.source.mkdir()
        self.batches = 0
        self.query = None

    def tearDown(self):
        if self.query is not None:
            self.query.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def start(self):
        # One file per micro-batch, so each send() is exactly one data batch.
        stream = self.spark.readStream.option('maxFilesPerTrigger', 1).text(str(self.source))
        self.aggregated = aggregate_exchange_window(parse_preprocessed_ticker(stream))
        self.table = f'avg07_{id(self)}'
        self.query = (self.aggregated.writeStream.format('memory').queryName(self.table)
                      .outputMode('append')
                      .option('checkpointLocation', str(self.tmp / 'checkpoint')).start())

    def send(self, *lines):
        """Run one micro-batch with `lines`, plus the no-data batch a watermark change triggers."""
        if self.query is None:
            self.start()
        self.batches += 1
        path = self.source / f'{self.batches:04d}.json'
        tmp = self.tmp / path.name
        tmp.write_text('\n'.join(lines) + '\n')
        tmp.rename(path)
        self.query.processAllAvailable()
        return self.output()

    def output(self):
        rows = self.spark.table(self.table).collect()
        return sorted(rows, key=lambda r: (r.time, r.market, r.coin_symbol))

    def dropped_by_watermark(self):
        return sum(op['numRowsDroppedByWatermark']
                   for progress in self.query.recentProgress for op in progress['stateOperators'])

    def test_output_columns_and_types(self):
        self.send(ticker(MINUTE_MS))
        schema = self.aggregated.schema
        self.assertEqual(self.aggregated.columns,
                         ['time', 'market', 'coin_symbol', 'message_count', *(f'{f}_sum' for f in FIELDS)])
        self.assertIsInstance(schema['time'].dataType, LongType)
        self.assertIsInstance(schema['message_count'].dataType, LongType)
        for field in FIELDS:
            self.assertEqual(schema[f'{field}_sum'].dataType, DecimalType(38, DECIMAL_TYPE.scale))

    def test_window_is_emitted_once_only_after_watermark_passes_its_end(self):
        self.assertEqual(self.send(ticker(MINUTE_MS), ticker(MINUTE_MS + 59999)), [])
        # Watermark = max event time - 10 s = END_MS - 1 ms: the window is still open.
        self.assertEqual(self.send(ticker(END_MS + 9999)), [])
        # Watermark reaches END_MS: [MINUTE_MS, END_MS) is final.
        rows = self.send(ticker(END_MS + 10000))
        self.assertEqual([(r.time, r.message_count) for r in rows], [(END_MS, 2)])
        # Later batches never re-emit or update it.
        rows = self.send(ticker(END_MS + 30000), ticker(END_MS + 125000))
        self.assertEqual([(r.time, r.message_count) for r in rows], [(END_MS, 2), (END_MS + 60000, 3)])
        self.assertEqual(self.send(ticker(END_MS + 130000)), rows)

    def test_events_in_a_finalized_window_are_dropped_by_watermark(self):
        self.send(ticker(MINUTE_MS, value='1'))
        self.send(ticker(END_MS + 15000, value='1'))  # watermark END_MS + 5 s closes the first window
        self.assertEqual(self.dropped_by_watermark(), 0)
        # Late for the closed window; must not change its emitted row or open a new one.
        self.send(ticker(MINUTE_MS + 30000, value='1000'), ticker(END_MS - 1, value='1000'))
        self.assertGreater(self.dropped_by_watermark(), 0)
        rows = self.send(ticker(END_MS + 180000, value='1'))
        self.assertEqual([(r.time, r.message_count, r.trade_price_sum) for r in rows],
                         [(END_MS, 1, Decimal(1)), (END_MS + 60000, 1, Decimal(1))])

    def test_event_older_than_watermark_still_counts_while_its_window_is_open(self):
        # ADR-024 (6): only events of a finalized window (end <= watermark) are dropped.
        self.send(ticker(END_MS, value='1'))
        self.send(ticker(END_MS + 50000, value='1'))  # watermark END_MS + 40 s; window still open
        self.send(ticker(END_MS + 5000, value='1000'))  # 35 s older than the watermark
        rows = self.send(ticker(END_MS + 80000, value='1'))
        self.assertEqual([(r.time, r.message_count, r.trade_price_sum) for r in rows],
                         [(END_MS + 60000, 3, Decimal(1002))])
        self.assertEqual(self.dropped_by_watermark(), 0)

    def test_one_row_per_exchange_coin_window_without_mixing(self):
        lines = [ticker(MINUTE_MS + i, value='1') for i in range(500)]
        lines += [ticker(MINUTE_MS + 1, value='10', coin='ETH'), ticker(MINUTE_MS + 2, value='20', coin='ETH'),
                  ticker(MINUTE_MS + 3, value='1000', market='BITHUMB'),
                  ticker(END_MS, value='7')]  # next window
        self.send(*lines)
        rows = self.send(ticker(END_MS + 70000))
        got = [(r.time, r.market, r.coin_symbol, r.message_count, r.trade_price_sum) for r in rows]
        self.assertEqual(got, [
            (END_MS, 'BITHUMB', 'BTC', 1, Decimal(1000)),
            (END_MS, 'UPBIT', 'BTC', 500, Decimal(500)),
            (END_MS, 'UPBIT', 'ETH', 2, Decimal(30)),
            (END_MS + 60000, 'UPBIT', 'BTC', 1, Decimal(7)),
        ])

    def test_each_field_is_summed_exactly_and_independently(self):
        values = {field: f'{i + 1}.000000000000000001' for i, field in enumerate(FIELDS)}
        values['signed_change_rate'] = '-0.123456789012345678'
        self.send(ticker(MINUTE_MS, **values), ticker(MINUTE_MS + 1, **values), ticker(MINUTE_MS + 2, **values))
        row, = self.send(ticker(END_MS + 10000))
        for field in FIELDS:
            self.assertEqual(getattr(row, f'{field}_sum'), Decimal(values[field]) * 3)

    def test_sum_overflow_fails_explicitly_instead_of_null(self):
        for big in ('99999999999999999999', '-99999999999999999999'):
            with self.subTest(big=big):
                self.tearDown()
                self.setUp()
                self.send(ticker(MINUTE_MS, trade_price=big), ticker(MINUTE_MS + 1, trade_price=big))
                with self.assertRaisesRegex(Exception, 'AVG-07: trade_price sum is out of'):
                    self.send(ticker(END_MS + 10000))

    def test_sum_at_the_decimal_limit_is_kept(self):
        self.send(ticker(MINUTE_MS, trade_price='99999999999999999998.999999999999999999'),
                  ticker(MINUTE_MS + 1, trade_price='1'))
        row, = self.send(ticker(END_MS + 10000))
        self.assertEqual(row.trade_price_sum, Decimal('99999999999999999999.999999999999999999'))


if __name__ == '__main__':
    unittest.main()
