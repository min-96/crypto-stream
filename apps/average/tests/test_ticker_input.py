"""AVG-06: Preprocessed Ticker parsing and 1-minute event-time window assignment (ADR-023).

Needs pyspark; run inside the Spark image with ../run-spark-tests.sh.
"""

import json
import sys
import unittest
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import DecimalType, TimestampType
except ImportError:  # pragma: no cover - host Python has no pyspark
    SparkSession = None

if SparkSession is not None:
    from ticker_input import DECIMAL_TYPE, FIELDS, assign_minute_window, parse_preprocessed_ticker

MINUTE_MS = 1793318400000  # 2026-10-29T00:00:00Z, a whole UTC minute


def ticker(timestamp, value='100', **overrides):
    record = {'market': 'UPBIT', 'timestamp': timestamp, 'coin_symbol': 'BTC',
              'data': {field: value for field in FIELDS}}
    record.update(overrides)
    return json.dumps(record).encode()


@unittest.skipIf(SparkSession is None, 'pyspark not available')
class TickerInputTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[1]').appName('avg06-test')
                     .config('spark.sql.session.timeZone', 'Asia/Seoul')
                     .config('spark.ui.enabled', 'false')
                     .config('spark.sql.shuffle.partitions', '1').getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, values):
        return self.spark.createDataFrame([(bytearray(v),) for v in values], 'value binary')

    def parse(self, values):
        return parse_preprocessed_ticker(self.frame(values))

    def test_event_time_keeps_every_millisecond(self):
        # Preprocessed timestamp is Upbit epoch ms / 1000 as a float (upbit.py).
        expected = [MINUTE_MS + ms for ms in range(0, 60000, 7)] + [1689633864893, 1793318459999]
        rows = self.parse([ticker(ms / 1000) for ms in expected]) \
            .selectExpr('unix_millis(event_time) AS ms').collect()
        self.assertEqual([r.ms for r in rows], expected)

    def test_output_columns_and_types(self):
        parsed = self.parse([ticker(MINUTE_MS / 1000)])
        self.assertEqual(parsed.columns, ['market', 'coin_symbol', 'event_time', *FIELDS])
        self.assertIsInstance(parsed.schema['event_time'].dataType, TimestampType)
        for field in FIELDS:
            self.assertEqual(parsed.schema[field].dataType, DECIMAL_TYPE)
        row = parsed.collect()[0]
        self.assertEqual((row.market, row.coin_symbol), ('UPBIT', 'BTC'))

    def test_fields_become_exact_decimals_without_double(self):
        # Strings as upbit.py writes them with str(float), including exponent form.
        cases = {'1e-05': '0.00001', '-0.0123': '-0.0123', '123456789.123456789': '123456789.123456789',
                 '0.1': '0.1', '159000000.0': '159000000', '0.123456789012345678': '0.123456789012345678'}
        for text, exact in cases.items():
            with self.subTest(text=text):
                row = self.parse([ticker(MINUTE_MS / 1000, value=text)]).collect()[0]
                for field in FIELDS:
                    self.assertEqual(getattr(row, field), Decimal(exact))

    def test_values_within_decimal_38_18_convert_exactly(self):
        cases = {
            '1e-18': '1E-18',
            '0.000000000000000001': '1E-18',
            '1.5e-17': '1.5E-17',
            '99999999999999999999.999999999999999999': '99999999999999999999.999999999999999999',
            '-99999999999999999999': '-99999999999999999999',
            '9.9999999999999999999e19': '99999999999999999999',
            # Trailing zeros carry no value, so they do not count as fractional digits.
            '1.0000000000000000000000': '1',
            '0012.50': '12.5',
            '0': '0',
        }
        for text, exact in cases.items():
            with self.subTest(text=text):
                row = self.parse([ticker(MINUTE_MS / 1000, value=text)]).collect()[0]
                for field in FIELDS:
                    self.assertEqual(getattr(row, field), Decimal(exact))

    def test_values_that_would_be_rounded_or_overflow_fail_explicitly(self):
        cases = {
            '0.1234567890123456789': 'more than 18 fractional digits',
            '1e-19': 'more than 18 fractional digits',
            '1.5e-18': 'more than 18 fractional digits',
            '99999999999999999999.9999999999999999999': 'more than 18 fractional digits',
            '100000000000000000000': 'more than 20 integer digits',
            '1e20': 'more than 20 integer digits',
            '-123456789012345678901.5': 'more than 20 integer digits',
            '1e99999999999': 'not a plain decimal number',
            'NaN': 'not a plain decimal number',
            'Infinity': 'not a plain decimal number',
            ' 1': 'not a plain decimal number',
            '1.2.3': 'not a plain decimal number',
        }
        for text, reason in cases.items():
            with self.subTest(text=text), self.assertRaisesRegex(Exception, f'AVG-06: opening_price (is|has) {reason}'):
                self.parse([ticker(MINUTE_MS / 1000, value=text)]).collect()

    def test_minute_window_is_start_inclusive_end_exclusive(self):
        cases = {MINUTE_MS: MINUTE_MS + 60000, MINUTE_MS + 59999: MINUTE_MS + 60000,
                 MINUTE_MS + 60000: MINUTE_MS + 120000, MINUTE_MS - 1: MINUTE_MS}
        windowed = assign_minute_window(self.parse([ticker(ms / 1000) for ms in cases]))
        rows = windowed.selectExpr('unix_millis(event_time) AS ms', 'unix_millis(window.start) AS start',
                                   'time').collect()
        self.assertEqual({r.ms: r.time for r in rows}, cases)
        for r in rows:
            self.assertEqual(r.time - r.start, 60000)
            self.assertEqual(r.time % 60000, 0)
        self.assertEqual(dict(windowed.dtypes)['time'], 'bigint')

    def test_contract_violations_fail_instead_of_becoming_null(self):
        bad_field = json.loads(ticker(MINUTE_MS / 1000))
        cases = {
            'malformed json': b'{"market": ',
            'null field': ticker(MINUTE_MS / 1000, value=None),
            'non-numeric field': ticker(MINUTE_MS / 1000, value='abc'),
            'empty field': ticker(MINUTE_MS / 1000, value=''),
            'missing timestamp': ticker(None),
            'string timestamp': ticker(str(MINUTE_MS / 1000)),
            'zero timestamp': ticker(0),
            'missing coin': ticker(MINUTE_MS / 1000, coin_symbol=None),
            'empty market': ticker(MINUTE_MS / 1000, market=''),
        }
        del bad_field['data']['trade_price']
        cases['missing field'] = json.dumps(bad_field).encode()
        for name, value in cases.items():
            with self.subTest(name), self.assertRaisesRegex(Exception, 'AVG-06'):
                # Window assignment uses only event_time; the check must still fire.
                assign_minute_window(self.parse([ticker(MINUTE_MS / 1000), value])).select('time').collect()

    def test_window_assignment_works_on_a_stream(self):
        source = self.spark.readStream.format('rate').option('rowsPerSecond', 1).load()
        stream = source.selectExpr(
            "cast(to_json(named_struct('market', 'UPBIT', 'timestamp', 1793318459.999D, "
            "'coin_symbol', 'BTC', 'data', named_struct("
            + ', '.join(f"'{f}', '1'" for f in FIELDS) + "))) AS binary) AS value")
        windowed = assign_minute_window(parse_preprocessed_ticker(stream))
        self.assertTrue(windowed.isStreaming)
        query = windowed.select('time').writeStream.format('memory').queryName('avg06_stream').start()
        try:
            query.processAllAvailable()
            while not self.spark.table('avg06_stream').count():
                query.processAllAvailable()
        finally:
            query.stop()
        self.assertEqual({r.time for r in self.spark.table('avg06_stream').collect()}, {MINUTE_MS + 60000})


if __name__ == '__main__':
    unittest.main()
