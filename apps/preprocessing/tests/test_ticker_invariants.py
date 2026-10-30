"""ADR-019 Ticker invariants: value errors skip one record, contract errors stop."""

import asyncio
import json
import sys
import types
import unittest
from collections import namedtuple
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
try:
    import aiokafka
except ModuleNotFoundError:
    aiokafka = types.ModuleType('aiokafka')
    aiokafka.TopicPartition = namedtuple('TopicPartition', 'topic partition')
    sys.modules['aiokafka'] = aiokafka

from offsets import publish_batch_transactionally
from pipeline import TICKER_FIELDS, process_record
from upbit import convert_ticker


PRICE_FIELDS = ('opening_price', 'trade_price', 'max_price', 'min_price', 'prev_closing_price')
KEY = b'BINANCE:BTC:USDT'


class Transaction:
    def __init__(self, producer):
        self.producer = producer

    async def __aenter__(self):
        self.producer.events.append('begin')

    async def __aexit__(self, kind, _error, _traceback):
        self.producer.events.append('abort' if kind else 'commit')


class Producer:
    def __init__(self):
        self.calls = []
        self.events = []
        self.offsets = None

    async def send_and_wait(self, topic, *, key, value):
        self.calls.append((topic, key, json.loads(value)))
        self.events.append('produce')

    def transaction(self):
        return Transaction(self)

    async def send_offsets_to_transaction(self, offsets, _group):
        self.events.append('offsets')
        self.offsets = {(tp.topic, tp.partition): offset for tp, offset in offsets.items()}


def raw(offset=7, *, topic='asiaSocketDataInBTC-ticker', key=KEY, value=b'{}', partition=2):
    return SimpleNamespace(topic=topic, partition=partition, offset=offset, key=key, value=value)


def ticker(**overrides):
    data = {name: '1000.0' for name in TICKER_FIELDS}
    data.update(overrides)
    return {'market': 'BINANCE', 'timestamp': 1730000000.5, 'coin_symbol': 'BTC', 'data': data}


def run(record, convert, producer=None):
    producer = producer or Producer()
    asyncio.run(process_record(record, producer, convert))
    return producer


class ValueErrorSkipsRecordTests(unittest.TestCase):
    """NULL, empty, non-numeric, NaN/Infinity and price <= 0 drop only that record."""

    def assert_skipped(self, field, value):
        with self.subTest(field=field, value=value):
            with self.assertLogs('pipeline', level='WARNING') as logs:
                producer = run(raw(), lambda _raw, _key: ticker(**{field: value}))
            self.assertEqual(producer.calls, [], 'invalid record must not reach Preprocessed')
            message = '\n'.join(logs.output)
            for expected in ('BINANCE', 'BTC', 'asiaSocketDataInBTC-ticker',
                             'partition=2', 'offset=7', field):
                self.assertIn(expected, message)

    def test_null_field(self):
        for field in TICKER_FIELDS:
            self.assert_skipped(field, None)

    def test_empty_or_blank_string(self):
        for field in TICKER_FIELDS:
            for value in ('', '   '):
                self.assert_skipped(field, value)

    def test_non_numeric_string(self):
        for field in TICKER_FIELDS:
            for value in ('abc', '1,000', '0x10'):
                self.assert_skipped(field, value)

    def test_nan_and_infinity(self):
        for field in TICKER_FIELDS:
            for value in ('NaN', 'nan', 'Infinity', '-Infinity', 'inf'):
                self.assert_skipped(field, value)

    def test_price_zero_or_negative(self):
        for field in PRICE_FIELDS:
            for value in ('0', '0.0', '-0', '-1', '-0.0001'):
                self.assert_skipped(field, value)


class NumericRangeTests(unittest.TestCase):
    """Values must parse to a finite double. No business upper bound is defined."""

    def test_overflow_to_infinity_is_skipped(self):
        for field in TICKER_FIELDS:
            for value in ('1e400', '-1e400'):
                ValueErrorSkipsRecordTests.assert_skipped(self, field, value)

    def test_price_underflow_to_zero_is_skipped(self):
        for field in PRICE_FIELDS:
            ValueErrorSkipsRecordTests.assert_skipped(self, field, '1e-400')

    def test_change_underflow_to_zero_is_valid(self):
        producer = run(raw(), lambda _raw, _key: ticker(signed_change_rate='1e-400'))
        self.assertEqual(len(producer.calls), 1)

    def test_largest_finite_value_is_valid(self):
        producer = run(raw(), lambda _raw, _key: ticker(max_price='1e308'))
        self.assertEqual(len(producer.calls), 1)


class ValidValuesPublishTests(unittest.TestCase):
    def assert_published(self, **overrides):
        producer = run(raw(), lambda _raw, _key: ticker(**overrides))
        self.assertEqual(len(producer.calls), 1)
        self.assertEqual(producer.calls[0][2]['data'], ticker(**overrides)['data'])

    def test_negative_change_price_and_rate_are_valid(self):
        self.assert_published(signed_change_price='-703000.0', signed_change_rate='-0.0064')

    def test_zero_change_is_valid(self):
        self.assert_published(signed_change_price='0', signed_change_rate='0.0')

    def test_small_positive_price_is_valid(self):
        self.assert_published(trade_price='0.00000001', min_price='1e-8')


class ContractViolationStopsTests(unittest.TestCase):
    """Key, schema and identity mismatches or unexpected errors still raise."""

    def assert_stops(self, record, convert, error=ValueError):
        producer = Producer()
        with self.assertRaises(error):
            asyncio.run(process_record(record, producer, convert))
        self.assertEqual(producer.calls, [])

    def test_malformed_key(self):
        self.assert_stops(raw(key=b'BINANCE-BTC'), lambda _raw, _key: ticker())

    def test_exchange_mismatch(self):
        self.assert_stops(raw(), lambda _raw, _key: {**ticker(), 'market': 'UPBIT'})

    def test_coin_mismatch(self):
        self.assert_stops(raw(), lambda _raw, _key: {**ticker(), 'coin_symbol': 'ETH'})

    def test_unknown_top_level_field(self):
        self.assert_stops(raw(), lambda _raw, _key: {**ticker(), 'extra': 1})

    def test_missing_data_field(self):
        """A converter output without a schema key is a converter bug, not a value error."""
        def convert(_raw, _key):
            record = ticker()
            del record['data']['trade_price']
            return record
        self.assert_stops(raw(), convert)

    def test_unknown_data_field(self):
        def convert(_raw, _key):
            record = ticker()
            record['data']['trade_volume'] = '1.0'
            return record
        self.assert_stops(raw(), convert)

    def test_non_string_field_type_is_schema_violation(self):
        self.assert_stops(raw(), lambda _raw, _key: ticker(trade_price=1000.0))

    def test_unexpected_converter_error(self):
        def convert(_raw, _key):
            raise KeyError('bug in converter')
        self.assert_stops(raw(), convert, KeyError)


class BatchTests(unittest.TestCase):
    def test_skipped_record_offset_commits_with_valid_records(self):
        values = {7: ticker(), 8: ticker(trade_price='0'), 9: ticker(trade_price='-5')}
        records = [raw(offset) for offset in values]
        producer = Producer()
        with self.assertLogs('pipeline', level='WARNING'):
            asyncio.run(publish_batch_transactionally(
                records, lambda record: process_record(
                    record, producer, lambda _raw, _key: values[record.offset]),
                producer, 'preprocessing-ticker'))
        self.assertEqual(producer.events, ['begin', 'produce', 'offsets', 'commit'])
        self.assertEqual(producer.offsets, {('asiaSocketDataInBTC-ticker', 2): 10})

    def test_contract_violation_aborts_whole_batch(self):
        values = {7: ticker(), 8: {**ticker(), 'market': 'UPBIT'}}
        records = [raw(offset) for offset in values]
        producer = Producer()
        with self.assertRaises(ValueError):
            asyncio.run(publish_batch_transactionally(
                records, lambda record: process_record(
                    record, producer, lambda _raw, _key: values[record.offset]),
                producer, 'preprocessing-ticker'))
        self.assertEqual(producer.events, ['begin', 'produce', 'abort'])
        self.assertIsNone(producer.offsets)


class UpbitInvariantTests(unittest.TestCase):
    """Upbit raw frames: null or non-finite numbers are value errors and skip the record.

    A missing key or a non-numeric JSON type means the Upbit frame no longer matches
    the expected contract, so the Consumer stops.
    """

    def frame(self, **overrides):
        frame = json.loads((HERE / 'fixtures' / 'upbit_ticker.json').read_text())
        frame.update(overrides)
        return json.dumps(frame).encode()

    def run_upbit(self, value, key=b'UPBIT:BTC:KRW'):
        record = raw(topic='koreaSocketDataInBTC-ticker', key=key, value=value)
        return run(record, convert_ticker)

    def test_bad_price_values_are_skipped(self):
        for source in ('opening_price', 'trade_price', 'high_price', 'low_price',
                       'prev_closing_price'):
            for value in (0, -1, None, float('nan'), float('inf')):
                with self.subTest(source=source, value=value):
                    with self.assertLogs('pipeline', level='WARNING'):
                        producer = self.run_upbit(self.frame(**{source: value}))
                    self.assertEqual(producer.calls, [])

    def test_missing_raw_field_stops(self):
        for source in ('trade_price', 'high_price', 'acc_trade_volume_24h', 'signed_change_rate'):
            with self.subTest(source=source):
                frame = json.loads(self.frame())
                del frame[source]
                with self.assertRaises(ValueError):
                    self.run_upbit(json.dumps(frame).encode())

    def test_non_numeric_raw_type_stops(self):
        for value in ('109868000', 'abc', True, [1], {'v': 1}):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    self.run_upbit(self.frame(trade_price=value))

    def test_numbers_are_converted_without_changing_value(self):
        producer = self.run_upbit(self.frame(trade_price=109868000, signed_change_rate=1e-08))
        data = producer.calls[0][2]['data']
        self.assertEqual(float(data['trade_price']), 109868000)
        self.assertEqual(float(data['signed_change_rate']), 1e-08)

    def test_negative_change_is_published(self):
        producer = self.run_upbit(self.frame(signed_change_price=-703000.0,
                                             signed_change_rate=-0.0064))
        self.assertEqual(producer.calls[0][2]['data']['signed_change_price'], '-703000.0')

    def test_trading_pair_mismatch_still_stops(self):
        with self.assertRaises(ValueError):
            self.run_upbit(self.frame(), key=b'UPBIT:ETH:KRW')

    def test_wrong_frame_type_still_stops(self):
        with self.assertRaises(ValueError):
            self.run_upbit(self.frame(type='orderbook'))


if __name__ == '__main__':
    unittest.main()
