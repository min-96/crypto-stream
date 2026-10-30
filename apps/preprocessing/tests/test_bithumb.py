import asyncio
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from bithumb import convert_ticker as convert_bithumb_ticker
from pipeline import process_record
from ticker import convert_ticker


KEY = b'BITHUMB:BTC:KRW'
RAW = (HERE / 'fixtures' / 'bithumb_ticker.json').read_bytes()


class Producer:
    def __init__(self):
        self.calls = []

    async def send_and_wait(self, topic, *, key, value):
        self.calls.append((topic, key, json.loads(value)))


def frame(**changes):
    value = json.loads(RAW)
    value.update(changes)
    return json.dumps(value).encode()


class BithumbTickerTests(unittest.TestCase):
    def process(self, raw=RAW, key=KEY, converter=convert_ticker):
        record = SimpleNamespace(topic='koreaSocketDataInBTC-ticker', partition=0,
                                 offset=7, key=key, value=raw)
        producer = Producer()
        asyncio.run(process_record(record, producer, converter))
        return producer.calls

    def test_supplied_sample_maps_to_preprocessed_topic(self):
        calls = self.process()
        self.assertEqual(len(calls), 1)
        topic, key, result = calls[0]
        self.assertEqual(topic, 'RegionKorea_TickerPreprocessing')
        self.assertEqual(key, KEY)
        self.assertEqual(result, {
            'market': 'BITHUMB', 'timestamp': 1725927377.931, 'coin_symbol': 'BTC',
            'data': {
                'opening_price': '484500', 'trade_price': '493100',
                'max_price': '493100', 'min_price': '472500',
                'prev_closing_price': '484500',
                'acc_trade_volume_24h': '13386.15417512',
                'signed_change_price': '8600', 'signed_change_rate': '0.01775026',
            },
        })

    def test_direct_converter_matches_dispatch(self):
        self.assertEqual(convert_bithumb_ticker(RAW, KEY), convert_ticker(RAW, KEY))

    def test_null_value_is_skipped_by_common_validation(self):
        with self.assertLogs('pipeline', level='WARNING') as logs:
            self.assertEqual(self.process(frame(trade_price=None)), [])
        self.assertIn('BITHUMB', '\n'.join(logs.output))
        self.assertIn('offset=7', '\n'.join(logs.output))

    def test_contract_errors_stop_before_publish(self):
        cases = (
            (RAW, b'BITHUMB:ETH:KRW'),
            (RAW, b'BITHUMB:BTC:USDT'),
            (frame(type='orderbook'), KEY),
            (frame(timestamp=0), KEY),
            (frame(timestamp=True), KEY),
            (frame(trade_price='493100'), KEY),
        )
        for raw, key in cases:
            with self.subTest(key=key, raw=raw):
                with self.assertRaises(ValueError):
                    self.process(raw, key)
        missing = json.loads(RAW)
        del missing['high_price']
        with self.assertRaises(ValueError):
            self.process(json.dumps(missing).encode())
        with self.assertRaises(ValueError):
            convert_bithumb_ticker(RAW, b'UPBIT:BTC:KRW')

    def test_dispatch_still_accepts_upbit(self):
        upbit_raw = (HERE / 'fixtures' / 'upbit_ticker.json').read_bytes()
        calls = self.process(upbit_raw, b'UPBIT:BTC:KRW')
        self.assertEqual(calls[0][2]['market'], 'UPBIT')


if __name__ == '__main__':
    unittest.main()
