import asyncio
import json
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from pipeline import process_record
from upbit import convert_ticker, convert_orderbook


class Producer:
    def __init__(self):
        self.calls = []

    async def send_and_wait(self, topic, *, key, value):
        self.calls.append((topic, key, json.loads(value)))


class UpbitSampleTests(unittest.TestCase):
    def check_sample(self, kind, converter):
        raw = (HERE / 'fixtures' / f'upbit_{kind}.json').read_bytes()
        record = type('Record', (), {'topic': f'koreaSocketDataInBTC-{kind}',
                                     'key': b'UPBIT:BTC:KRW', 'value': raw})()
        producer = Producer()
        asyncio.run(process_record(record, producer, converter))
        self.assertEqual(len(producer.calls), 1)
        topic, key, value = producer.calls[0]
        self.assertEqual(key, record.key)
        return topic, value

    def test_official_ticker_sample(self):
        topic, value = self.check_sample('ticker', convert_ticker)
        self.assertEqual(topic, 'RegionKorea_TickerPreprocessing')
        self.assertEqual(value['market'], 'UPBIT')
        self.assertEqual(value['coin_symbol'], 'BTC')
        self.assertEqual(value['timestamp'], 1787729042.606)
        self.assertEqual(value['data']['max_price'], '110117000.0')
        self.assertEqual(value['data']['min_price'], '108958000.0')
        self.assertEqual(value['data']['trade_price'], '109868000.0')
        self.assertEqual(value['data']['acc_trade_volume_24h'], '1807.38027625')

    def test_official_orderbook_sample(self):
        topic, value = self.check_sample('orderbook', convert_orderbook)
        self.assertEqual(topic, 'RegionKorea_OrderbookPreprocessing')
        self.assertEqual(len(value), 1)
        self.assertEqual(value[0]['highest_bid'], 109880000.0)
        self.assertEqual(value[0]['lowest_ask'], 109950000.0)
        self.assertEqual(value[0]['total_bid_volume'], 11.48721726)
        self.assertEqual(value[0]['total_ask_volume'], 18.93431797)
        self.assertEqual(value[0]['timestamp'], '2026-08-26T07:05:47.526000Z')

    def test_quote_mismatch_is_rejected(self):
        raw = (HERE / 'fixtures' / 'upbit_ticker.json').read_bytes()
        with self.assertRaises(ValueError):
            convert_ticker(raw, b'UPBIT:BTC:USDT')

    def test_empty_orderbook_is_rejected(self):
        raw = json.loads((HERE / 'fixtures' / 'upbit_orderbook.json').read_text())
        raw['orderbook_units'] = []
        with self.assertRaises(ValueError):
            convert_orderbook(json.dumps(raw).encode(), b'UPBIT:BTC:KRW')


if __name__ == '__main__':
    unittest.main()
