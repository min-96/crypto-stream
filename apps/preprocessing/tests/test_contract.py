import asyncio
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import process_record, output_topic


class Record:
    topic = 'asiaSocketDataInBTC-ticker'
    key = b'BINANCE:BTC:USDT'
    value = b'{"raw":"ticker"}'


class Producer:
    def __init__(self):
        self.calls = []

    async def send_and_wait(self, topic, *, key, value):
        self.calls.append((topic, key, value))
        return object()


class ContractTests(unittest.TestCase):
    def test_ticker_preserves_original_quote_and_schema(self):
        producer = Producer()
        def convert(raw, key):
            self.assertEqual(raw, Record.value)
            self.assertEqual(key, Record.key)
            return {'market': 'BINANCE', 'timestamp': 1730000000.5, 'coin_symbol': 'BTC',
                    'data': {name: '1000.0' for name in (
                        'opening_price', 'trade_price', 'max_price', 'min_price',
                        'prev_closing_price', 'acc_trade_volume_24h',
                        'signed_change_price', 'signed_change_rate')}}
        asyncio.run(process_record(Record(), producer, convert))
        topic, key, value = producer.calls[0]
        self.assertEqual(topic, 'RegionAsia_TickerPreprocessing')
        self.assertEqual(key, Record.key)
        self.assertEqual(json.loads(value)['coin_symbol'], 'BTC')

    def test_orderbook_array_preserves_key_and_krw_values(self):
        record = Record()
        record.topic = 'asiaSocketDataInBTC-orderbook'
        producer = Producer()
        item = {'region': 'Asia', 'market': 'BINANCE', 'symbol': 'BTC',
                'highest_bid': 1000.0, 'lowest_ask': 1001.0,
                'total_bid_volume': 2.0, 'total_ask_volume': 3.0,
                'timestamp': '2026-10-29T00:00:00Z'}
        asyncio.run(process_record(record, producer, lambda raw, key: [item]))
        topic, key, value = producer.calls[0]
        self.assertEqual(topic, 'RegionAsia_OrderbookPreprocessing')
        self.assertEqual(key, Record.key)
        self.assertEqual(json.loads(value), [item])

    def test_rejects_bad_orderbook_before_publish(self):
        record = Record()
        record.topic = 'asiaSocketDataInBTC-orderbook'
        producer = Producer()
        with self.assertRaises(ValueError):
            asyncio.run(process_record(record, producer, lambda raw, key: []))
        self.assertEqual(producer.calls, [])

    def test_topic_mapping_rejects_unknown(self):
        with self.assertRaises(ValueError):
            output_topic('unknown')


if __name__ == '__main__':
    unittest.main()
