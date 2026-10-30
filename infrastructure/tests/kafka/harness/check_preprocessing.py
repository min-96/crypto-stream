"""Isolated Raw -> preprocessing consumer -> Preprocessed Kafka check."""

import asyncio
import json
import os
import sys
import time
from pathlib import Path

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

sys.path.insert(0, '/code/preprocessing')
from consumer import run_at_least_once as run
from upbit import convert_ticker, convert_orderbook


BOOTSTRAP = os.environ['KAFKA_BOOTSTRAP_SERVERS']
KEY = b'BINANCE:BTC:USDT'


def fixture_converter(raw, key):
    assert key == KEY
    return json.loads(raw)['converted']


async def check(kind, raw_topic, pre_topic, raw_value, key, converter, expected, label):
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all')
    await producer.start()
    try:
        sent = await producer.send_and_wait(raw_topic, key=key, value=raw_value)
    finally:
        await producer.stop()

    await asyncio.wait_for(run(kind, converter, bootstrap=BOOTSTRAP, once=True), 60)
    consumer = AIOKafkaConsumer(pre_topic, bootstrap_servers=BOOTSTRAP,
                                auto_offset_reset='earliest', enable_auto_commit=False)
    await consumer.start()
    try:
        record = await asyncio.wait_for(consumer.getone(), 20)
        assert record.key == key, (record.key, key)
        assert json.loads(record.value) == expected
        print(f'PREPROCESS_{label}_{kind.upper()}_OK raw_offset={sent.offset} '
              f'pre_offset={record.offset} key={record.key.decode()} bytes={len(record.value)}')
    finally:
        await consumer.stop()


async def main():
    timestamp = time.time()
    ticker = {'market': 'BINANCE', 'timestamp': timestamp, 'coin_symbol': 'BTC',
              'data': {name: '1000.0' for name in (
                  'opening_price', 'trade_price', 'max_price', 'min_price',
                  'prev_closing_price', 'acc_trade_volume_24h',
                  'signed_change_price', 'signed_change_rate')}}
    orderbook = [{'region': 'Asia', 'market': 'BINANCE', 'symbol': 'BTC',
                  'highest_bid': 1000.0, 'lowest_ask': 1001.0,
                  'total_bid_volume': 2.0, 'total_ask_volume': 3.0,
                  'timestamp': '2026-10-29T00:00:00Z'}]
    await check('ticker', 'asiaSocketDataInBTC-ticker',
                'RegionAsia_TickerPreprocessing',
                json.dumps({'converted': ticker}).encode(), KEY,
                fixture_converter, ticker, 'FIXTURE')
    await check('orderbook', 'asiaSocketDataInBTC-orderbook',
                'RegionAsia_OrderbookPreprocessing',
                json.dumps({'converted': orderbook}).encode(), KEY,
                fixture_converter, orderbook, 'FIXTURE')
    fixture_dir = Path('/code/preprocessing/tests/fixtures')
    upbit_key = b'UPBIT:BTC:KRW'
    for kind, converter in (('ticker', convert_ticker), ('orderbook', convert_orderbook)):
        raw = (fixture_dir / f'upbit_{kind}.json').read_bytes()
        await check(kind, f'koreaSocketDataInBTC-{kind}',
                    f'RegionKorea_{kind.capitalize()}Preprocessing',
                    raw, upbit_key, converter, converter(raw, upbit_key), 'UPBIT')


if __name__ == '__main__':
    asyncio.run(main())
