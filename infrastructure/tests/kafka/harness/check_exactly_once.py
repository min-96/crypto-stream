"""Abort after Kafka ACK, then prove Raw replay has one committed average input."""
import asyncio
import json
import os
import sys
import uuid
from pathlib import Path

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

import check

sys.path.insert(0, '/code/preprocessing')
from consumer import run


BOOTSTRAP = os.environ['KAFKA_BOOTSTRAP_SERVERS']
RAW = 'asiaSocketDataInBTC-ticker'
PRE = 'RegionAsia_TickerPreprocessing'
KEY = b'BINANCE:BTC:USDT'
ARTIFACT = Path('/artifacts/exactly_once.json')


def converted(raw, key):
    assert key == KEY
    return json.loads(raw)['converted']


def ticker(price):
    names = (
        'opening_price', 'trade_price', 'max_price', 'min_price',
        'prev_closing_price', 'acc_trade_volume_24h',
        'signed_change_price', 'signed_change_rate',
    )
    return {
        'market': 'BINANCE', 'timestamp': 1793270000.0, 'coin_symbol': 'BTC',
        'data': {name: str(price) for name in names},
    }


async def committed(group):
    consumer = AIOKafkaConsumer(bootstrap_servers=BOOTSTRAP, group_id=group,
                                enable_auto_commit=False)
    await consumer.start()
    try:
        return await consumer.committed(TopicPartition(RAW, 0))
    finally:
        await consumer.stop()


async def read_visible(expected, timeout=20):
    consumer = AIOKafkaConsumer(
        PRE, bootstrap_servers=BOOTSTRAP, auto_offset_reset='earliest',
        enable_auto_commit=False, isolation_level='read_committed')
    await consumer.start()
    try:
        records = []
        deadline = asyncio.get_running_loop().time() + timeout
        while len(records) < expected and asyncio.get_running_loop().time() < deadline:
            batches = await consumer.getmany(timeout_ms=1000)
            for part_records in batches.values():
                records.extend(part_records)
        return records
    finally:
        await consumer.stop()


async def transaction():
    group = 'kaf-eos-' + uuid.uuid4().hex
    instance = 'ticker-instance-1'
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all')
    await producer.start()
    try:
        for price in (10, 20):
            await producer.send_and_wait(
                RAW, key=KEY, value=json.dumps({'converted': ticker(price)}).encode())
    finally:
        await producer.stop()

    def interrupt(_raw):
        raise RuntimeError('injected after Kafka produce ACK before transaction commit')

    try:
        await asyncio.wait_for(run(
            'ticker', converted, bootstrap=BOOTSTRAP, group_id=group,
            instance_id=instance, topics=(RAW,), once=True, after_ack=interrupt), 40)
    except RuntimeError as error:
        assert 'injected after Kafka produce ACK' in str(error), error
    else:
        raise AssertionError('expected transaction abort after produce ACK')
    assert await committed(group) is None
    assert await read_visible(1, timeout=3) == []
    print('EOS_ABORT_INVISIBLE_OK raw_committed=None')

    await asyncio.wait_for(run(
        'ticker', converted, bootstrap=BOOTSTRAP, group_id=group,
        instance_id=instance, topics=(RAW,), once=True), 40)
    assert await committed(group) == 1
    await asyncio.wait_for(run(
        'ticker', converted, bootstrap=BOOTSTRAP, group_id=group,
        instance_id=instance, topics=(RAW,), once=True), 40)
    assert await committed(group) == 2
    records = await read_visible(2)
    assert len(records) == 2, records
    assert [float(json.loads(record.value)['data']['trade_price'])
            for record in records] == [10, 20], records
    assert all(record.key == KEY for record in records)
    evidence = {
        'raw_committed_after_abort': None,
        'visible_after_abort': 0,
        'raw_committed_after_replay': 2,
        'visible_prices': [10, 20],
        'preprocessed_offsets': [record.offset for record in records],
        'expected_average': 15.0,
    }
    ARTIFACT.write_text(json.dumps(evidence, indent=2))
    print(f'EOS_KAFKA_INPUT_OK offsets={evidence["preprocessed_offsets"]} mean=15')


def archive():
    check.ensure_buckets()
    check.register_connectors()
    evidence = json.loads(ARTIFACT.read_text())
    expected = dict(zip(evidence['preprocessed_offsets'], (10, 20)))

    def archived():
        objects = check.archived_objects('preprocessed', PRE)
        values = {name: body for name, body in objects.items()
                  if name.endswith('.bin') and '.keys.' not in name}
        keys = {name: body for name, body in objects.items()
                if name.endswith('.keys.bin')}
        if len(values) != 2 or len(keys) != 2:
            return False
        found = {}
        for offset, price in expected.items():
            suffix = f'+{offset:010d}.bin'
            matches = [name for name in values if name.endswith(suffix)]
            if len(matches) != 1:
                return False
            value_file = matches[0]
            key_file = value_file[:-4] + '.keys.bin'
            if key_file not in keys or KEY not in keys[key_file]:
                return False
            if f'"trade_price":"{price}"'.encode() not in values[value_file]:
                return False
            found[offset] = value_file
        return found

    files = check.wait_until('committed-only Preprocessed archive', archived, timeout=120)
    evidence['archived_offsets'] = sorted(files)
    evidence['archived_value_count'] = 2
    evidence['archived_key_count'] = 2
    ARTIFACT.write_text(json.dumps(evidence, indent=2))
    print(f'EOS_CONNECT_ARCHIVE_OK offsets={sorted(files)}')


if __name__ == '__main__':
    phase = sys.argv[1]
    if phase == 'transaction':
        asyncio.run(transaction())
    elif phase == 'archive':
        archive()
    else:
        raise ValueError(phase)
