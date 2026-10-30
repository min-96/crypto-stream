"""KAF-11 retention fault injection in a dedicated Compose project."""
import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

sys.path.insert(0, '/code/preprocessing')
from consumer import RequiredRawOffsetExpired, run_at_least_once as run


BOOTSTRAP = os.environ['KAFKA_BOOTSTRAP_SERVERS']
RAW = 'asiaSocketDataInBTC-ticker'
PRE = 'RegionAsia_TickerPreprocessing'
KEY = b'BINANCE:BTC:USDT'
ARTIFACT = Path('/artifacts/retention.json')


def convert(raw, key):
    assert key == KEY
    return json.loads(raw)['converted']


async def offsets(consumer):
    partitions = [TopicPartition(RAW, 0), TopicPartition(PRE, 0)]
    return {tp.topic: (await consumer.beginning_offsets([tp]))[tp] for tp in partitions}


async def setup():
    marker = 'retention-' + uuid.uuid4().hex
    group = 'kaf11-' + uuid.uuid4().hex
    ticker = {
        'market': 'BINANCE', 'timestamp': time.time(), 'coin_symbol': 'BTC',
        'data': {name: marker if name == 'opening_price' else '1000.0' for name in (
            'opening_price', 'trade_price', 'max_price', 'min_price',
            'prev_closing_price', 'acc_trade_volume_24h', 'signed_change_price',
            'signed_change_rate')}
    }
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all')
    await producer.start()
    try:
        raw_first = await producer.send_and_wait(
            RAW, key=KEY, value=json.dumps({'converted': ticker}).encode())
        assert raw_first.offset == 0
        await asyncio.wait_for(run('ticker', convert, bootstrap=BOOTSTRAP, once=True,
                                   group_id=group, topics=(RAW,)), 30)
    finally:
        await producer.stop()
    consumer = AIOKafkaConsumer(bootstrap_servers=BOOTSTRAP, group_id=group)
    await consumer.start()
    try:
        assert await consumer.committed(TopicPartition(RAW, 0)) == 1
    finally:
        await consumer.stop()
    ARTIFACT.write_text(json.dumps({
        'group': group, 'marker': marker, 'raw_required_offset': 1,
        'spark_required_offset': 1, 'raw_expiring_offset': 1,
        'pre_expiring_offset': 1,
    }, indent=2))
    print('RETENTION_SETUP_OK raw_committed=1 spark_next=1')


async def prepare_expiring():
    evidence = json.loads(ARTIFACT.read_text())
    marker = evidence['marker'].encode()
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all')
    await producer.start()
    try:
        raw_second = await producer.send_and_wait(RAW, key=KEY, value=b'raw-expiring-' + marker)
        pre_second = await producer.send_and_wait(PRE, key=KEY, value=b'pre-expiring-' + marker)
        assert raw_second.offset == 1 and pre_second.offset == 1
    finally:
        await producer.stop()
    print('RETENTION_UNPROCESSED_OFFSETS_OK raw=1 pre=1')


async def roll():
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all')
    await producer.start()
    try:
        raw = await producer.send_and_wait(RAW, key=KEY, value=b'raw-after-roll-' + uuid.uuid4().hex.encode())
        pre = await producer.send_and_wait(PRE, key=KEY, value=b'pre-after-roll-' + uuid.uuid4().hex.encode())
        assert raw.offset == 2 and pre.offset == 2
    finally:
        await producer.stop()
    print('RETENTION_SEGMENT_ROLL_OK raw_offset=2 pre_offset=2')


async def verify():
    evidence = json.loads(ARTIFACT.read_text())
    consumer = AIOKafkaConsumer(bootstrap_servers=BOOTSTRAP, group_id=evidence['group'])
    await consumer.start()
    try:
        starts = await offsets(consumer)
        assert starts[RAW] > evidence['raw_required_offset'], starts
        assert starts[PRE] > evidence['spark_required_offset'], starts
        evidence['beginning_offsets'] = starts
        assert await consumer.committed(TopicPartition(RAW, 0)) == 1
    finally:
        await consumer.stop()
    try:
        await asyncio.wait_for(run('ticker', convert, bootstrap=BOOTSTRAP, once=True,
                                   group_id=evidence['group'], topics=(RAW,)), 30)
    except RequiredRawOffsetExpired as error:
        evidence['raw_failure'] = str(error)
    else:
        raise AssertionError('Raw preprocessing silently resumed after required offset expired')
    ARTIFACT.write_text(json.dumps(evidence, indent=2))
    print(f'RAW_RETENTION_FAILURE_OK required=1 beginning={starts[RAW]}')


async def wait_expired():
    deadline = time.monotonic() + 120
    consumer = AIOKafkaConsumer(bootstrap_servers=BOOTSTRAP)
    await consumer.start()
    try:
        while time.monotonic() < deadline:
            starts = await offsets(consumer)
            if starts[RAW] > 1 and starts[PRE] > 1:
                print(f'RETENTION_LOG_START_OK raw={starts[RAW]} pre={starts[PRE]}')
                return
            await asyncio.sleep(2)
    finally:
        await consumer.stop()
    raise AssertionError(f'retention did not remove required offsets within 120s: {starts}')


async def main(phase):
    if phase == 'setup':
        await setup()
    elif phase == 'prepare':
        await prepare_expiring()
    elif phase == 'roll':
        await roll()
    elif phase == 'verify':
        await verify()
    elif phase == 'wait':
        await wait_expired()
    else:
        raise ValueError(phase)


if __name__ == '__main__':
    asyncio.run(main(sys.argv[1]))
