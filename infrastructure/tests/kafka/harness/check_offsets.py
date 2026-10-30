"""KAF-05 fault injection against an isolated Kafka cluster."""

import asyncio
import json
import os
import sys
import uuid

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

sys.path.insert(0, '/code/preprocessing')
from consumer import run_at_least_once as run


BOOTSTRAP = os.environ['KAFKA_BOOTSTRAP_SERVERS']
RAW = 'asiaSocketDataInBTC-ticker'
PRE = 'RegionAsia_TickerPreprocessing'
KEY = b'BINANCE:BTC:USDT'


def convert(raw, key):
    assert key == KEY
    return json.loads(raw)['converted']


class NoAckProducer(AIOKafkaProducer):
    async def send_and_wait(self, *args, **kwargs):
        raise RuntimeError('injected before produce ACK')


async def committed(group):
    consumer = AIOKafkaConsumer(bootstrap_servers=BOOTSTRAP, group_id=group,
                                enable_auto_commit=False)
    await consumer.start()
    try:
        return await consumer.committed(TopicPartition(RAW, 0))
    finally:
        await consumer.stop()


async def end_offset():
    consumer = AIOKafkaConsumer(bootstrap_servers=BOOTSTRAP,
                                enable_auto_commit=False)
    await consumer.start()
    try:
        return (await consumer.end_offsets([TopicPartition(PRE, 0)]))[TopicPartition(PRE, 0)]
    finally:
        await consumer.stop()


async def expected_failure(coro, message):
    try:
        await asyncio.wait_for(coro, 30)
    except RuntimeError as error:
        assert message in str(error), str(error)
    else:
        raise AssertionError(f'expected {message}')


async def main():
    initial = await end_offset()
    group = f'kaf05-no-ack-{uuid.uuid4().hex}'
    await expected_failure(run('ticker', convert, bootstrap=BOOTSTRAP, once=True,
                               group_id=group, topics=(RAW,), producer_factory=NoAckProducer),
                           'before produce ACK')
    assert await committed(group) is None
    assert await end_offset() == initial
    await asyncio.wait_for(run('ticker', convert, bootstrap=BOOTSTRAP, once=True,
                               group_id=group, topics=(RAW,)), 30)
    assert await committed(group) == 1
    assert await end_offset() == initial + 1
    print('PREPROCESS_NO_ACK_NO_COMMIT_OK')

    initial = await end_offset()
    group = f'kaf05-after-ack-{uuid.uuid4().hex}'
    def interrupt(_raw):
        raise RuntimeError('injected after produce ACK')
    await expected_failure(run('ticker', convert, bootstrap=BOOTSTRAP, once=True,
                               group_id=group, topics=(RAW,), after_ack=interrupt), 'after produce ACK')
    assert await committed(group) is None
    assert await end_offset() == initial + 1
    await asyncio.wait_for(run('ticker', convert, bootstrap=BOOTSTRAP, once=True,
                               group_id=group, topics=(RAW,)), 30)
    assert await committed(group) == 1
    assert await end_offset() == initial + 2
    print('PREPROCESS_AFTER_ACK_REPLAY_OK')

    initial = await end_offset()
    group = f'kaf05-prior-failure-{uuid.uuid4().hex}'
    source = AIOKafkaConsumer(RAW, bootstrap_servers=BOOTSTRAP,
                              auto_offset_reset='earliest', enable_auto_commit=False)
    await source.start()
    try:
        first = await asyncio.wait_for(source.getone(), 20)
    finally:
        await source.stop()
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all')
    await producer.start()
    try:
        second = await producer.send_and_wait(RAW, key=first.key, value=first.value)
        assert second.offset == first.offset + 1
    finally:
        await producer.stop()
    seen = []
    def reject_first(raw, key):
        seen.append(raw)
        raise RuntimeError('injected first offset failure')
    await expected_failure(run('ticker', reject_first, bootstrap=BOOTSTRAP,
                               group_id=group, topics=(RAW,)), 'first offset failure')
    assert len(seen) == 1
    assert await committed(group) is None
    assert await end_offset() == initial
    print('PREPROCESS_PRIOR_OFFSET_BLOCKS_COMMIT_OK')


if __name__ == '__main__':
    asyncio.run(main())
