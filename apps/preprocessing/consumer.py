"""Run a single Ticker or Orderbook consumer group with a supplied KRW converter.

The converter is a Python callable ``(raw_bytes, original_key_bytes) -> schema value``.
It owns exchange normalization and KRW conversion. The Kafka boundary validates
its result before publishing. A Ticker with an invalid value (null, non-decimal,
non-finite, or a non-positive price) is logged and skipped; its Raw offset still
commits. Any other failed conversion, validation, or publish stops consumption.
"""

import asyncio
import importlib
import os
import re
import sys
import time

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

from pipeline import process_record
from offsets import publish_then_commit, publish_batch_transactionally


TOPICS = {
    'ticker': ('koreaSocketDataInBTC-ticker', 'neSocketDataInBTC-ticker', 'asiaSocketDataInBTC-ticker'),
    'orderbook': ('koreaSocketDataInBTC-orderbook', 'neSocketDataInBTC-orderbook', 'asiaSocketDataInBTC-orderbook'),
}


class RequiredRawOffsetExpired(RuntimeError):
    """The group's next Raw offset is below the topic's retained beginning."""


async def ensure_committed_offsets_available(consumer, topics):
    """Fail before processing when retention removed a group's next Raw record."""
    for topic in topics:
        partitions = consumer.partitions_for_topic(topic)
        if partitions is None:
            raise RuntimeError(f'Raw topic unavailable: {topic}')
        for partition in partitions:
            tp = TopicPartition(topic, partition)
            committed = await consumer.committed(tp)
            if committed is None:
                continue
            beginning = (await consumer.beginning_offsets([tp]))[tp]
            if committed < beginning:
                raise RequiredRawOffsetExpired(
                    f'required Raw offset {committed} expired: {topic} partition {partition}, '
                    f'earliest retained offset {beginning}')


async def run_at_least_once(kind, convert, *, bootstrap, once=False, group_id=None,
                            after_ack=None, producer_factory=AIOKafkaProducer, topics=None):
    """Legacy delivery path retained for KAF-05 regression fixtures."""
    if kind not in TOPICS:
        raise ValueError(f'unknown preprocessing kind: {kind}')
    subscribed_topics = TOPICS[kind] if topics is None else topics
    consumer = AIOKafkaConsumer(*subscribed_topics, bootstrap_servers=bootstrap,
                                group_id=group_id or f'coin-market-preprocessing-{kind}',
                                enable_auto_commit=False, auto_offset_reset='earliest')
    await consumer.start()
    try:
        await ensure_committed_offsets_available(consumer, subscribed_topics)
        producer = producer_factory(bootstrap_servers=bootstrap, acks='all',
                                    enable_idempotence=True, max_request_size=1_000_000)
        await producer.start()
        try:
            async for raw in consumer:
                await publish_then_commit(
                    raw, lambda record: process_record(record, producer, convert),
                    consumer, after_ack=after_ack)
                if once:
                    return
        finally:
            await producer.stop()
    finally:
        await consumer.stop()


async def run(kind, convert, *, bootstrap, instance_id, once=False, group_id=None,
              after_ack=None, producer_factory=AIOKafkaProducer, topics=None,
              batch_size=32, batch_linger_ms=250):
    """Consume Raw and commit a bounded batch of Preprocessed writes atomically."""
    if kind not in TOPICS:
        raise ValueError(f'unknown preprocessing kind: {kind}')
    if not isinstance(instance_id, str) or not re.fullmatch(r'[A-Za-z0-9._-]{1,64}', instance_id):
        raise ValueError('PREPROCESS_INSTANCE_ID must be 1-64 stable ASCII identifier characters')
    if not 1 <= batch_size <= 1000 or not 0 <= batch_linger_ms <= 5000:
        raise ValueError('batch_size must be 1-1000 and batch_linger_ms must be 0-5000')
    subscribed_topics = TOPICS[kind] if topics is None else topics
    group = group_id or f'coin-market-preprocessing-{kind}'
    transactional_id = f'{group}-{instance_id}'
    consumer = AIOKafkaConsumer(
        *subscribed_topics, bootstrap_servers=bootstrap, group_id=group,
        enable_auto_commit=False, auto_offset_reset='earliest',
        isolation_level='read_committed')
    await consumer.start()
    try:
        await ensure_committed_offsets_available(consumer, subscribed_topics)
        producer = producer_factory(
            bootstrap_servers=bootstrap, acks='all',
            transactional_id=transactional_id, max_request_size=1_000_000)
        await producer.start()
        try:
            while True:
                records = await fetch_batch(
                    consumer, 1 if once else batch_size,
                    0 if once else batch_linger_ms)
                if not records:
                    continue
                await publish_batch_transactionally(
                    records, lambda record: process_record(record, producer, convert),
                    producer, group, after_ack=after_ack)
                if once:
                    return
        finally:
            await producer.stop()
    finally:
        await consumer.stop()


async def fetch_batch(consumer, batch_size, linger_ms):
    """Collect up to batch_size records, waiting at most linger_ms after the first."""
    records = []
    deadline = None
    while len(records) < batch_size:
        timeout_ms = 1000 if deadline is None else max(0, int((deadline - time.monotonic()) * 1000))
        if deadline is not None and timeout_ms == 0:
            break
        batches = await consumer.getmany(timeout_ms=timeout_ms,
                                         max_records=batch_size - len(records))
        for partition_records in batches.values():
            records.extend(partition_records)
        if records and deadline is None:
            deadline = time.monotonic() + linger_ms / 1000
    return records


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in TOPICS:
        raise SystemExit('usage: python consumer.py ticker|orderbook module:function')
    instance_id = os.environ.get('PREPROCESS_INSTANCE_ID')
    if not instance_id:
        raise SystemExit('PREPROCESS_INSTANCE_ID is required for transactional preprocessing')
    module, separator, name = sys.argv[2].partition(':')
    if not separator or not module or not name:
        raise SystemExit('converter must be module:function')
    convert = getattr(importlib.import_module(module), name)
    asyncio.run(run(sys.argv[1], convert,
                    bootstrap=os.environ['KAFKA_BOOTSTRAP_SERVERS'],
                    instance_id=instance_id,
                    once=os.environ.get('PREPROCESS_ONCE') == '1',
                    batch_size=int(os.environ.get('PREPROCESS_BATCH_SIZE', '32')),
                    batch_linger_ms=int(os.environ.get('PREPROCESS_BATCH_LINGER_MS', '250'))))


if __name__ == '__main__':
    main()
