"""AVG-09 Kafka tool, run inside the project network by avg09.py.

  produce   stdin JSON lines {"key": str, "value": object} → RegionKorea_TickerPreprocessing,
            acks=all, one send at a time; prints {"offset", "partition"} per record
  dump      prints every record of the topic as {"offset", "key", "value"} (read_committed)
"""

import asyncio
import json
import os
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

TOPIC = 'RegionKorea_TickerPreprocessing'
BOOTSTRAP = os.environ['KAFKA_BOOTSTRAP_SERVERS']


async def produce():
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all', enable_idempotence=True)
    await producer.start()
    try:
        for line in sys.stdin:
            if not line.strip():
                continue
            record = json.loads(line)
            meta = await producer.send_and_wait(TOPIC, json.dumps(record['value']).encode(),
                                                key=record['key'].encode())
            print(json.dumps({'partition': meta.partition, 'offset': meta.offset}), flush=True)
    finally:
        await producer.stop()


async def dump():
    consumer = AIOKafkaConsumer(bootstrap_servers=BOOTSTRAP, isolation_level='read_committed',
                                enable_auto_commit=False)
    await consumer.start()
    try:
        partitions = [TopicPartition(TOPIC, p) for p in sorted(await consumer.partitions_for_topic(TOPIC) or ())]
        consumer.assign(partitions)
        await consumer.seek_to_beginning(*partitions)
        end = await consumer.end_offsets(partitions)
        while any(await consumer.position(tp) < end[tp] for tp in partitions):
            batch = await consumer.getmany(timeout_ms=1000)
            for records in batch.values():
                for r in records:
                    print(json.dumps({'partition': r.partition, 'offset': r.offset, 'key': r.key.decode(),
                                      'value': json.loads(r.value)}), flush=True)
    finally:
        await consumer.stop()


asyncio.run({'produce': produce, 'dump': dump}[sys.argv[1]]())
