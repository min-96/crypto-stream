"""Independently consume and verify KAF-08 Spark Kafka sink output."""
import asyncio
import hashlib
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from aiokafka import AIOKafkaConsumer, TopicPartition

sys.path.insert(0, '/kaf08')
from processed_contract import TOPIC_CONTRACTS, identity_for, key_for


BOOTSTRAP = os.environ['KAFKA_BOOTSTRAP_SERVERS']
ARTIFACTS = Path('/artifacts')


async def main():
    expected = json.loads((ARTIFACTS / 'processed_sink.json').read_text())
    assert set(expected) == set(TOPIC_CONTRACTS)
    consumer = AIOKafkaConsumer(
        *TOPIC_CONTRACTS, bootstrap_servers=BOOTSTRAP,
        auto_offset_reset='earliest', enable_auto_commit=False)
    await consumer.start()
    try:
        seen = {topic: [] for topic in TOPIC_CONTRACTS}
        deadline = time.monotonic() + 90
        while any(len(records) < 2 for records in seen.values()) and time.monotonic() < deadline:
            batches = await consumer.getmany(timeout_ms=1000)
            for records in batches.values():
                for record in records:
                    if record.topic not in seen:
                        raise AssertionError(f'unexpected topic: {record.topic}')
                    seen[record.topic].append(record)
        evidence = {}
        for topic, records in seen.items():
            assert len(records) == 2, (topic, len(records))
            item = expected[topic]
            assert item['key'].encode() == records[0].key == records[1].key
            assert records[0].value == records[1].value == item['value'].encode()
            assert records[0].offset < records[1].offset
            identities = []
            for record in records:
                assert record.partition == 0
                row = json.loads(record.value)
                assert isinstance(row, dict)
                assert set(row) == set(item['fields'])
                for name, kind in item['field_types'].items():
                    value = row[name]
                    if kind in ('double', 'float'):
                        assert isinstance(value, (int, float)) and not isinstance(value, bool)
                    elif kind in ('int', 'bigint'):
                        assert isinstance(value, int) and not isinstance(value, bool)
                    elif kind == 'timestamp':
                        assert isinstance(value, str) and value.endswith('Z')
                        datetime.fromisoformat(value.replace('Z', '+00:00'))
                    elif kind == 'string':
                        assert isinstance(value, str)
                    else:
                        raise AssertionError(f'unsupported wire type {kind}')
                assert key_for(topic, row) == record.key
                identity = identity_for(topic, record.key, row)
                assert identity == item['identity']
                assert hashlib.sha256(record.value).hexdigest() == item['value_sha256']
                identities.append(identity)
            assert identities[0] == identities[1]
            end_offsets = await consumer.end_offsets({TopicPartition(topic, 0)})
            assert end_offsets[TopicPartition(topic, 0)] == records[1].offset + 1
            evidence[topic] = {
                'schema': item['schema'], 'key': item['key'],
                'value_sha256': item['value_sha256'], 'identity': item['identity'],
                'offsets': [record.offset for record in records],
                'same_key_value_identity': True,
            }
            print(f'PROCESSED_SINK_OK topic={topic} offsets={records[0].offset},{records[1].offset}')
        (ARTIFACTS / 'processed_sink_evidence.json').write_text(json.dumps(evidence, indent=2))
        print('PROCESSED_SINK_REPLAY_OK topics=6 duplicate_pairs=6')
    finally:
        await consumer.stop()


if __name__ == '__main__':
    asyncio.run(main())
