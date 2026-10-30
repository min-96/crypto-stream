"""Check Spark Processed records against Connect's MinIO Value and Key objects."""
import asyncio
import hashlib
import json
import sys
import time
from pathlib import Path

import requests
from aiokafka import AIOKafkaConsumer, TopicPartition

import check

sys.path.insert(0, '/kaf08')
from processed_contract import TOPIC_CONTRACTS, identity_for, key_for


ARTIFACTS = Path('/artifacts')
CONNECTOR = 'processed-to-minio'
ARCHIVE = ARTIFACTS / 'processed_archive_evidence.json'


def setup():
    check.ensure_buckets()
    check.wait_until('Connect REST', lambda: requests.get(
        f'{check.CONNECT}/connector-plugins', timeout=5).ok)
    config = json.loads(Path('/configs/processed-to-minio.json').read_text())
    assert set(config['topics'].split(',')) == set(TOPIC_CONTRACTS)
    assert config['store.kafka.keys'] == 'true'
    response = requests.put(
        f'{check.CONNECT}/connectors/{CONNECTOR}/config', json=config, timeout=20)
    response.raise_for_status()
    check.wait_connector(CONNECTOR)
    print('PROCESSED_CONNECTOR_READY topics=6 store.kafka.keys=true')


def restart():
    response = requests.post(
        f'{check.CONNECT}/connectors/{CONNECTOR}/restart',
        params={'includeTasks': 'true', 'onlyFailed': 'false'}, timeout=20)
    response.raise_for_status()
    check.wait_connector(CONNECTOR)
    print('PROCESSED_CONNECTOR_TASK_RESTART_OK')


async def kafka_records(count, expected):
    consumer = AIOKafkaConsumer(
        *TOPIC_CONTRACTS, bootstrap_servers=check.BOOTSTRAP,
        auto_offset_reset='earliest', enable_auto_commit=False)
    await consumer.start()
    try:
        found = {topic: {} for topic in TOPIC_CONTRACTS}
        deadline = time.monotonic() + 90
        while any(len(records) < count for records in found.values()):
            if time.monotonic() >= deadline:
                raise AssertionError(f'Processed Kafka records missing: {found}')
            batches = await consumer.getmany(timeout_ms=1000)
            for records in batches.values():
                for record in records:
                    if record.topic in found and record.offset < count:
                        assert record.partition == 0
                        found[record.topic][record.offset] = record
        for topic, records in found.items():
            assert set(records) == set(range(count)), (topic, records)
            end = await consumer.end_offsets({TopicPartition(topic, 0)})
            assert end[TopicPartition(topic, 0)] == count, (topic, end)
            item = expected[topic]
            for offset, record in records.items():
                assert record.key == item['key'].encode()
                assert record.value == item['value'].encode()
                row = json.loads(record.value)
                assert key_for(topic, row) == record.key
                assert identity_for(topic, record.key, row) == item['identity']
        return found
    finally:
        await consumer.stop()


def archived_topic(topic, records):
    def complete():
        objects = check.archived_objects('processed', topic)
        result = {}
        for offset, record in records.items():
            suffix = f'+{offset:010d}.bin'
            values = [name for name in objects if name.endswith(suffix)
                      and '.keys.' not in name and '/partition=0/' in name]
            if len(values) != 1:
                return False
            value_file = values[0]
            key_file = value_file[:-4] + '.keys.bin'
            if key_file not in objects:
                return False
            value_body, key_body = objects[value_file], objects[key_file]
            if record.value not in value_body or record.key not in key_body:
                return False
            result[str(offset)] = {
                'partition': 0, 'offset': offset,
                'kafka_key': record.key.decode(),
                'kafka_value_sha256': hashlib.sha256(record.value).hexdigest(),
                'value_file': value_file,
                'value_object_sha256': hashlib.sha256(value_body).hexdigest(),
                'key_file': key_file,
                'key_object_sha256': hashlib.sha256(key_body).hexdigest(),
                'value_contains_kafka_record': True,
                'key_contains_kafka_record': True,
            }
        value_files = [name for name in objects if name.endswith('.bin') and '.keys.' not in name]
        key_files = [name for name in objects if name.endswith('.keys.bin')]
        if len(value_files) != len(records) or len(key_files) != len(records):
            return False
        return result
    return check.wait_until(f'Processed archive {topic}', complete, timeout=120)


async def verify(phase):
    count = 2 if phase == 'before' else 4
    expected = json.loads((ARTIFACTS / 'processed_sink.json').read_text())
    assert set(expected) == set(TOPIC_CONTRACTS)
    records = await kafka_records(count, expected)
    previous = json.loads(ARCHIVE.read_text()) if phase == 'after' else None
    evidence = {'phase': phase, 'connector': CONNECTOR, 'topics': {}}
    for topic in TOPIC_CONTRACTS:
        objects = archived_topic(topic, records[topic])
        if previous:
            for offset in ('0', '1'):
                assert objects[offset] == previous['topics'][topic]['objects'][offset], topic
        evidence['topics'][topic] = {
            'identity': expected[topic]['identity'],
            'key': expected[topic]['key'],
            'offsets': list(range(count)),
            'objects': objects,
            'visible_value_copies_per_offset': {str(offset): 1 for offset in range(count)},
            'visible_key_copies_per_offset': {str(offset): 1 for offset in range(count)},
        }
        print(f'PROCESSED_ARCHIVE_OK phase={phase} topic={topic} offsets={list(range(count))}')
    if previous:
        evidence['before_restart_objects_preserved'] = True
        evidence['observed_missing_offsets'] = 0
        evidence['observed_duplicate_value_objects'] = 0
        evidence['observed_duplicate_key_objects'] = 0
    ARCHIVE.write_text(json.dumps(evidence, indent=2))
    print(f'PROCESSED_ARCHIVE_{phase.upper()}_OK topics=6')


def main(phase):
    if phase == 'setup':
        setup()
    elif phase == 'restart':
        restart()
    elif phase in ('before', 'after'):
        asyncio.run(verify(phase))
    else:
        raise ValueError(phase)


if __name__ == '__main__':
    main(sys.argv[1])
