"""Compare S3 ByteArrayFormat files with the Kafka records they archive."""

import re
import base64
import json
from collections import defaultdict


OBJECT_NAME = re.compile(
    r'^topics/([^/]+)/partition=(\d+)/\1\+\2\+(\d+)\.bin$')


def _lines(body):
    # The connector inserts the platform line separator between byte-array records.
    # These test records are JSON and ASCII keys, both without embedded newlines.
    parts = body.split(b'\n')
    if parts[-1] == b'':
        parts.pop()
    assert parts and all(parts), 'empty ByteArrayFormat record'
    return parts


def compare_objects(expected_records, value_objects, key_objects, *, offset_envelope=False):
    """Return a full byte comparison; mark offsets that S3 alone cannot recover."""
    expected = defaultdict(list)
    for record in expected_records:
        assert b'\n' not in record.key and b'\n' not in record.value
        expected[(record.topic, record.partition)].append(record)
    for records in expected.values():
        records.sort(key=lambda record: record.offset)
        assert len({record.offset for record in records}) == len(records)

    assert set(key_objects) == {name[:-4] + '.keys.bin' for name in value_objects}, \
        'Value/Key object names differ'
    grouped = defaultdict(list)
    for name, body in value_objects.items():
        match = OBJECT_NAME.fullmatch(name)
        assert match, f'unknown S3 object name: {name}'
        topic, partition, first_offset = match.groups()
        grouped[(topic, int(partition))].append((int(first_offset), name, body))
    assert set(grouped) <= set(expected), 'unexpected archived topic/partition'

    matched_offsets = []
    multi_record_objects = 0
    for partition_key, records in expected.items():
        cursor = 0
        for first_offset, name, body in sorted(grouped.get(partition_key, [])):
            assert cursor < len(records), f'extra S3 object: {name}'
            assert records[cursor].offset == first_offset, f'first offset mismatch: {name}'
            values = _lines(body)
            keys = _lines(key_objects[name[:-4] + '.keys.bin'])
            assert len(values) == len(keys), f'Key/Value count mismatch: {name}'
            multi_record_objects += len(values) > 1
            for key, value in zip(keys, values):
                assert cursor < len(records), f'extra archived record: {name}'
                record = records[cursor]
                if offset_envelope:
                    envelope = json.loads(value)
                    assert envelope['archive_format'] == 1
                    assert envelope['offset'] == record.offset, \
                        f'Kafka offset mismatch: {name}, expected {record.offset}'
                    value = base64.b64decode(envelope['value_b64'], validate=True)
                assert key == record.key and value == record.value, \
                    f'Kafka bytes mismatch: {name}, expected offset {record.offset}'
                matched_offsets.append((record.topic, record.partition, record.offset))
                cursor += 1

    # Only the first offset is encoded in each filename. Transaction control
    # records can make following Kafka data offsets nonconsecutive.
    return {
        'expected_records': len(expected_records),
        'matched_records': len(matched_offsets),
        'value_objects': len(value_objects),
        'key_objects': len(key_objects),
        'multi_record_objects': multi_record_objects,
        'all_kafka_records_matched': len(matched_offsets) == len(expected_records),
        'offsets_recoverable_from_s3_alone': offset_envelope or multi_record_objects == 0,
    }


def fetch_objects(client, value_paths=None, key_paths=None):
    values, keys = {}, {}
    if value_paths is not None and key_paths is not None:
        for name in value_paths:
            values[name] = client.get_object(Bucket='preprocessed', Key=name)['Body'].read()
        for name in key_paths:
            keys[name] = client.get_object(Bucket='preprocessed', Key=name)['Body'].read()
        return values, keys
    token = None
    while True:
        options = {'Bucket': 'preprocessed', 'Prefix': 'topics/'}
        if token:
            options['ContinuationToken'] = token
        page = client.list_objects_v2(**options)
        for item in page.get('Contents', []):
            name = item['Key']
            if name.endswith('.bin'):
                body = client.get_object(Bucket='preprocessed', Key=name)['Body'].read()
                (keys if name.endswith('.keys.bin') else values)[name] = body
        token = page.get('NextContinuationToken')
        if not token:
            return values, keys
