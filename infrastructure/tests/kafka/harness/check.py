"""Live Kafka/Connect/MinIO checks for the isolated Compose project."""
import asyncio
import hashlib
import json
import logging
import os
import sys
import time
import uuid
from pathlib import Path

import boto3
import requests
import websockets
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError

sys.path.insert(0, '/code')
import collector


BOOTSTRAP = os.environ['KAFKA_BOOTSTRAP_SERVERS']
CONNECT = os.environ['CONNECT_URL']
RAW_TOPIC = 'asiaSocketDataInBTC-ticker'
PRE_TOPIC = 'RegionAsia_TickerPreprocessing'
PROCESSED = (
    'TimeMetricsProcessedCoin', 'ArbitrageProcessedCoin',
    'OrderbookProcessedRegionCoin', 'OrderbookProcessedAllRegionCoin',
    'MarketArbitrageProcessedCoin', 'MarketArbitrageSignalProcessedCoin',
)
KEY = b'BINANCE:BTC:USDT'
ARTIFACTS = Path('/artifacts')


def s3():
    return boto3.client('s3', endpoint_url=os.environ['MINIO_ENDPOINT'],
                        aws_access_key_id='harness-local',
                        aws_secret_access_key='harness-local-only',
                        region_name='us-east-1')


def wait_until(label, check, timeout=90):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except Exception as error:
            last_error = error
        time.sleep(2)
    raise AssertionError(f'{label} did not become ready within {timeout}s: {last_error}')


def ensure_buckets():
    client = s3()
    wait_until('MinIO', lambda: client.list_buckets() is not None)
    for name in ('preprocessed', 'processed'):
        try:
            client.create_bucket(Bucket=name)
        except client.exceptions.BucketAlreadyOwnedByYou:
            pass


def register_connectors():
    wait_until('Connect REST', lambda: requests.get(f'{CONNECT}/connector-plugins', timeout=5).ok)
    for name in ('preprocessed-to-minio', 'processed-to-minio'):
        config = json.loads(Path(f'/configs/{name}.json').read_text())
        response = requests.put(f'{CONNECT}/connectors/{name}/config', json=config, timeout=20)
        response.raise_for_status()
        wait_connector(name)


def wait_connector(name):
    def running():
        response = requests.get(f'{CONNECT}/connectors/{name}/status', timeout=5)
        if not response.ok:
            return False
        status = response.json()
        return status['connector']['state'] == 'RUNNING' and all(
            task['state'] == 'RUNNING' for task in status['tasks']) and bool(status['tasks'])
    wait_until(f'{name} RUNNING', running)


async def produce(topic, key, value, timeout=15):
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all',
                                request_timeout_ms=min(8000, (timeout - 1) * 1000))
    await asyncio.wait_for(producer.start(), timeout)
    try:
        return await asyncio.wait_for(producer.send_and_wait(topic, key=key, value=value), timeout)
    finally:
        try:
            await asyncio.wait_for(producer.stop(), 3)
        except asyncio.TimeoutError:
            # A failed in-flight write must not keep the failure test running indefinitely.
            pass


async def produce_after_election(topic, key, value):
    last_error = None
    for _ in range(6):
        try:
            return await produce(topic, key, value)
        except Exception as error:
            last_error = error
            await asyncio.sleep(3)
    raise AssertionError('acks=all did not recover after leader election') from last_error


async def read_marker(topic, marker, timeout=20):
    consumer = AIOKafkaConsumer(topic, bootstrap_servers=BOOTSTRAP,
                                auto_offset_reset='earliest', enable_auto_commit=False)
    await asyncio.wait_for(consumer.start(), timeout)
    try:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            records = await consumer.getmany(timeout_ms=1000)
            for partition_records in records.values():
                for record in partition_records:
                    if marker in record.value:
                        return record
        raise AssertionError(f'Kafka marker missing in {topic}: {marker!r}')
    finally:
        await consumer.stop()


async def check_collector():
    marker = f'collector-{uuid.uuid4().hex}'.encode()
    frame = b'{ "marker":"' + marker + b'", "pair":"BTCUSDT" }\n'
    acknowledgement = asyncio.Event()
    metadata = []

    class TrackingProducer:
        def __init__(self, delegate):
            self.delegate = delegate

        async def send_and_wait(self, *args, **kwargs):
            result = await self.delegate.send_and_wait(*args, **kwargs)
            metadata.append(result)
            acknowledgement.set()
            return result

    async def websocket_handler(socket):
        await socket.send(frame)
        await asyncio.sleep(10)

    consumer = AIOKafkaConsumer(RAW_TOPIC, bootstrap_servers=BOOTSTRAP,
                                auto_offset_reset='earliest', enable_auto_commit=False)
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all')
    await consumer.start()
    await producer.start()
    try:
        async with websockets.serve(websocket_handler, '127.0.0.1', 0) as server:
            port = server.sockets[0].getsockname()[1]
            source = {'name': 'harness-binance-btc', 'url': f'ws://127.0.0.1:{port}',
                      'topic': RAW_TOPIC, 'key': KEY.decode(), 'subscribe': []}
            task = asyncio.create_task(collector.collect(source, TrackingProducer(producer)))
            try:
                await asyncio.wait_for(acknowledgement.wait(), 20)
                # Earlier isolated preprocessing fixtures may share this Raw topic.
                while True:
                    record = await asyncio.wait_for(consumer.getone(), 20)
                    if marker in record.value:
                        break
                assert record.key == KEY, (record.key, KEY)
                assert record.value == frame, (record.value, frame)
                assert record.offset == metadata[0].offset
                assert record.partition == metadata[0].partition
                print(f'COLLECTOR_OK topic={record.topic} partition={record.partition} '
                      f'offset={record.offset} key={record.key.decode()} marker={marker.decode()}')
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    finally:
        await producer.stop()
        await consumer.stop()


def archived_objects(bucket, topic):
    client = s3()
    objects = client.list_objects_v2(Bucket=bucket, Prefix=f'topics/{topic}/').get('Contents', [])
    return {item['Key']: client.get_object(Bucket=bucket, Key=item['Key'])['Body'].read()
            for item in objects}


def wait_archive(bucket, topic, marker, key=None, offset=None, value=None):
    def found():
        objects = archived_objects(bucket, topic)
        value_files = [name for name, body in objects.items()
                       if name.endswith('.bin') and '.keys.' not in name and marker in body]
        for value_file in value_files:
            if offset is not None and not value_file.endswith(f'+{offset:010d}.bin'):
                continue
            key_file = value_file[:-4] + '.keys.bin'
            if (key is None or key in objects.get(key_file, b'')) and (value is None or value in objects[value_file]):
                return {'value_file': value_file, 'key_file': key_file if key else None}
        return False
    result = wait_until(f'{bucket}/{topic} marker and key', found, 90)
    print(f'ARCHIVE_OK bucket={bucket} topic={topic} files={json.dumps(result)}')
    return result


def archive_evidence(topic, offset, key, value, files):
    objects = archived_objects('preprocessed', topic)
    value_body = objects[files['value_file']]
    key_body = objects[files['key_file']]
    assert value in value_body and key in key_body
    return {
        'topic': topic, 'partition': 0, 'offset': offset,
        'kafka_key': key.decode('ascii'),
        'kafka_value_sha256': hashlib.sha256(value).hexdigest(),
        'value_file': files['value_file'],
        'value_object_sha256': hashlib.sha256(value_body).hexdigest(),
        'key_file': files['key_file'],
        'key_object_sha256': hashlib.sha256(key_body).hexdigest(),
        'value_contains_kafka_record': True,
        'key_contains_kafka_record': True,
    }


async def check_archives():
    ensure_buckets()
    register_connectors()
    marker = f'pre-{uuid.uuid4().hex}'.encode()
    first_marker = marker
    value = b'{"market":"BINANCE","coin_symbol":"BTC","marker":"' + marker + b'"}'
    sent = await produce(PRE_TOPIC, KEY, value)
    received = await read_marker(PRE_TOPIC, marker)
    assert received.key == KEY and received.value == value and received.offset == sent.offset
    first_files = wait_archive('preprocessed', PRE_TOPIC, marker, KEY, sent.offset, value)
    first_evidence = archive_evidence(PRE_TOPIC, sent.offset, KEY, value, first_files)

    # These are transport fixtures. The six final Processed wire contracts remain undecided.
    for topic in PROCESSED:
        marker = f'processed-{uuid.uuid4().hex}'.encode()
        value = b'{"marker":"' + marker + b'"}'
        sent = await produce(topic, KEY, value)
        received = await read_marker(topic, marker)
        assert received.key == KEY and received.value == value and received.offset == sent.offset
        wait_archive('processed', topic, marker, offset=sent.offset)

    response = requests.post(f'{CONNECT}/connectors/preprocessed-to-minio/restart',
                             params={'includeTasks': 'true', 'onlyFailed': 'false'}, timeout=20)
    response.raise_for_status()
    wait_connector('preprocessed-to-minio')
    marker = f'pre-after-restart-{uuid.uuid4().hex}'.encode()
    value = b'{"marker":"' + marker + b'"}'
    sent = await produce(PRE_TOPIC, KEY, value)
    received = await read_marker(PRE_TOPIC, marker)
    assert received.key == KEY and received.value == value and received.offset == sent.offset
    second_files = wait_archive('preprocessed', PRE_TOPIC, marker, KEY, sent.offset, value)
    second_evidence = archive_evidence(PRE_TOPIC, sent.offset, KEY, value, second_files)
    archived = archived_objects('preprocessed', PRE_TOPIC)
    # Verify the earlier marker still has one archived Value object after restart.
    first_marker_copies = [name for name, body in archived.items()
                           if name.endswith('.bin') and '.keys.' not in name and first_marker in body]
    assert len(first_marker_copies) == 1
    assert first_marker_copies[0] == first_files['value_file']
    (ARTIFACTS / 'archive_evidence.json').write_text(json.dumps({
        'before_restart': first_evidence,
        'after_restart': second_evidence,
        'before_restart_value_copies_after_restart': len(first_marker_copies),
    }, indent=2))
    print('PREPROCESSED_ARCHIVE_EVIDENCE_OK')
    print('CONNECT_RESTART_OK connector=preprocessed-to-minio')

    response = requests.post(f'{CONNECT}/connectors/processed-to-minio/restart',
                             params={'includeTasks': 'true', 'onlyFailed': 'false'}, timeout=20)
    response.raise_for_status()
    wait_connector('processed-to-minio')
    marker = f'processed-after-restart-{uuid.uuid4().hex}'.encode()
    sent = await produce(PROCESSED[0], KEY, b'{"marker":"' + marker + b'"}')
    received = await read_marker(PROCESSED[0], marker)
    assert received.key == KEY and received.offset == sent.offset
    wait_archive('processed', PROCESSED[0], marker, offset=sent.offset)
    print('CONNECT_RESTART_OK connector=processed-to-minio')


def check_spark_archive():
    result = json.loads((ARTIFACTS / 'spark.json').read_text())
    expected_topics = {
        'RegionKorea_TickerPreprocessing', 'RegionKorea_OrderbookPreprocessing',
        'RegionNE_TickerPreprocessing', 'RegionNE_OrderbookPreprocessing',
        'RegionAsia_TickerPreprocessing', 'RegionAsia_OrderbookPreprocessing',
    }
    assert set(result['topics']) == expected_topics
    assert len({entry['checkpoint'] for entry in result['topics'].values()}) == 6
    evidence = {}
    for topic, entry in result['topics'].items():
        assert entry['second_offset'] > entry['first_offset']
        assert entry['replay_count'] == 0
        files = {}
        for phase in ('first', 'second'):
            value = entry[f'{phase}_value'].encode()
            marker = entry[f'{phase}_marker'].encode()
            files[phase] = wait_archive(
                'preprocessed', topic, marker, KEY, entry[f'{phase}_offset'], value)
        evidence[topic] = {
            'checkpoint': entry['checkpoint'],
            'first_offset': entry['first_offset'],
            'second_offset': entry['second_offset'],
            'replay_count': entry['replay_count'],
            'first_marker': entry['first_marker'],
            'second_marker': entry['second_marker'],
            'archive_files': files,
        }
    (ARTIFACTS / 'spark_connect_evidence.json').write_text(json.dumps(evidence, indent=2))
    print('SPARK_CONNECT_INDEPENDENT_OK topics=6')


async def failover_phase(phase):
    path = ARTIFACTS / 'failover.json'
    if phase == 'baseline':
        marker = f'failover-{uuid.uuid4().hex}'.encode()
        result = await produce(RAW_TOPIC, KEY, marker)
        path.write_text(json.dumps({'marker': marker.decode(), 'offset': result.offset}))
        print(f'BASELINE_ACK_OK offset={result.offset}')
    elif phase == 'one-down':
        result = await produce_after_election(RAW_TOPIC, KEY, b'one-down-' + uuid.uuid4().hex.encode())
        print(f'ONE_DOWN_ACK_OK offset={result.offset}')
    elif phase == 'two-down':
        kafka_logger = logging.getLogger('aiokafka')
        previous_level = kafka_logger.level
        kafka_logger.setLevel(logging.CRITICAL)
        try:
            await produce(RAW_TOPIC, KEY, b'two-down-' + uuid.uuid4().hex.encode(), timeout=6)
        except (KafkaError, asyncio.TimeoutError, OSError) as error:
            print(f'TWO_DOWN_WRITE_FAILED_AS_EXPECTED type={type(error).__name__}')
        else:
            raise AssertionError('acks=all unexpectedly succeeded with two KRaft nodes stopped')
        finally:
            kafka_logger.setLevel(previous_level)
    elif phase == 'recovered':
        prior = json.loads(path.read_text())
        record = await read_marker(RAW_TOPIC, prior['marker'].encode())
        assert record.key == KEY and record.offset == prior['offset']
        result = await produce_after_election(RAW_TOPIC, KEY, b'recovered-' + uuid.uuid4().hex.encode())
        print(f'RECOVERY_OK preserved_offset={record.offset} new_ack_offset={result.offset}')
    else:
        raise ValueError(phase)


def main():
    phase = sys.argv[1]
    if phase == 'pipeline':
        asyncio.run(check_collector())
        asyncio.run(check_archives())
    elif phase == 'spark-archive':
        check_spark_archive()
    else:
        asyncio.run(failover_phase(phase))


if __name__ == '__main__':
    main()
