"""Exercise 9 synthetic exchanges x 35 pairs through Collector and downstream Kafka paths."""
import asyncio
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests
import websockets
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

sys.path.insert(0, '/code')
sys.path.insert(0, '/code/preprocessing')
import collector
from consumer import run as run_preprocessor


BOOTSTRAP = os.environ['KAFKA_BOOTSTRAP_SERVERS']
ARTIFACTS = Path('/artifacts')
KINDS = ('ticker', 'orderbook')
EXCHANGES = tuple(f'E{number:02d}' for number in range(1, 10))
COINS = tuple(f'C{number:02d}' for number in range(1, 36))
UPDATES = 2
PER_KIND = len(EXCHANGES) * len(COINS) * UPDATES
TOTAL = len(KINDS) * PER_KIND
REGIONS = ('Korea', 'NE', 'Asia')


def region_for(exchange):
    return REGIONS[(int(exchange[1:]) - 1) // 3]


def raw_topic(exchange, kind):
    prefix = {'Korea': 'korea', 'NE': 'ne', 'Asia': 'asia'}[region_for(exchange)]
    return f'{prefix}SocketDataInBTC-{kind}'


def pre_topic(exchange, kind):
    return f'Region{region_for(exchange)}_{kind.title()}Preprocessing'


RAW_TOPICS = tuple(raw_topic(exchange, kind) for kind in KINDS
                   for exchange in ('E01', 'E04', 'E07'))
PRE_TOPICS = tuple(pre_topic(exchange, kind) for kind in KINDS
                   for exchange in ('E01', 'E04', 'E07'))


def specs(kind):
    return [(exchange, coin) for exchange in EXCHANGES for coin in COINS]


def template_for(kind):
    template = json.loads(Path(f'/code/preprocessing/tests/fixtures/upbit_{kind}.json').read_text())
    if kind == 'orderbook':
        unit = template['orderbook_units'][0]
        template['orderbook_units'] = [
            {**unit, 'ask_price': unit['ask_price'] + index * 1000,
             'bid_price': unit['bid_price'] - index * 1000} for index in range(30)]
    return template


async def generate_kind(kind, producer, run_id):
    sources = specs(kind)
    template = template_for(kind)
    ready = asyncio.Event()
    begin = asyncio.Event()
    done = asyncio.Event()
    connected = set()
    connections = {index: 0 for index in range(len(sources))}
    sent = 0
    acknowledged = 0
    first_sent = None
    last_ack = None
    send_start = None
    frame_bytes = None
    partition_counts = {}

    class TrackingProducer:
        async def send_and_wait(self, topic, *, key, value):
            nonlocal acknowledged, last_ack
            metadata = await producer.send_and_wait(topic, key=key, value=value)
            acknowledged += 1
            last_ack = time.monotonic()
            label = f'{topic}:{metadata.partition}'
            partition_counts[label] = partition_counts.get(label, 0) + 1
            return metadata

    async def handler(socket):
        nonlocal sent, first_sent, frame_bytes
        index = int(socket.path.strip('/'))
        assert 0 <= index < len(sources)
        connections[index] += 1
        connected.add(index)
        if len(connected) == len(sources):
            ready.set()
        await begin.wait()
        exchange, coin = sources[index]
        for update in range(UPDATES):
            scheduled = send_start + update + index / len(sources)
            await asyncio.sleep(max(0, scheduled - time.monotonic()))
            frame = dict(template)
            frame['code'] = f'KRW-{coin}'
            frame['timestamp'] = int(time.time() * 1000)
            frame['bench_run'] = run_id
            frame['bench_update'] = update
            payload = json.dumps(frame, separators=(',', ':')).encode()
            if first_sent is None:
                first_sent = time.monotonic()
                frame_bytes = len(payload)
            await socket.send(payload)
            sent += 1
        await done.wait()

    server = await websockets.serve(handler, '127.0.0.1', 0, backlog=1024)
    port = server.sockets[0].getsockname()[1]
    tasks = []
    try:
        for index, (exchange, coin) in enumerate(sources):
            source = {'name': f'bench-{kind}-{exchange}-{coin}',
                      'url': f'ws://127.0.0.1:{port}/{index}',
                      'topic': raw_topic(exchange, kind),
                      'key': f'{exchange}:{coin}:KRW', 'subscribe': []}
            tasks.append(asyncio.create_task(collector.collect(source, TrackingProducer())))
        await asyncio.wait_for(ready.wait(), 90)
        send_start = time.monotonic() + 0.2
        begin.set()
        deadline = send_start + UPDATES + 90
        while acknowledged < PER_KIND and time.monotonic() < deadline:
            for task in tasks:
                if task.done() and not task.cancelled():
                    task.result()
            await asyncio.sleep(0.05)
        assert sent == acknowledged == PER_KIND, (kind, sent, acknowledged, PER_KIND)
        assert all(count == 1 for count in connections.values()), 'WebSocket reconnect during load'
        return {'kind': kind, 'keys': len(sources), 'sent': sent, 'acknowledged': acknowledged,
                'frame_bytes_sample': frame_bytes,
                'ack_elapsed_seconds': round(last_ack - first_sent, 3),
                'ack_rate_per_second': round(PER_KIND / (last_ack - first_sent), 2),
                'partition_counts': partition_counts}
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        done.set()
        server.close()
        await server.wait_closed()


async def read_raw(run_id):
    consumer = AIOKafkaConsumer(bootstrap_servers=BOOTSTRAP, enable_auto_commit=False,
                                auto_offset_reset='earliest')
    await consumer.start()
    try:
        partitions = []
        for topic in RAW_TOPICS:
            partition_ids = consumer.partitions_for_topic(topic)
            assert partition_ids is not None
            partitions.extend(TopicPartition(topic, pid) for pid in partition_ids)
        consumer.assign(partitions)
        await consumer.seek_to_beginning()
        seen = set()
        topic_counts = {topic: 0 for topic in RAW_TOPICS}
        deadline = time.monotonic() + 90
        while len(seen) < TOTAL and time.monotonic() < deadline:
            batches = await consumer.getmany(timeout_ms=1000, max_records=2000)
            for batch in batches.values():
                for record in batch:
                    frame = json.loads(record.value)
                    assert frame['bench_run'] == run_id
                    exchange, coin, quote = record.key.decode().split(':')
                    kind = frame['type']
                    assert quote == 'KRW' and frame['code'] == f'KRW-{coin}'
                    assert record.topic == raw_topic(exchange, kind)
                    identity = (kind, exchange, coin, frame['bench_update'])
                    assert identity not in seen, f'duplicate Raw frame {identity}'
                    seen.add(identity)
                    topic_counts[record.topic] += 1
        assert len(seen) == TOTAL, (len(seen), TOTAL)
        assert set(topic_counts.values()) == {3 * len(COINS) * UPDATES}
        return topic_counts
    finally:
        await consumer.stop()


async def raw_phase():
    run_id = uuid.uuid4().hex
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all',
                                enable_idempotence=True)
    await producer.start()
    try:
        runs = [await generate_kind(kind, producer, run_id) for kind in KINDS]
    finally:
        await producer.stop()
    counts = await read_raw(run_id)
    result = {'run_id': run_id, 'exchange_count': len(EXCHANGES),
              'coins_per_exchange': len(COINS), 'updates_per_key_and_kind': UPDATES,
              'total_raw_records': TOTAL, 'runs': runs, 'raw_topic_counts': counts}
    (ARTIFACTS / 'bench_315_raw.json').write_text(json.dumps(result, indent=2) + '\n')
    print(f'BENCH_315_RAW_OK keys_per_kind={len(EXCHANGES) * len(COINS)} records={TOTAL}')


def convert_ticker(raw, key):
    frame = json.loads(raw)
    exchange, coin, quote = key.decode().split(':')
    assert frame['type'] == 'ticker' and frame['code'] == f'{quote}-{coin}'
    mapping = {'opening_price': 'opening_price', 'trade_price': 'trade_price',
               'max_price': 'high_price', 'min_price': 'low_price',
               'prev_closing_price': 'prev_closing_price',
               'acc_trade_volume_24h': 'acc_trade_volume_24h',
               'signed_change_price': 'signed_change_price',
               'signed_change_rate': 'signed_change_rate'}
    return {'market': exchange, 'timestamp': frame['timestamp'] / 1000,
            'coin_symbol': coin,
            'data': {target: str(frame[source]) for target, source in mapping.items()}}


def convert_orderbook(raw, key):
    frame = json.loads(raw)
    exchange, coin, quote = key.decode().split(':')
    assert frame['type'] == 'orderbook' and frame['code'] == f'{quote}-{coin}'
    units = frame['orderbook_units']
    timestamp = datetime.fromtimestamp(frame['timestamp'] / 1000, tz=timezone.utc)
    return [{'region': region_for(exchange), 'market': exchange, 'symbol': coin,
             'highest_bid': max(unit['bid_price'] for unit in units),
             'lowest_ask': min(unit['ask_price'] for unit in units),
             'total_bid_volume': frame['total_bid_size'],
             'total_ask_volume': frame['total_ask_size'],
             'timestamp': timestamp.isoformat().replace('+00:00', 'Z')}]


def s3_client():
    return boto3.client('s3', endpoint_url=os.environ['MINIO_ENDPOINT'],
                        aws_access_key_id='harness-local',
                        aws_secret_access_key='harness-local-only',
                        region_name='us-east-1')


def register_connector():
    client = s3_client()
    try:
        client.create_bucket(Bucket='preprocessed')
    except client.exceptions.BucketAlreadyOwnedByYou:
        pass
    config = json.loads(Path('/configs/preprocessed-to-minio.json').read_text())
    response = requests.put(f'{os.environ["CONNECT_URL"]}/connectors/preprocessed-to-minio/config',
                            json=config, timeout=30)
    response.raise_for_status()
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        status = requests.get(f'{os.environ["CONNECT_URL"]}/connectors/preprocessed-to-minio/status', timeout=5)
        if status.ok:
            body = status.json()
            if body['connector']['state'] == 'RUNNING' and body['tasks'] and all(
                    task['state'] == 'RUNNING' for task in body['tasks']):
                return client
        time.sleep(2)
    detail = status.text if 'status' in locals() else 'status unavailable'
    (ARTIFACTS / 'preprocessed_connector_status.json').write_text(detail + '\n')
    raise AssertionError(f'preprocessed Connector did not become RUNNING: {detail}')


def archive_inventory(client):
    value_paths, key_paths = set(), set()
    value_bytes = key_bytes = 0
    token = None
    while True:
        options = {'Bucket': 'preprocessed', 'Prefix': 'topics/'}
        if token:
            options['ContinuationToken'] = token
        result = client.list_objects_v2(**options)
        for item in result.get('Contents', []):
            path = item['Key']
            if path.endswith('.keys.bin'):
                key_paths.add(path)
                key_bytes += item['Size']
            elif path.endswith('.bin'):
                value_paths.add(path)
                value_bytes += item['Size']
        token = result.get('NextContinuationToken')
        if not token:
            return value_paths, key_paths, value_bytes, key_bytes


def archive_paths(client):
    values, keys, _, _ = archive_inventory(client)
    return values, keys


def ensure_connector_running():
    try:
        response = requests.get(
            f'{os.environ["CONNECT_URL"]}/connectors/preprocessed-to-minio/status', timeout=3)
        response.raise_for_status()
        body = response.json()
        assert body['connector']['state'] == 'RUNNING' and body['tasks'] and all(
            task['state'] == 'RUNNING' for task in body['tasks']), body
    except (requests.RequestException, AssertionError) as error:
        raise RuntimeError('preprocessed Connector is not RUNNING during catch-up') from error


def expected_paths(records):
    values, keys = set(), set()
    for record in records:
        topic, partition, offset = record['topic'], record['partition'], record['offset']
        base = f'topics/{topic}/partition={partition}/{topic}+{partition}+{offset:010d}'
        values.add(base + '.bin')
        keys.add(base + '.keys.bin')
    return values, keys


async def preprocessed_phase():
    raw = json.loads((ARTIFACTS / 'bench_315_raw.json').read_text())
    assert raw['total_raw_records'] == TOTAL
    s3 = register_connector()
    monitor = AIOKafkaConsumer(bootstrap_servers=BOOTSTRAP, enable_auto_commit=False,
                               auto_offset_reset='earliest', isolation_level='read_committed')
    await monitor.start()
    workers = []
    started = time.monotonic()
    try:
        partitions = []
        for topic in PRE_TOPICS:
            ids = monitor.partitions_for_topic(topic)
            assert ids is not None
            partitions.extend(TopicPartition(topic, pid) for pid in ids)
        monitor.assign(partitions)
        await monitor.seek_to_beginning()
        for kind, converter in (('ticker', convert_ticker), ('orderbook', convert_orderbook)):
            topics = tuple(raw_topic(exchange, kind) for exchange in ('E01', 'E04', 'E07'))
            for index in (1, 2):
                workers.append(asyncio.create_task(run_preprocessor(
                    kind, converter, bootstrap=BOOTSTRAP, instance_id=f'bench-{kind}-{index}',
                    group_id=f'bench-315-{kind}', topics=topics)))
        records = []
        identities = set()
        counts = {topic: 0 for topic in PRE_TOPICS}
        samples = [{'seconds': 0.0, 'processed': 0, 'raw_pending': TOTAL,
                    'archived_values': 0, 'archive_pending': TOTAL}]
        next_sample = started + 5
        preprocess_done = None
        archive_done = None
        deadline = started + 600
        while time.monotonic() < deadline:
            if len(records) < TOTAL:
                batches = await monitor.getmany(timeout_ms=500, max_records=2000)
                for batch in batches.values():
                    for record in batch:
                        exchange, coin, _ = record.key.decode().split(':')
                        kind = 'ticker' if 'Ticker' in record.topic else 'orderbook'
                        assert record.topic == pre_topic(exchange, kind)
                        value = json.loads(record.value)
                        timestamp = value['timestamp'] if kind == 'ticker' else value[0]['timestamp']
                        identity = (kind, exchange, coin, timestamp)
                        assert identity not in identities, f'duplicate Preprocessed {identity}'
                        identities.add(identity)
                        counts[record.topic] += 1
                        records.append({'topic': record.topic, 'partition': record.partition,
                                        'offset': record.offset, 'key': record.key.decode()})
                for worker in workers:
                    if worker.done() and not worker.cancelled():
                        worker.result()
                if len(records) == TOTAL:
                    preprocess_done = time.monotonic()
                    for worker in workers:
                        worker.cancel()
                    await asyncio.gather(*workers, return_exceptions=True)
            if time.monotonic() >= next_sample:
                value_paths, key_paths = await asyncio.to_thread(archive_paths, s3)
                now = time.monotonic()
                samples.append({'seconds': round(now - started, 2),
                                'processed': len(records), 'raw_pending': TOTAL - len(records),
                                'archived_values': len(value_paths),
                                'archive_pending': TOTAL - len(value_paths)})
                (ARTIFACTS / 'bench_315_progress.json').write_text(json.dumps({
                    'samples': samples, 'preprocessed_topic_counts': counts,
                    'archive_key_objects': len(key_paths)}, indent=2) + '\n')
                ensure_connector_running()
                next_sample = now + 5
                if preprocess_done is not None and len(value_paths) >= TOTAL and len(key_paths) >= TOTAL:
                    archive_done = now
                    break
            await asyncio.sleep(0.1)
        assert preprocess_done is not None and archive_done is not None, samples[-1]
        assert set(counts.values()) == {3 * len(COINS) * UPDATES}, counts
        expected_values, expected_keys = expected_paths(records)
        assert value_paths == expected_values, ('Value object mismatch', len(value_paths), len(expected_values))
        assert key_paths == expected_keys, ('Key object mismatch', len(key_paths), len(expected_keys))
        for topic in PRE_TOPICS:
            representative = next(record for record in records if record['topic'] == topic)
            _, paths = expected_paths([representative])
            body = s3.get_object(Bucket='preprocessed', Key=next(iter(paths)))['Body'].read()
            assert representative['key'].encode() in body
        result = {'total_raw': TOTAL, 'preprocessed_records': len(records),
                  'archived_value_objects': len(value_paths), 'archived_key_objects': len(key_paths),
                  'preprocessed_topic_counts': counts,
                  'preprocessing_catchup_seconds': round(preprocess_done - started, 2),
                  'archive_catchup_seconds': round(archive_done - started, 2),
                  'samples': samples, 'records': records}
        (ARTIFACTS / 'bench_315_downstream.json').write_text(json.dumps(result, indent=2) + '\n')
        print(f'BENCH_315_DOWNSTREAM_OK processed={len(records)} archived={len(value_paths)} '
              f'pre_seconds={result["preprocessing_catchup_seconds"]} '
              f'archive_seconds={result["archive_catchup_seconds"]}')
    finally:
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        await monitor.stop()


if __name__ == '__main__':
    phase = sys.argv[1]
    if phase == 'raw':
        asyncio.run(raw_phase())
    elif phase == 'preprocessed':
        asyncio.run(preprocessed_phase())
    else:
        raise SystemExit(f'unknown phase: {phase}')
