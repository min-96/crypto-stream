"""Sustained 9 x 35 synthetic WebSocket load through preprocessing and Connect."""
import asyncio
import json
import os
import re
import sys
import time
import uuid
from pathlib import Path

import websockets
import requests
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

from bench_315_pipeline import (ARTIFACTS, BOOTSTRAP, COINS, EXCHANGES, KINDS,
                                PRE_TOPICS, RAW_TOPICS, archive_inventory,
                                convert_orderbook, convert_ticker, ensure_connector_running,
                                pre_topic, raw_topic, register_connector, region_for,
                                specs, template_for)
from verify_batch_archive import compare_objects, fetch_objects

sys.path.insert(0, '/code')
sys.path.insert(0, '/code/preprocessing')
import collector
from consumer import run as run_preprocessor

DURATION = int(os.environ.get('BENCH_DURATION_SECONDS', '75'))
UPDATES = int(os.environ.get('BENCH_UPDATES_PER_KEY', '2'))
PREPROCESS_INSTANCES = int(os.environ.get('BENCH_PREPROCESS_INSTANCES', '2'))
PREPROCESS_BATCH_SIZE = int(os.environ.get('BENCH_PREPROCESS_BATCH_SIZE', '32'))
PREPROCESS_BATCH_LINGER_MS = int(os.environ.get('BENCH_PREPROCESS_BATCH_LINGER_MS', '250'))
MINIO_METRICS = os.environ.get('BENCH_MINIO_METRICS') == '1'
ARCHIVE_BATCH = os.environ.get('BENCH_ARCHIVE_BATCH') == '1'
ARCHIVE_OFFSET_ENVELOPE = os.environ.get('BENCH_ARCHIVE_OFFSET_ENVELOPE') == '1'
RESTART_CONNECT_SECONDS = int(os.environ.get('BENCH_RESTART_CONNECT_SECONDS', '0'))
TOTAL = len(KINDS) * len(EXCHANGES) * len(COINS) * UPDATES
SAMPLE_SECONDS = 5
DRAIN_SECONDS = int(os.environ.get('BENCH_DRAIN_SECONDS', '240'))
assert DURATION >= 30
assert UPDATES >= 2
assert 1 <= PREPROCESS_INSTANCES <= 18


def minio_s3_metrics():
    """Read server-side S3 request counters from this isolated MinIO instance."""
    response = requests.get(os.environ['MINIO_ENDPOINT'] + '/minio/metrics/v3/api/requests',
                            timeout=5)
    response.raise_for_status()
    fields = {'minio_api_requests_total': 'count',
              'minio_api_requests_duration_total': 'duration_ms',
              'minio_api_requests_errors_total': 'errors'}
    operations = {}
    for line in response.text.splitlines():
        match = re.match(r'([^ {]+)\{([^}]*)\}\s+([0-9.eE+-]+)$', line)
        if not match or match.group(1) not in fields:
            continue
        labels = dict(re.findall(r'(\w+)="([^"]*)"', match.group(2)))
        if labels.get('type') != 's3':
            continue
        operation = labels.get('name') or labels.get('api')
        if operation:
            operations.setdefault(operation, {})[fields[match.group(1)]] = float(match.group(3))
    if not operations:
        raise RuntimeError('MinIO S3 API metrics unavailable')
    return operations


def restart_connector():
    url = os.environ['CONNECT_URL'] + '/connectors/preprocessed-to-minio'
    response = requests.post(url + '/restart?includeTasks=true&onlyFailed=false', timeout=15)
    response.raise_for_status()
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        status = requests.get(url + '/status', timeout=5)
        if status.ok:
            body = status.json()
            if body['connector']['state'] == 'RUNNING' and body['tasks'] and all(
                    task['state'] == 'RUNNING' for task in body['tasks']):
                return {'http_status': response.status_code, 'task_count': len(body['tasks'])}
        time.sleep(1)
    raise AssertionError('Connector did not recover after restart')


async def generate(kind, producer, run_id, counters, benchmark_started):
    sources = specs(kind)
    template = template_for(kind)
    connected = set()
    ready = asyncio.Event()
    begin = asyncio.Event()
    done = asyncio.Event()
    connections = [0] * len(sources)
    start = None

    class TrackingProducer:
        async def send_and_wait(self, topic, *, key, value):
            metadata = await producer.send_and_wait(topic, key=key, value=value)
            counters['acked'] += 1
            return metadata

    async def handler(socket):
        index = int(socket.path.strip('/'))
        connections[index] += 1
        connected.add(index)
        if len(connected) == len(sources):
            ready.set()
        await begin.wait()
        exchange, coin = sources[index]
        for update in range(UPDATES):
            scheduled = start + (update * len(sources) + index) * DURATION / (UPDATES * len(sources))
            await asyncio.sleep(max(0, scheduled - time.monotonic()))
            frame = dict(template)
            frame.update(code=f'KRW-{coin}', timestamp=int(time.time() * 1000),
                         bench_run=run_id, bench_update=update)
            await socket.send(json.dumps(frame, separators=(',', ':')).encode())
            counters['sent'] += 1
        await done.wait()

    server = await websockets.serve(handler, '127.0.0.1', 0, backlog=1024)
    port = server.sockets[0].getsockname()[1]
    tasks = []
    try:
        for index, (exchange, coin) in enumerate(sources):
            source = {'name': f'sustained-{kind}-{exchange}-{coin}',
                      'url': f'ws://127.0.0.1:{port}/{index}',
                      'topic': raw_topic(exchange, kind),
                      'key': f'{exchange}:{coin}:KRW', 'subscribe': []}
            tasks.append(asyncio.create_task(collector.collect(source, TrackingProducer())))
        await asyncio.wait_for(ready.wait(), 90)
        start = time.monotonic() + 0.2
        counters['started_seconds'] = round(start - benchmark_started, 2)
        begin.set()
        deadline = start + DURATION + 90
        while counters['acked'] < len(sources) * UPDATES and time.monotonic() < deadline:
            for task in tasks:
                if task.done() and not task.cancelled():
                    task.result()
            await asyncio.sleep(0.1)
        assert counters['sent'] == counters['acked'] == len(sources) * UPDATES, counters
        assert all(count == 1 for count in connections), 'WebSocket reconnect during load'
        counters['finished_seconds'] = round(time.monotonic() - benchmark_started, 2)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        done.set()
        server.close()
        await server.wait_closed()


async def main():
    run_id = uuid.uuid4().hex
    s3 = register_connector()
    monitor = AIOKafkaConsumer(bootstrap_servers=BOOTSTRAP, enable_auto_commit=False,
                               auto_offset_reset='earliest', isolation_level='read_committed')
    await monitor.start()
    inspectors = {
        name: AIOKafkaConsumer(bootstrap_servers=BOOTSTRAP, group_id=group,
                               enable_auto_commit=False, isolation_level='read_committed')
        for name, group in (('ticker', 'bench-sustained-ticker'),
                            ('orderbook', 'bench-sustained-orderbook'),
                            ('connect', 'connect-preprocessed-to-minio'))}
    for inspector in inspectors.values():
        await inspector.start()
    workers = []
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all', enable_idempotence=True)
    await producer.start()
    started = time.monotonic()
    counters = {kind: {'sent': 0, 'acked': 0} for kind in KINDS}
    records = set()
    kafka_records = []
    topic_counts = {topic: 0 for topic in PRE_TOPICS}
    samples = []
    archive_done_at = None
    restart_evidence = None
    restart_grace_until = 0
    minio_initial = await asyncio.to_thread(minio_s3_metrics) if MINIO_METRICS else None
    try:
        partitions = [TopicPartition(topic, pid) for topic in PRE_TOPICS
                      for pid in monitor.partitions_for_topic(topic)]
        raw_partitions = {kind: [TopicPartition(topic, pid)
                                 for topic in RAW_TOPICS if topic.endswith(kind)
                                 for pid in monitor.partitions_for_topic(topic)] for kind in KINDS}
        monitor.assign(partitions)
        await monitor.seek_to_beginning()
        for kind, converter in (('ticker', convert_ticker), ('orderbook', convert_orderbook)):
            topics = tuple(raw_topic(exchange, kind) for exchange in ('E01', 'E04', 'E07'))
            for index in range(1, PREPROCESS_INSTANCES + 1):
                workers.append(asyncio.create_task(run_preprocessor(
                    kind, converter, bootstrap=BOOTSTRAP, instance_id=f'sustained-{kind}-{index}',
                    group_id=f'bench-sustained-{kind}', topics=topics,
                    batch_size=PREPROCESS_BATCH_SIZE,
                    batch_linger_ms=PREPROCESS_BATCH_LINGER_MS)))
        start = time.monotonic() + 2
        generators = [asyncio.create_task(generate(kind, producer, run_id, counters[kind], started))
                      for kind in KINDS]
        deadline = start + DURATION + DRAIN_SECONDS + 90
        next_sample = start
        archived = 0
        while time.monotonic() < deadline:
            if RESTART_CONNECT_SECONDS and restart_evidence is None \
                    and time.monotonic() - started >= RESTART_CONNECT_SECONDS:
                restart_evidence = await asyncio.to_thread(restart_connector)
                restart_evidence['requested_at_seconds'] = round(time.monotonic() - started, 2)
                restart_grace_until = time.monotonic() + 45
            batches = await monitor.getmany(timeout_ms=500, max_records=2000)
            for batch in batches.values():
                for record in batch:
                    exchange, coin, quote = record.key.decode().split(':')
                    kind = 'ticker' if 'Ticker' in record.topic else 'orderbook'
                    assert quote == 'KRW' and record.topic == pre_topic(exchange, kind)
                    value = json.loads(record.value)
                    timestamp = value['timestamp'] if kind == 'ticker' else value[0]['timestamp']
                    identity = (kind, exchange, coin, timestamp)
                    assert identity not in records, f'duplicate Preprocessed {identity}'
                    records.add(identity)
                    kafka_records.append(record)
                    topic_counts[record.topic] += 1
            for task in workers + generators:
                if task.done() and not task.cancelled():
                    task.result()
            if time.monotonic() >= next_sample:
                values, keys, value_bytes, key_bytes = await asyncio.to_thread(archive_inventory, s3)
                acked = sum(value['acked'] for value in counters.values())
                committed_lag = {}
                for name, assigned in [(kind, raw_partitions[kind]) for kind in KINDS] + [('connect', partitions)]:
                    inspector = inspectors[name]
                    ends = await inspector.end_offsets(assigned)
                    commits = await asyncio.gather(*(inspector.committed(tp) for tp in assigned))
                    committed_lag[name] = sum(max(0, ends[tp] - (offset or 0))
                                              for tp, offset in zip(assigned, commits))
                archive_check = None
                if ARCHIVE_BATCH and all(task.done() for task in generators) and len(kafka_records) == TOTAL \
                        and committed_lag['connect'] <= len(partitions) \
                        and keys == {name[:-4] + '.keys.bin' for name in values}:
                    value_objects, key_objects = await asyncio.to_thread(fetch_objects, s3, values, keys)
                    archive_check = compare_objects(kafka_records, value_objects, key_objects,
                                                    offset_envelope=ARCHIVE_OFFSET_ENVELOPE)
                archived = (archive_check['matched_records'] if archive_check else 0) \
                    if ARCHIVE_BATCH else len(values)
                sample = {'seconds': round(time.monotonic() - started, 2),
                          'raw_sent': sum(value['sent'] for value in counters.values()),
                          'raw_acked': acked, 'preprocessed': len(records),
                          'archived_values': archived if not ARCHIVE_BATCH or archive_check else None,
                          'archived_keys': len(keys) if not ARCHIVE_BATCH else
                          (archive_check['matched_records'] if archive_check else None),
                          'archived_value_bytes': value_bytes, 'archived_key_bytes': key_bytes,
                          'raw_to_preprocessed_lag': acked - len(records),
                          'preprocessed_to_archive_lag': len(records) - archived
                          if not ARCHIVE_BATCH or archive_check else None,
                          'preprocessing_group_committed_lag': committed_lag['ticker'] + committed_lag['orderbook'],
                          'connect_group_committed_lag': committed_lag['connect']}
                if ARCHIVE_BATCH:
                    sample['archive_value_objects'] = len(values)
                    sample['archive_key_objects'] = len(keys)
                    sample['archive_check'] = archive_check
                if MINIO_METRICS:
                    sample['minio_s3_api'] = await asyncio.to_thread(minio_s3_metrics)
                samples.append(sample)
                if archive_done_at is None and len(records) == archived == TOTAL \
                        and (ARCHIVE_BATCH or len(keys) == TOTAL):
                    archive_done_at = sample['seconds']
                (ARTIFACTS / 'bench_315_sustained_progress.json').write_text(
                    json.dumps({'run_id': run_id, 'samples': samples}, indent=2) + '\n')
                if time.monotonic() >= restart_grace_until:
                    ensure_connector_running()
                next_sample = time.monotonic() + SAMPLE_SECONDS
                if (all(task.done() for task in generators)
                        and len(records) == archived == TOTAL
                        and (ARCHIVE_BATCH or len(keys) == TOTAL)
                        and committed_lag['ticker'] == committed_lag['orderbook'] == 0
                        and committed_lag['connect'] <= len(partitions)
                        and time.monotonic() >= restart_grace_until):
                    break
            await asyncio.sleep(0.1)
        assert all(task.done() for task in generators), 'generation did not finish'
        for task in generators:
            task.result()
        assert len(records) == archived == TOTAL, samples[-1]
        if RESTART_CONNECT_SECONDS:
            assert restart_evidence, 'Connector restart did not run'
        ensure_connector_running()
        if ARCHIVE_BATCH:
            assert archive_check and archive_check['all_kafka_records_matched'], samples[-1]
        assert set(topic_counts.values()) == {3 * len(COINS) * UPDATES}, topic_counts
        assert samples[-1]['preprocessing_group_committed_lag'] == 0
        assert samples[-1]['connect_group_committed_lag'] <= len(partitions)
        finished = max(c['finished_seconds'] for c in counters.values())
        result = {'run_id': run_id, 'duration_seconds': DURATION,
                  'updates_per_key_and_kind': UPDATES, 'total_records': TOTAL,
                  'preprocessing_instances_per_kind': PREPROCESS_INSTANCES,
                  'preprocessing_batch_size': PREPROCESS_BATCH_SIZE,
                  'preprocessing_batch_linger_ms': PREPROCESS_BATCH_LINGER_MS,
                  'target_rate_per_second': round(TOTAL / DURATION, 3),
                  'counters': counters, 'preprocessed_topic_counts': topic_counts,
                  'samples': samples,
                  'archive_batch_check': archive_check if ARCHIVE_BATCH else None,
                  'connector_restart': restart_evidence,
                  'minio_s3_api_delta': {
                      operation: {field: round(value - minio_initial.get(operation, {}).get(field, 0), 3)
                                  for field, value in fields.items()}
                      for operation, fields in samples[-1].get('minio_s3_api', {}).items()
                  } if MINIO_METRICS else None,
                  'terminal_connect_control_offset_floor': len(partitions),
                  'archive_catchup_after_production_seconds': max(0, round(archive_done_at - finished, 2)),
                  'commit_catchup_after_production_seconds': max(0, round(samples[-1]['seconds'] - finished, 2))}
        (ARTIFACTS / 'bench_315_sustained.json').write_text(json.dumps(result, indent=2) + '\n')
        print(f'KAF315_SUSTAINED_OK records={TOTAL} duration={DURATION} '
              f'archive_drain={result["archive_catchup_after_production_seconds"]} '
              f'commit_drain={result["commit_catchup_after_production_seconds"]}')
    finally:
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        await producer.stop()
        await monitor.stop()
        for inspector in inspectors.values():
            await inspector.stop()


if __name__ == '__main__':
    asyncio.run(main())
