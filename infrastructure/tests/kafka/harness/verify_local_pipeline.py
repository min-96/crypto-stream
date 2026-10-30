"""Check both mock exchange streams through Kafka, MinIO and MySQL."""

import json
import os
import subprocess
import sys
import time
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
from pathlib import Path


HERE = Path(__file__).resolve().parent
PROJECT = os.environ.get('LOCAL_PIPELINE_PROJECT', 'coin-market-local-pipeline')
COMPOSE = ['docker', 'compose', '--project-name', PROJECT,
           '-f', str(HERE / 'compose.yaml'),
           '-f', str(HERE / 'compose.local-pipeline.yaml')]
EXCHANGES = {'UPBIT', 'BITHUMB'}
FIELDS = ('opening_price', 'trade_price', 'max_price', 'min_price',
          'prev_closing_price', 'acc_trade_volume_24h',
          'signed_change_price', 'signed_change_rate')


def dc(*args):
    result = subprocess.run([*COMPOSE, *args], text=True, capture_output=True,
                            timeout=90, check=True)
    return result.stdout


def kafka_records(topic):
    output = dc('exec', '-T', 'kafka-1', 'kafka-console-consumer',
                '--bootstrap-server', 'localhost:9092', '--topic', topic,
                '--from-beginning', '--max-messages', '30', '--timeout-ms', '10000',
                '--consumer-property', 'isolation.level=read_committed',
                '--property', 'print.key=true')
    records = []
    for line in output.splitlines():
        key, payload = line.split('\t', 1)
        records.append((key, json.loads(payload)))
    return records


def recent_preprocessed_records():
    offsets = dc('exec', '-T', 'kafka-1', 'kafka-run-class',
                 'kafka.tools.GetOffsetShell', '--broker-list', 'localhost:9092',
                 '--topic', 'RegionKorea_TickerPreprocessing', '--time', '-1')
    records = []
    for line in offsets.splitlines():
        _topic, partition, end = line.rsplit(':', 2)
        end = int(end)
        start = max(0, end - 400)
        if end == start:
            continue
        output = dc('exec', '-T', 'kafka-1', 'kafka-console-consumer',
                    '--bootstrap-server', 'localhost:9092',
                    '--topic', 'RegionKorea_TickerPreprocessing',
                    '--partition', partition, '--offset', str(start),
                    '--max-messages', str(min(220, end - start)), '--timeout-ms', '5000',
                    '--consumer-property', 'isolation.level=read_committed',
                    '--property', 'print.key=true')
        for record in output.splitlines():
            key, payload = record.split('\t', 1)
            records.append((key, json.loads(payload)))
    return records


def mysql_average():
    sql = ('SELECT name,time,' + ','.join(FIELDS)
           + ' FROM coin_average ORDER BY time DESC LIMIT 1;')
    output = dc('exec', '-T', 'mysql', 'sh', '-c',
                'mysql -uroot -p"$MYSQL_ROOT_PASSWORD" coin_market -N -e "' + sql + '"')
    values = output.strip().split('\t')
    if len(values) != len(FIELDS) + 2:
        raise AssertionError(f'expected one complete MySQL average row: {output!r}')
    return dict(zip(('name', 'time', *FIELDS), values))


def expected_from_preprocessed(records, time_ms):
    values = {market: {field: [] for field in FIELDS} for market in EXCHANGES}
    for key, record in records:
        market = record.get('market')
        if market not in EXCHANGES or key != f'{market}:BTC:KRW':
            continue
        event_ms = round(record['timestamp'] * 1000)
        if not time_ms - 60000 <= event_ms < time_ms:
            continue
        for field in FIELDS:
            values[market][field].append(Decimal(record['data'][field]))
    counts = {market: len(values[market]['trade_price']) for market in EXCHANGES}
    if any(count == 0 for count in counts.values()):
        raise AssertionError(f'Kafka input missing one market in MySQL window: {counts}')
    with localcontext() as context:
        context.prec = 80
        context.rounding = ROUND_HALF_EVEN
        expected = {}
        for field in FIELDS:
            market_means = [sum(values[market][field], Decimal(0)) / counts[market]
                            for market in sorted(EXCHANGES)]
            expected[field] = (sum(market_means, Decimal(0)) / len(market_means)).quantize(
                Decimal('0.000000000000000001'))
        message_weighted_trade_price = (
            sum((sum(values[market]['trade_price'], Decimal(0)) for market in EXCHANGES),
                Decimal(0)) / sum(counts.values())).quantize(
                    Decimal('0.000000000000000001'))
    return expected, counts, message_weighted_trade_price


def main():
    deadline = time.monotonic() + 180
    average = None
    counts = None
    while time.monotonic() < deadline:
        try:
            candidate = mysql_average()
            records = recent_preprocessed_records()
            _, candidate_counts, _ = expected_from_preprocessed(records, int(candidate['time']))
            if len(set(candidate_counts.values())) > 1:
                average, counts = candidate, candidate_counts
                break
        except AssertionError:
            pass  # The first closed window or both exchange inputs may not be ready yet.
        time.sleep(5)
    if average is None:
        raise AssertionError('no closed BTC window with unequal two-exchange Kafka input counts')

    raw = kafka_records('koreaSocketDataInBTC-ticker')
    preprocessed = kafka_records('RegionKorea_TickerPreprocessing')
    raw_exchanges = {key.split(':')[0] for key, frame in raw
                     if frame.get('type') == 'ticker' and frame.get('code') == 'KRW-BTC'}
    pre_exchanges = {record.get('market') for key, record in preprocessed
                     if record.get('coin_symbol') == 'BTC' and key.startswith(('UPBIT:', 'BITHUMB:'))}
    if raw_exchanges != EXCHANGES or pre_exchanges != EXCHANGES:
        raise AssertionError(f'Kafka exchanges: Raw={raw_exchanges}, Preprocessed={pre_exchanges}')

    status = json.loads(dc('exec', '-T', 'connect', 'curl', '-fsS',
                           'http://localhost:8083/connectors/preprocessed-to-minio/status'))
    if status['connector']['state'] != 'RUNNING' or not status['tasks'] or any(
            task['state'] != 'RUNNING' for task in status['tasks']):
        raise AssertionError(f'Connect not running: {status}')

    archive = dc('run', '--rm', '--no-deps', '--entrypoint', '/bin/sh', 'init-buckets',
                 '-c', 'mc alias set local http://minio:9000 harness-local '
                 'harness-local-only >/dev/null && '
                 'mc ls --recursive local/preprocessed/topics/RegionKorea_TickerPreprocessing/')
    if '.keys.bin' not in archive or '.bin' not in archive:
        raise AssertionError('Preprocessed Value/Key objects missing from MinIO')

    expected, counts, message_weighted_trade_price = expected_from_preprocessed(
        recent_preprocessed_records(), int(average['time']))
    mismatches = {field: (average[field], format(expected[field], 'f')) for field in FIELDS
                  if Decimal(average[field]) != expected[field]}
    if mismatches:
        raise AssertionError(f'MySQL differs from Kafka input average: {mismatches}')
    if expected['trade_price'] == message_weighted_trade_price:
        raise AssertionError('unequal-rate test did not distinguish equal and message weighting')
    print(json.dumps({'result': 'LOCAL_PIPELINE_OK', 'raw_exchanges': sorted(raw_exchanges),
                      'preprocessed_exchanges': sorted(pre_exchanges),
                      'mysql_window_end_ms': int(average['time']),
                      'mysql_trade_price': average['trade_price'],
                      'message_weighted_trade_price': format(message_weighted_trade_price, 'f'),
                      'input_counts_by_exchange': counts,
                      'independently_checked_fields': list(FIELDS),
                      'connect_state': status['connector']['state'],
                      'minio_preprocessed_objects': archive.count('.bin')},
                     ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except (AssertionError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        print(f'LOCAL_PIPELINE_FAILED: {error}', file=sys.stderr)
        raise SystemExit(1)
