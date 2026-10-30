"""Measure the real Collector path from paced WebSocket frames to Kafka Raw reads."""
import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path

import websockets
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

sys.path.insert(0, '/code')
import collector


BOOTSTRAP = os.environ['KAFKA_BOOTSTRAP_SERVERS']
COINS = ('BTC', 'ETH', 'XRP', 'SOL', 'ADA', 'DOGE', 'DOT', 'LINK',
         'AVAX', 'ATOM', 'TRX', 'LTC')
TOPICS = {'ticker': 'koreaSocketDataInBTC-ticker',
          'orderbook': 'koreaSocketDataInBTC-orderbook'}


def percentile(values, fraction):
    ordered = sorted(values)
    if not ordered:
        return None
    return round(ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))], 3)


async def run(kind, partitions, target_rate, duration):
    topic = TOPICS[kind]
    run_id = uuid.uuid4().hex
    total = target_rate * duration
    template = json.loads(Path(f'/code/preprocessing/tests/fixtures/upbit_{kind}.json').read_text())
    if kind == 'orderbook':
        unit = template['orderbook_units'][0]
        template['orderbook_units'] = [
            {**unit, 'ask_price': unit['ask_price'] + i * 1000,
             'bid_price': unit['bid_price'] - i * 1000} for i in range(30)]

    consumer = AIOKafkaConsumer(bootstrap_servers=BOOTSTRAP,
                                enable_auto_commit=False, auto_offset_reset='latest')
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP,
                                acks='all', enable_idempotence=True)
    await consumer.start()
    try:
        pids = consumer.partitions_for_topic(topic)
        assert pids is not None and len(pids) == partitions, (topic, pids, partitions)
        consumer.assign([TopicPartition(topic, pid) for pid in sorted(pids)])
        await consumer.seek_to_end()
        await producer.start()
        try:
            sent = 0
            acknowledged = 0
            consumed = 0
            ack_ms = []
            end_to_end_ms = []
            seen = set()
            connections = {coin: 0 for coin in COINS}
            partition_counts = {str(pid): 0 for pid in sorted(pids)}
            max_socket_backlog = 0
            max_kafka_backlog = 0
            done = asyncio.Event()
            start = time.monotonic()
            first_send = None
            last_ack = None
            frame_size = None

            class TrackingProducer:
                async def send_and_wait(self, *args, **kwargs):
                    nonlocal acknowledged, last_ack
                    began = time.monotonic_ns()
                    metadata = await producer.send_and_wait(*args, **kwargs)
                    ack_ms.append((time.monotonic_ns() - began) / 1_000_000)
                    acknowledged += 1
                    last_ack = time.monotonic()
                    return metadata

            async def socket_handler(socket, source_index):
                nonlocal sent, first_send, frame_size
                for index in range(source_index, total, len(COINS)):
                    await asyncio.sleep(max(0, start + index / target_rate - time.monotonic()))
                    frame = dict(template)
                    frame['code'] = f'KRW-{COINS[source_index]}'
                    frame['bench_run'] = run_id
                    frame['bench_index'] = index
                    frame['bench_sent_ns'] = time.monotonic_ns()
                    payload = json.dumps(frame, separators=(',', ':')).encode()
                    if first_send is None:
                        first_send = time.monotonic()
                        frame_size = len(payload)
                    await socket.send(payload)
                    sent += 1
                await done.wait()

            async def sample_backlogs():
                nonlocal max_socket_backlog, max_kafka_backlog
                while not done.is_set():
                    max_socket_backlog = max(max_socket_backlog, sent - acknowledged)
                    max_kafka_backlog = max(max_kafka_backlog, acknowledged - consumed)
                    await asyncio.sleep(0.2)

            server_contexts = []
            collector_tasks = []
            sampler = None
            try:
                for source_index, coin in enumerate(COINS):
                    async def handler(socket, index=source_index):
                        connections[COINS[index]] += 1
                        await socket_handler(socket, index)
                    server = await websockets.serve(handler, '127.0.0.1', 0)
                    server_contexts.append(server)
                    port = server.sockets[0].getsockname()[1]
                    source = {'name': f'bench-{coin}', 'url': f'ws://127.0.0.1:{port}',
                              'topic': topic, 'key': f'BENCH:{coin}:KRW', 'subscribe': []}
                    collector_tasks.append(asyncio.create_task(collector.collect(source, TrackingProducer())))
                sampler = asyncio.create_task(sample_backlogs())
                deadline = start + duration + max(45, total / 100)
                while (consumed < total or acknowledged < total) and time.monotonic() < deadline:
                    records = await consumer.getmany(timeout_ms=500, max_records=2000)
                    for batch in records.values():
                        for record in batch:
                            frame = json.loads(record.value)
                            if frame.get('bench_run') != run_id:
                                continue
                            index = frame['bench_index']
                            assert index not in seen, f'duplicate benchmark frame {index}'
                            assert record.key == f'BENCH:{frame["code"][4:]}:KRW'.encode()
                            seen.add(index)
                            consumed += 1
                            partition_counts[str(record.partition)] += 1
                            end_to_end_ms.append((time.monotonic_ns() - frame['bench_sent_ns']) / 1_000_000)
                    for task in collector_tasks:
                        if task.done() and not task.cancelled():
                            task.result()
                assert sent == acknowledged == consumed == total, (sent, acknowledged, consumed, total)
                assert all(count == 1 for count in connections.values()), connections
            finally:
                for task in collector_tasks:
                    task.cancel()
                await asyncio.gather(*collector_tasks, return_exceptions=True)
                done.set()
                if sampler is not None:
                    await sampler
                for server in server_contexts:
                    server.close()
                    await server.wait_closed()

            elapsed = last_ack - first_send
            result = {
                'kind': kind, 'topic': topic, 'partitions': partitions,
                'target_rate_per_second': target_rate, 'planned_duration_seconds': duration,
                'source_count': len(COINS), 'sent': sent, 'acknowledged': acknowledged,
                'consumed': consumed, 'value_bytes_sample': frame_size,
                'websocket_connections': connections,
                'ack_elapsed_seconds': round(elapsed, 3),
                'ack_rate_per_second': round(total / elapsed, 2),
                'ack_ms_p50': percentile(ack_ms, 0.50),
                'ack_ms_p95': percentile(ack_ms, 0.95),
                'end_to_end_ms_p95': percentile(end_to_end_ms, 0.95),
                'max_socket_to_ack_backlog': max_socket_backlog,
                'max_ack_to_consumer_backlog': max_kafka_backlog,
                'partition_counts': partition_counts,
            }
            output = Path('/artifacts') / f'bench_{kind}_{partitions}p_{target_rate}r.json'
            output.write_text(json.dumps(result, indent=2) + '\n')
            print(f'BENCH_OK kind={kind} partitions={partitions} target={target_rate} '
                  f'ack_rate={result["ack_rate_per_second"]} p95_ack_ms={result["ack_ms_p95"]}')
            return result
        finally:
            await producer.stop()
    finally:
        await consumer.stop()


if __name__ == '__main__':
    asyncio.run(run(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])))
