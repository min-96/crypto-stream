"""Config-driven WebSocket to Kafka raw collector.

Exchange-specific subscriptions and the final coin list remain external config.
Raw payload bytes are forwarded without normalization.
"""
import asyncio
import json
import logging
import os
import re
from pathlib import Path

import websockets
from aiokafka import AIOKafkaProducer

LOG = logging.getLogger(__name__)
RAW_TOPICS = {
    'koreaSocketDataInBTC-ticker', 'koreaSocketDataInBTC-orderbook',
    'neSocketDataInBTC-ticker', 'neSocketDataInBTC-orderbook',
    'asiaSocketDataInBTC-ticker', 'asiaSocketDataInBTC-orderbook',
}
RAW_KEY = re.compile(r'[A-Z][A-Z0-9_-]*:[A-Z0-9][A-Z0-9._-]*:[A-Z0-9][A-Z0-9._-]*\Z')


class PublishFailed(RuntimeError):
    """A Kafka write failed; reconnecting the socket must not hide it."""


def load_sources(path: str) -> list[dict]:
    sources = json.loads(Path(path).read_text())
    if not isinstance(sources, list) or not sources:
        raise ValueError('sources must be a nonempty JSON array')
    for source in sources:
        if not isinstance(source, dict):
            raise ValueError('each source must be an object')
        if not all(isinstance(source.get(field), str) and source[field] for field in ('name', 'url', 'topic', 'key')):
            raise ValueError('source needs nonempty name, url, topic and key strings')
        if not source['url'].startswith(('wss://', 'ws://')):
            raise ValueError(f"invalid WebSocket URL for {source['name']}")
        if source['topic'] not in RAW_TOPICS:
            raise ValueError(f"unrecognized raw topic: {source['topic']}")
        if not RAW_KEY.fullmatch(source['key']):
            raise ValueError(f"invalid single-pair raw key for {source['name']}: {source['key']}")
        messages = source.get('subscribe', [])
        if not isinstance(messages, list):
            raise ValueError('subscribe must be an array')
    return sources


async def forward_frame(source: dict, producer: AIOKafkaProducer, payload: str | bytes) -> None:
    """Send one single-pair WebSocket frame and wait for its Kafka ACK."""
    value = payload.encode('utf-8') if isinstance(payload, str) else payload
    await producer.send_and_wait(source['topic'], key=source['key'].encode('utf-8'), value=value)


async def collect(source: dict, producer: AIOKafkaProducer) -> None:
    retry = 1
    while True:
        try:
            async with websockets.connect(source['url'], ping_interval=20, ping_timeout=20, max_size=8 * 1024 * 1024) as socket:
                LOG.info('connected source=%s', source['name'])
                for message in source.get('subscribe', []):
                    await socket.send(message if isinstance(message, str) else json.dumps(message, separators=(',', ':')))
                retry = 1
                async for payload in socket:
                    try:
                        await forward_frame(source, producer, payload)
                    except Exception as error:
                        raise PublishFailed(f"Kafka ACK failed for source={source['name']}") from error
        except asyncio.CancelledError:
            raise
        except PublishFailed:
            LOG.exception('source=%s Kafka publish failed', source['name'])
            raise
        except Exception:
            LOG.exception('source=%s disconnected; retrying in %ds', source['name'], retry)
            await asyncio.sleep(retry)
            retry = min(retry * 2, 30)


async def main() -> None:
    sources = load_sources(os.environ.get('COLLECTOR_SOURCES', '/config/sources.json'))
    producer = AIOKafkaProducer(
        bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka-1:9092,kafka-2:9092,kafka-3:9092'),
        acks='all',
        enable_idempotence=True,
    )
    await producer.start()
    try:
        async with asyncio.TaskGroup() as group:
            for source in sources:
                group.create_task(collect(source, producer))
    finally:
        await producer.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    asyncio.run(main())
