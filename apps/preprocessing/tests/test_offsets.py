import asyncio
import sys
import types
import unittest
from collections import namedtuple
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
try:
    import aiokafka
except ModuleNotFoundError:
    aiokafka = types.ModuleType('aiokafka')
    aiokafka.TopicPartition = namedtuple('TopicPartition', 'topic partition')
    sys.modules['aiokafka'] = aiokafka

from offsets import publish_then_commit


class Consumer:
    def __init__(self):
        self.commits = []

    async def commit(self, offsets):
        self.commits.append(offsets)


class OffsetTests(unittest.TestCase):
    def setUp(self):
        self.raw = SimpleNamespace(topic='koreaSocketDataInBTC-ticker', partition=0, offset=7)
        self.consumer = Consumer()
        self.events = []

    def test_ack_precedes_explicit_commit(self):
        async def publish(raw):
            self.events.append('ack')
        original_commit = self.consumer.commit
        async def commit(offsets):
            self.events.append('commit')
            await original_commit(offsets)
        self.consumer.commit = commit
        asyncio.run(publish_then_commit(self.raw, publish, self.consumer))
        self.assertEqual(self.events, ['ack', 'commit'])
        offsets = self.consumer.commits[0]
        self.assertEqual([(tp.topic, tp.partition, value) for tp, value in offsets.items()],
                         [(self.raw.topic, 0, 8)])

    def test_publish_failure_leaves_offset_uncommitted(self):
        async def publish(raw):
            raise RuntimeError('no ACK')
        with self.assertRaisesRegex(RuntimeError, 'no ACK'):
            asyncio.run(publish_then_commit(self.raw, publish, self.consumer))
        self.assertEqual(self.consumer.commits, [])

    def test_orderbook_uses_same_partition_boundary(self):
        self.raw.topic = 'koreaSocketDataInBTC-orderbook'
        async def publish(raw):
            self.events.append('ack')
        asyncio.run(publish_then_commit(self.raw, publish, self.consumer))
        offsets = self.consumer.commits[0]
        self.assertEqual([(tp.topic, tp.partition, value) for tp, value in offsets.items()],
                         [('koreaSocketDataInBTC-orderbook', 0, 8)])

    def test_interruption_after_ack_leaves_offset_uncommitted(self):
        async def publish(raw):
            self.events.append('ack')
        def after_ack(raw):
            raise RuntimeError('stopped before commit')
        with self.assertRaisesRegex(RuntimeError, 'stopped before commit'):
            asyncio.run(publish_then_commit(self.raw, publish, self.consumer, after_ack=after_ack))
        self.assertEqual(self.events, ['ack'])
        self.assertEqual(self.consumer.commits, [])


if __name__ == '__main__':
    unittest.main()
