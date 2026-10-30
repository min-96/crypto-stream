import asyncio
import sys
import types
import unittest
from collections import namedtuple
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
try:
    import aiokafka
except ModuleNotFoundError:
    aiokafka = types.ModuleType('aiokafka')
    sys.modules['aiokafka'] = aiokafka
if not hasattr(aiokafka, 'TopicPartition'):
    aiokafka.TopicPartition = namedtuple('TopicPartition', 'topic partition')
if not hasattr(aiokafka, 'AIOKafkaConsumer'):
    aiokafka.AIOKafkaConsumer = object
if not hasattr(aiokafka, 'AIOKafkaProducer'):
    aiokafka.AIOKafkaProducer = object

from consumer import RequiredRawOffsetExpired, ensure_committed_offsets_available, fetch_batch


class FakeConsumer:
    def __init__(self, committed, beginning):
        self._committed = committed
        self._beginning = beginning

    def partitions_for_topic(self, topic):
        return {0}

    async def committed(self, partition):
        return self._committed

    async def beginning_offsets(self, partitions):
        return {partitions[0]: self._beginning}


class RetentionTests(unittest.TestCase):
    def test_expired_committed_offset_is_explicit_failure(self):
        consumer = FakeConsumer(committed=1, beginning=2)
        with self.assertRaisesRegex(RequiredRawOffsetExpired, 'required Raw offset 1'):
            asyncio.run(ensure_committed_offsets_available(consumer, ('asiaSocketDataInBTC-ticker',)))

    def test_available_or_new_group_can_start(self):
        for committed, beginning in ((2, 2), (None, 2)):
            asyncio.run(ensure_committed_offsets_available(
                FakeConsumer(committed, beginning), ('asiaSocketDataInBTC-ticker',)))

    def test_fetch_batch_caps_poll_and_keeps_partition_order(self):
        class BatchConsumer:
            def __init__(self):
                self.calls = []

            async def getmany(self, *, timeout_ms, max_records):
                self.calls.append((timeout_ms, max_records))
                return {('raw', 0): [1, 2]} if len(self.calls) == 1 else {('raw', 0): [3]}

        consumer = BatchConsumer()
        self.assertEqual(asyncio.run(fetch_batch(consumer, 3, 250)), [1, 2, 3])
        self.assertEqual([limit for _, limit in consumer.calls], [3, 1])


if __name__ == '__main__':
    unittest.main()
