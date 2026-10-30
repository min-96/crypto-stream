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

from offsets import publish_then_commit_transactionally, publish_batch_transactionally


class Transaction:
    def __init__(self, producer):
        self.producer = producer

    async def __aenter__(self):
        self.producer.events.append('begin')

    async def __aexit__(self, kind, _error, _traceback):
        self.producer.events.append('abort' if kind else 'commit')


class Producer:
    def __init__(self):
        self.events = []
        self.offsets = None
        self.group = None

    def transaction(self):
        return Transaction(self)

    async def send_offsets_to_transaction(self, offsets, group):
        self.events.append('offsets')
        self.offsets = offsets
        self.group = group


class TransactionTests(unittest.TestCase):
    def setUp(self):
        self.raw = SimpleNamespace(topic='asiaSocketDataInBTC-ticker', partition=0, offset=7)
        self.producer = Producer()

    def test_output_and_offset_commit_in_one_transaction(self):
        async def publish(_raw):
            self.producer.events.append('produce')
        asyncio.run(publish_then_commit_transactionally(
            self.raw, publish, self.producer, 'preprocessing-ticker'))
        self.assertEqual(self.producer.events, ['begin', 'produce', 'offsets', 'commit'])
        self.assertEqual(self.producer.group, 'preprocessing-ticker')
        self.assertEqual([(tp.topic, tp.partition, offset)
                          for tp, offset in self.producer.offsets.items()],
                         [('asiaSocketDataInBTC-ticker', 0, 8)])

    def test_after_write_failure_aborts_output_and_offset(self):
        async def publish(_raw):
            self.producer.events.append('produce')
        def interrupt(_raw):
            raise RuntimeError('stopped after write')
        with self.assertRaisesRegex(RuntimeError, 'stopped after write'):
            asyncio.run(publish_then_commit_transactionally(
                self.raw, publish, self.producer, 'preprocessing-ticker',
                after_ack=interrupt))
        self.assertEqual(self.producer.events, ['begin', 'produce', 'abort'])
        self.assertIsNone(self.producer.offsets)

    def test_batch_commits_last_contiguous_offset_per_partition_once(self):
        records = [SimpleNamespace(topic='raw', partition=0, offset=7),
                   SimpleNamespace(topic='raw', partition=1, offset=3),
                   SimpleNamespace(topic='raw', partition=0, offset=8)]
        async def publish(raw):
            self.producer.events.append(f'produce-{raw.partition}-{raw.offset}')
        asyncio.run(publish_batch_transactionally(
            records, publish, self.producer, 'preprocessing-ticker'))
        self.assertEqual(self.producer.events,
                         ['begin', 'produce-0-7', 'produce-1-3', 'produce-0-8',
                          'offsets', 'commit'])
        self.assertEqual({(tp.topic, tp.partition): offset
                          for tp, offset in self.producer.offsets.items()},
                         {('raw', 0): 9, ('raw', 1): 4})

    def test_batch_failure_aborts_all_outputs_and_offsets(self):
        records = [SimpleNamespace(topic='raw', partition=0, offset=7),
                   SimpleNamespace(topic='raw', partition=0, offset=8)]
        async def publish(raw):
            self.producer.events.append(f'produce-{raw.offset}')
            if raw.offset == 8:
                raise RuntimeError('second record failed')
        with self.assertRaisesRegex(RuntimeError, 'second record failed'):
            asyncio.run(publish_batch_transactionally(
                records, publish, self.producer, 'preprocessing-ticker'))
        self.assertEqual(self.producer.events, ['begin', 'produce-7', 'produce-8', 'abort'])
        self.assertIsNone(self.producer.offsets)

    def test_batch_after_ack_failure_aborts_previous_writes(self):
        records = [SimpleNamespace(topic='raw', partition=0, offset=7),
                   SimpleNamespace(topic='raw', partition=0, offset=8)]
        async def publish(raw):
            self.producer.events.append(f'produce-{raw.offset}')
        def interrupt(raw):
            if raw.offset == 8:
                raise RuntimeError('stopped after second ACK')
        with self.assertRaisesRegex(RuntimeError, 'stopped after second ACK'):
            asyncio.run(publish_batch_transactionally(
                records, publish, self.producer, 'preprocessing-ticker',
                after_ack=interrupt))
        self.assertEqual(self.producer.events, ['begin', 'produce-7', 'produce-8', 'abort'])
        self.assertIsNone(self.producer.offsets)

    def test_batch_rejects_out_of_order_offsets_before_publishing(self):
        records = [SimpleNamespace(topic='raw', partition=0, offset=7),
                   SimpleNamespace(topic='raw', partition=0, offset=6)]
        async def publish(_raw):
            self.fail('out-of-order batch must be rejected before publishing')
        with self.assertRaisesRegex(ValueError, 'out of order'):
            asyncio.run(publish_batch_transactionally(
                records, publish, self.producer, 'preprocessing-ticker'))
        self.assertEqual(self.producer.events, [])


if __name__ == '__main__':
    unittest.main()
