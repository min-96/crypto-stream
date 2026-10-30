import asyncio
import importlib.util
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


# The contract tests use a fake producer and do not require a Kafka installation.
with patch.dict(sys.modules, {
    'aiokafka': types.SimpleNamespace(AIOKafkaProducer=object),
    'websockets': types.SimpleNamespace(connect=None),
}):
    spec = importlib.util.spec_from_file_location('collector', Path(__file__).parents[1] / 'collector.py')
    collector = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(collector)


class CollectorContractTests(unittest.TestCase):
    def load(self, source):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'sources.json'
            path.write_text(json.dumps([source]))
            return collector.load_sources(str(path))

    def source(self, **changes):
        source = {'name': 'binance-btc', 'url': 'ws://localhost/ws',
                  'topic': 'asiaSocketDataInBTC-ticker', 'key': 'BINANCE:BTC:USDT'}
        source.update(changes)
        return source

    def test_valid_single_pair_key(self):
        self.assertEqual(self.load(self.source())[0]['key'], 'BINANCE:BTC:USDT')

    def test_invalid_key_and_topic_are_rejected(self):
        for key in ('BINANCE:BTC', 'binance:BTC:USDT', 'BINANCE:BTC:USDT:EXTRA',
                    'BINANCE:BTC:', 'BINANCE:BTC:USD T'):
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.load(self.source(key=key))
        with self.assertRaises(ValueError):
            self.load(self.source(topic='unapproved-raw-topic'))

    def test_frame_bytes_and_key_are_sent_unchanged_after_ack(self):
        producer = types.SimpleNamespace(send_and_wait=AsyncMock(return_value=object()))
        frame = b'{ "symbol":"BTCUSDT", "price":1.0 }\n'
        asyncio.run(collector.forward_frame(self.source(), producer, frame))
        producer.send_and_wait.assert_awaited_once_with(
            'asiaSocketDataInBTC-ticker', key=b'BINANCE:BTC:USDT', value=frame)
        asyncio.run(collector.forward_frame(self.source(), producer, '{ "price": 1.0 }'))
        self.assertEqual(producer.send_and_wait.await_args.kwargs['value'], b'{ "price": 1.0 }')

    def test_producer_failure_is_propagated(self):
        producer = types.SimpleNamespace(send_and_wait=AsyncMock(side_effect=RuntimeError('no ACK')))
        with self.assertRaisesRegex(RuntimeError, 'no ACK'):
            asyncio.run(collector.forward_frame(self.source(), producer, b'frame'))

    def test_collect_does_not_reconnect_after_kafka_failure(self):
        class Socket:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_):
                pass

            def __aiter__(self):
                async def frames():
                    yield b'frame'
                return frames()

        producer = types.SimpleNamespace(send_and_wait=AsyncMock(side_effect=RuntimeError('no ACK')))
        with patch.object(collector.websockets, 'connect', return_value=Socket()) as connect:
            with self.assertRaises(collector.PublishFailed), self.assertLogs(collector.LOG, level='ERROR'):
                asyncio.run(collector.collect(self.source(), producer))
        connect.assert_called_once()

    def test_collect_reconnects_after_websocket_closes(self):
        class Socket:
            def __init__(self, frame):
                self.frame = frame

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_):
                pass

            def __aiter__(self):
                async def frames():
                    yield self.frame
                return frames()

        producer = types.SimpleNamespace(send_and_wait=AsyncMock(return_value=object()))
        attempts = [Socket(b'first'), Socket(b'after-reconnect')]

        def connect(*_args, **_kwargs):
            if attempts:
                return attempts.pop(0)
            raise asyncio.CancelledError()

        with patch.object(collector.websockets, 'connect', side_effect=connect) as opened, \
                patch.object(collector.asyncio, 'sleep', new_callable=AsyncMock):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(collector.collect(self.source(), producer))
        self.assertEqual(opened.call_count, 3)
        self.assertEqual([call.kwargs['value'] for call in producer.send_and_wait.await_args_list],
                         [b'first', b'after-reconnect'])


if __name__ == '__main__':
    unittest.main()
