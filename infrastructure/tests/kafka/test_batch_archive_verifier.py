import importlib.util
import base64
import unittest
from pathlib import Path
from types import SimpleNamespace


module_path = Path(__file__).parent / 'harness' / 'verify_batch_archive.py'
spec = importlib.util.spec_from_file_location('verify_batch_archive', module_path)
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


class BatchArchiveVerifierTest(unittest.TestCase):
    def setUp(self):
        self.records = [
            SimpleNamespace(topic='RegionKorea_TickerPreprocessing', partition=0,
                            offset=offset, key=key, value=value)
            for offset, key, value in (
                (1, b'BINANCE:BTC:USDT', b'{"price":1}'),
                (3, b'BINANCE:ETH:USDT', b'{"price":2}'),
            )
        ]
        self.name = ('topics/RegionKorea_TickerPreprocessing/partition=0/'
                     'RegionKorea_TickerPreprocessing+0+0000000001.bin')
        self.value_objects = {self.name: b'{"price":1}\n{"price":2}'}
        self.key_objects = {self.name[:-4] + '.keys.bin':
                            b'BINANCE:BTC:USDT\nBINANCE:ETH:USDT'}

    def test_batch_bytes_match_but_later_offset_is_not_in_s3(self):
        result = verifier.compare_objects(self.records, self.value_objects,
                                          self.key_objects)
        self.assertTrue(result['all_kafka_records_matched'])
        self.assertEqual(result['matched_records'], 2)
        self.assertEqual(result['multi_record_objects'], 1)
        self.assertFalse(result['offsets_recoverable_from_s3_alone'])

    def test_corrupt_key_is_rejected(self):
        self.key_objects[self.name[:-4] + '.keys.bin'] = b'BAD\nBINANCE:ETH:USDT'
        with self.assertRaisesRegex(AssertionError, 'Kafka bytes mismatch'):
            verifier.compare_objects(self.records, self.value_objects,
                                     self.key_objects)

    def test_single_record_file_recovers_its_offset(self):
        self.records.pop()
        self.value_objects[self.name] = b'{"price":1}'
        self.key_objects[self.name[:-4] + '.keys.bin'] = b'BINANCE:BTC:USDT'
        result = verifier.compare_objects(self.records, self.value_objects,
                                          self.key_objects)
        self.assertTrue(result['offsets_recoverable_from_s3_alone'])

    def test_envelope_recovers_every_offset_from_s3(self):
        def envelope(offset, value):
            encoded = base64.b64encode(value).decode()
            return ('{"archive_format":1,"offset":%d,"value_b64":"%s"}'
                    % (offset, encoded)).encode()
        self.value_objects[self.name] = b'\n'.join(
            envelope(record.offset, record.value) for record in self.records)
        result = verifier.compare_objects(self.records, self.value_objects,
                                          self.key_objects, offset_envelope=True)
        self.assertTrue(result['all_kafka_records_matched'])
        self.assertTrue(result['offsets_recoverable_from_s3_alone'])


if __name__ == '__main__':
    unittest.main()
