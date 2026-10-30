"""Static guard for the Processed Connector's Kafka Key archive contract."""
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[3]
CONFIG = ROOT / 'infrastructure/connect/connector-configs/processed-to-minio.json'


class ProcessedArchiveContractTests(unittest.TestCase):
    def test_connector_keeps_binary_keys_for_all_processed_topics(self):
        config = json.loads(CONFIG.read_text())
        self.assertEqual(config['s3.bucket.name'], 'processed')
        self.assertEqual(config['format.class'],
                         'io.confluent.connect.s3.format.bytearray.ByteArrayFormat')
        self.assertEqual(config['value.converter'],
                         'org.apache.kafka.connect.converters.ByteArrayConverter')
        self.assertEqual(config.get('store.kafka.keys'), 'true')
        self.assertEqual(config.get('keys.format.class'),
                         'io.confluent.connect.s3.format.bytearray.ByteArrayFormat')
        self.assertEqual(config.get('key.converter'),
                         'org.apache.kafka.connect.converters.ByteArrayConverter')
        expected = {
            'TimeMetricsProcessedCoin', 'ArbitrageProcessedCoin',
            'OrderbookProcessedRegionCoin', 'OrderbookProcessedAllRegionCoin',
            'MarketArbitrageProcessedCoin', 'MarketArbitrageSignalProcessedCoin',
        }
        self.assertEqual(set(config['topics'].split(',')), expected)


if __name__ == '__main__':
    unittest.main()
