import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parents[2] / 'spark'))
from processed_contract import TOPIC_CONTRACTS, identity_for, key_for, validate_record


class ProcessedSinkContractTests(unittest.TestCase):
    def setUp(self):
        self.row = {
            'region': 'ASIA', 'coin_symbol': 'BTC', 'symbol': 'BTC',
            'market': 'BINANCE', 'start': '2026-10-29T00:00:00.000Z',
            'end': '2026-10-29T00:01:00.000Z',
        }

    def test_six_topic_keys(self):
        expected = {
            'TimeMetricsProcessedCoin': b'ASIA:BTC',
            'ArbitrageProcessedCoin': b'BTC',
            'OrderbookProcessedRegionCoin': b'ASIA:BTC',
            'OrderbookProcessedAllRegionCoin': b'BTC',
            'MarketArbitrageProcessedCoin': b'BTC',
            'MarketArbitrageSignalProcessedCoin': b'BINANCE:BTC',
        }
        self.assertEqual(set(TOPIC_CONTRACTS), set(expected))
        for topic, key in expected.items():
            with self.subTest(topic=topic):
                self.assertEqual(key_for(topic, self.row), key)

    def test_required_dimensions_and_window(self):
        for topic, contract in TOPIC_CONTRACTS.items():
            for field in (*contract['key_fields'], 'start', 'end'):
                with self.subTest(topic=topic, field=field):
                    row = {**self.row, field: None}
                    with self.assertRaises(ValueError):
                        validate_record(topic, row)

    def test_replay_has_same_identity_and_different_window_does_not(self):
        topic = 'TimeMetricsProcessedCoin'
        key = key_for(topic, self.row)
        first = identity_for(topic, key, self.row)
        self.assertEqual(first, identity_for(topic, key, dict(self.row)))
        equivalent = {**self.row, 'start': '2026-10-29T00:00:00Z'}
        self.assertEqual(first, identity_for(topic, key, equivalent))
        changed = {**self.row, 'end': '2026-10-29T00:02:00.000Z'}
        self.assertNotEqual(first, identity_for(topic, key, changed))

    def test_bad_key_dimension_rejected(self):
        with self.assertRaises(ValueError):
            key_for('MarketArbitrageSignalProcessedCoin', {**self.row, 'market': 'binance'})

    def test_invalid_window_and_mismatched_key_rejected(self):
        topic = 'TimeMetricsProcessedCoin'
        with self.assertRaises(ValueError):
            key_for(topic, {**self.row, 'end': '2026-10-29T00:00:00.000Z'})
        with self.assertRaises(ValueError):
            key_for(topic, {**self.row, 'start': '2026-10-29T00:00:00+09:00'})
        with self.assertRaises(ValueError):
            identity_for(topic, b'KOREA:BTC', self.row)


if __name__ == '__main__':
    unittest.main()
