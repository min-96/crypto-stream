import json
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE.parents[1] / 'preprocessing'))

from mock_upbit import make_ticker, validate_subscription
from pipeline import validate
from upbit import convert_ticker
from bithumb import convert_ticker as convert_bithumb_ticker


class MockUpbitTests(unittest.TestCase):
    def setUp(self):
        self.template = json.loads((HERE.parents[1] / 'preprocessing' / 'tests' /
                                    'fixtures' / 'upbit_ticker.json').read_text())

    def test_repeated_frames_match_real_converter_contract(self):
        previous_volume = 0
        for sequence in range(15):
            frame = make_ticker(self.template, sequence, 1793320000000 + sequence * 1000)
            result = convert_ticker(json.dumps(frame).encode(), b'UPBIT:BTC:KRW')
            validate(result, 'korea', 'ticker', b'UPBIT:BTC:KRW')
            self.assertEqual(result['timestamp'], 1793320000 + sequence)
            self.assertEqual(frame['stream_type'], 'SNAPSHOT' if sequence == 0 else 'REALTIME')
            self.assertGreater(frame['acc_trade_volume'], previous_volume)
            self.assertEqual(frame['trade_price'] - frame['prev_closing_price'],
                             frame['signed_change_price'])
            previous_volume = frame['acc_trade_volume']

    def test_accepts_collector_subscription_and_rejects_other_pairs(self):
        source = json.loads((HERE.parent / 'sources.mock-upbit.json').read_text())[0]
        validate_subscription(json.dumps(source['subscribe'][0]))
        with self.assertRaises(ValueError):
            validate_subscription('[{"ticket":"x"},{"type":"ticker","codes":["KRW-ETH"]}]')

    def test_bithumb_fixture_matches_converter(self):
        template = json.loads((HERE.parents[1] / 'preprocessing' / 'tests' /
                               'fixtures' / 'bithumb_ticker.json').read_text())
        frame = make_ticker(template, 2, 1793320002000)
        result = convert_bithumb_ticker(json.dumps(frame).encode(), b'BITHUMB:BTC:KRW')
        validate(result, 'korea', 'ticker', b'BITHUMB:BTC:KRW')
        self.assertEqual(result['market'], 'BITHUMB')


if __name__ == '__main__':
    unittest.main()
