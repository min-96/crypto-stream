"""AVG-08: second-stage average and coin_average UPSERT inside foreachBatch (ADR-024).

Pure Python; runs on the host and in the Spark image.
"""

import random
import re
import sys
import unittest
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from coin_average import batch_writer, coin_averages
from mysql_sink import FIELDS, UPSERT_SQL

END_MS = 1793318460000  # window [2026-10-29T00:00Z, +1 min) → time
MAX = '99999999999999999999.999999999999999999'  # largest Decimal(38,18)
PLAIN = re.compile(r'^-?[0-9]+\.[0-9]{18}$')


def exchange(market, count, total, coin='BTC', time_ms=END_MS, **sums):
    """One AVG-07 output row: every field sums to `total` unless overridden."""
    row = {'time': time_ms, 'market': market, 'coin_symbol': coin, 'message_count': count}
    row.update({f'{field}_sum': Decimal(sums.get(field, total)) for field in FIELDS})
    return row


def trade_price(rows):
    return [(r['name'], r['time'], r['data']['trade_price']) for r in coin_averages(rows)]


class CoinAverageTests(unittest.TestCase):
    def test_single_exchange_is_its_own_average(self):
        self.assertEqual(trade_price([exchange('UPBIT', 4, '402')]),
                         [('BTC', END_MS, '100.500000000000000000')])

    def test_no_exchange_makes_no_row(self):
        self.assertEqual(coin_averages([]), [])

    def test_exchanges_have_equal_weight_regardless_of_message_count(self):
        rows = [exchange('UPBIT', 100, '10000'), exchange('BITHUMB', 1, '110')]
        self.assertEqual(trade_price(rows), [('BTC', END_MS, '105.000000000000000000')])

    def test_repeating_decimal_100_100_101(self):
        # Three messages on one exchange, or three exchanges of one message each.
        self.assertEqual(trade_price([exchange('UPBIT', 3, '301')]),
                         [('BTC', END_MS, '100.333333333333333333')])
        rows = [exchange('UPBIT', 1, '100'), exchange('BITHUMB', 1, '100'), exchange('COINONE', 1, '101')]
        self.assertEqual(trade_price(rows), [('BTC', END_MS, '100.333333333333333333')])
        self.assertEqual(trade_price([exchange('UPBIT', 3, '200')]),
                         [('BTC', END_MS, '66.666666666666666667')])

    def test_rounds_once_to_18_digits_half_to_even(self):
        cases = {
            ('0.000000000000000001', 2): '0.000000000000000000',  # 0.5 → 0
            ('0.000000000000000003', 2): '0.000000000000000002',  # 1.5 → 2
            ('0.000000000000000005', 2): '0.000000000000000002',  # 2.5 → 2
            ('0.000000000000000003', 4): '0.000000000000000001',  # 0.75 → 1
            ('-0.000000000000000003', 2): '-0.000000000000000002',
            ('-0.000000000000000005', 2): '-0.000000000000000002',
            ('-0.000000000000000001', 2): '0.000000000000000000',  # never "-0"
            ('-0.000000000000000001', 3): '0.000000000000000000',
        }
        for (total, count), expected in cases.items():
            with self.subTest(total=total, count=count):
                self.assertEqual(trade_price([exchange('UPBIT', count, total)])[0][2], expected)

    def test_exchange_averages_are_not_rounded_before_the_coin_average(self):
        # Exchange averages 0.6e-18, 0.6e-18, 0: exact mean 0.4e-18 → 0.
        # Rounding each exchange first would give (1 + 1 + 0) / 3 → 1e-18.
        rows = [exchange('UPBIT', 5, '0.000000000000000003'),
                exchange('BITHUMB', 5, '0.000000000000000003'),
                exchange('COINONE', 1, '0')]
        self.assertEqual(trade_price(rows)[0][2], '0.000000000000000000')

    def test_largest_and_smallest_decimal_inputs_keep_every_digit(self):
        # More than the 16 integer digits Spark's avg(decimal(38,18)) would keep.
        self.assertEqual(trade_price([exchange('UPBIT', 1, MAX), exchange('BITHUMB', 1, MAX)])[0][2], MAX)
        self.assertEqual(trade_price([exchange('UPBIT', 1, '-' + MAX), exchange('BITHUMB', 1, '-' + MAX)])[0][2],
                         '-' + MAX)
        self.assertEqual(trade_price([exchange('UPBIT', 1, MAX), exchange('BITHUMB', 1, '-' + MAX)])[0][2],
                         '0.000000000000000000')
        self.assertEqual(trade_price([exchange('UPBIT', 3, MAX)])[0][2],
                         '33333333333333333333.333333333333333333')
        self.assertEqual(trade_price([exchange('UPBIT', 7, MAX)])[0][2],
                         '14285714285714285714.285714285714285714')
        self.assertEqual(trade_price([exchange('UPBIT', 2, '12345678901234567890.123456789012345678')])[0][2],
                         '6172839450617283945.061728394506172839')

    def test_output_is_plain_decimal_without_exponent(self):
        rows = [exchange('UPBIT', 1, '1', trade_price=Decimal('1E-18'), max_price=Decimal('1E+19'),
                         min_price=Decimal('0E-18'), opening_price=Decimal('-0'))]
        data = coin_averages(rows)[0]['data']
        self.assertEqual(data['trade_price'], '0.000000000000000001')
        self.assertEqual(data['max_price'], '10000000000000000000.000000000000000000')
        self.assertEqual(data['min_price'], '0.000000000000000000')
        self.assertEqual(data['opening_price'], '0.000000000000000000')
        for field, value in data.items():
            with self.subTest(field=field):
                self.assertRegex(value, PLAIN)

    def test_each_field_is_averaged_independently(self):
        sums_a = {field: str(i + 1) for i, field in enumerate(FIELDS)}
        sums_b = {field: str(-(i + 1) * 3) for i, field in enumerate(FIELDS)}
        data = coin_averages([exchange('UPBIT', 1, '0', **sums_a), exchange('BITHUMB', 1, '0', **sums_b)])[0]['data']
        self.assertEqual(list(data), list(FIELDS))
        for i, field in enumerate(FIELDS):
            with self.subTest(field=field):
                self.assertEqual(data[field], f'{-(i + 1)}.000000000000000000')

    def test_coins_and_windows_are_not_mixed(self):
        rows = [exchange('UPBIT', 1, '1'), exchange('BITHUMB', 1, '3'),
                exchange('UPBIT', 1, '10', coin='ETH'),
                exchange('UPBIT', 1, '100', time_ms=END_MS + 60000)]
        self.assertEqual(trade_price(rows), [
            ('BTC', END_MS, '2.000000000000000000'),
            ('BTC', END_MS + 60000, '100.000000000000000000'),
            ('ETH', END_MS, '10.000000000000000000'),
        ])

    def test_same_batch_gives_same_keys_and_values_in_any_row_order(self):
        rows = [exchange(f'EX{i}', i + 1, f'{i}.00000000000000000{i}', coin=coin)
                for i in range(7) for coin in ('BTC', 'ETH', 'XRP')]
        expected = coin_averages(rows)
        for seed in range(5):
            shuffled = list(rows)
            random.Random(seed).shuffle(shuffled)
            with self.subTest(seed=seed):
                self.assertEqual(coin_averages(shuffled), expected)

    def test_contract_violations_fail_explicitly(self):
        bad_rows = {
            'zero count': exchange('UPBIT', 0, '1'),
            'negative count': exchange('UPBIT', -1, '1'),
            'bool count': exchange('UPBIT', True, '1'),
            'float count': exchange('UPBIT', 1.0, '1'),
            'null sum': dict(exchange('UPBIT', 1, '1'), trade_price_sum=None),
            'float sum': dict(exchange('UPBIT', 1, '1'), trade_price_sum=1.0),
            'NaN sum': dict(exchange('UPBIT', 1, '1'), trade_price_sum=Decimal('NaN')),
            'empty coin': exchange('UPBIT', 1, '1', coin=''),
            'second-scale time': exchange('UPBIT', 1, '1', time_ms=END_MS // 1000),
            'time inside a minute': exchange('UPBIT', 1, '1', time_ms=END_MS + 1000),
        }
        for reason, row in bad_rows.items():
            with self.subTest(reason=reason), self.assertRaises(ValueError):
                coin_averages([row])
        with self.assertRaisesRegex(ValueError, 'UPBIT'):
            coin_averages([exchange('UPBIT', 1, '1'), exchange('UPBIT', 2, '4')])


class FakeStatement:
    def __init__(self, connection, sql):
        self.connection, self.sql, self.params, self.batch = connection, sql, {}, []

    def setString(self, index, value):
        assert isinstance(value, str)
        self.params[index] = value

    def setLong(self, index, value):
        assert type(value) is int
        self.params[index] = value

    def addBatch(self):
        self.batch.append(tuple(self.params[i] for i in sorted(self.params)))
        self.params = {}

    def executeBatch(self):
        if self.connection.fail:
            raise RuntimeError('MySQL went away')
        self.connection.executed.extend((self.sql, params) for params in self.batch)
        return [1] * len(self.batch)

    def close(self):
        self.connection.events.append('statement.close')


class FakeConnection:
    """The java.sql.Connection subset batch_writer uses, recording what it does."""

    def __init__(self, fail=False):
        self.fail, self.executed, self.events, self.auto_commit = fail, [], [], True

    def setAutoCommit(self, value):
        self.auto_commit = value

    def prepareStatement(self, sql):
        return FakeStatement(self, sql)

    def commit(self):
        assert not self.auto_commit
        self.events.append('commit')

    def rollback(self):
        self.events.append('rollback')

    def close(self):
        self.events.append('close')


class FakeBatch:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return list(self.rows)


class BatchWriterTests(unittest.TestCase):
    def setUp(self):
        self.connections = []

    def connect(self, fail=False):
        def factory():
            self.connections.append(FakeConnection(fail))
            return self.connections[-1]
        return factory

    def test_upserts_every_coin_window_in_one_transaction(self):
        rows = [exchange('UPBIT', 3, '301'), exchange('UPBIT', 1, '7', coin='ETH')]
        batch_writer(self.connect())(FakeBatch(rows), 0)
        connection, = self.connections
        self.assertEqual(connection.executed, [
            (UPSERT_SQL, ('BTC', END_MS, *['100.333333333333333333'] * len(FIELDS))),
            (UPSERT_SQL, ('ETH', END_MS, *['7.000000000000000000'] * len(FIELDS))),
        ])
        self.assertEqual(connection.events, ['statement.close', 'commit', 'close'])

    def test_replaying_a_batch_writes_the_same_keys_and_values(self):
        rows = [exchange('UPBIT', 3, '200'), exchange('BITHUMB', 7, MAX), exchange('UPBIT', 2, '-5', coin='ETH')]
        write = batch_writer(self.connect())
        write(FakeBatch(rows), 4)
        write(FakeBatch(list(reversed(rows))), 4)
        first, second = self.connections
        self.assertEqual(first.executed, second.executed)

    def test_batch_without_closed_windows_does_not_connect(self):
        batch_writer(self.connect())(FakeBatch([]), 1)
        self.assertEqual(self.connections, [])

    def test_invalid_row_fails_before_connecting(self):
        with self.assertRaises(ValueError):
            batch_writer(self.connect())(FakeBatch([exchange('UPBIT', 0, '1')]), 2)
        self.assertEqual(self.connections, [])

    def test_write_failure_rolls_back_and_fails_the_batch(self):
        with self.assertRaisesRegex(RuntimeError, 'MySQL went away'):
            batch_writer(self.connect(fail=True))(FakeBatch([exchange('UPBIT', 1, '1')]), 3)
        connection, = self.connections
        self.assertEqual(connection.executed, [])
        self.assertEqual(connection.events, ['statement.close', 'rollback', 'close'])


if __name__ == '__main__':
    unittest.main()
