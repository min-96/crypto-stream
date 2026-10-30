"""AVG-05: coin_average DDL and (name, time) UPSERT against a throwaway MySQL."""

import os
import re
import shutil
import subprocess
import sys
import time
import unittest
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from mysql_sink import DDL_PATH, FIELDS, UPSERT_SQL, upsert_params

MYSQL_IMAGE = 'mysql:8.4.3'
ROOT_PASSWORD = 'avg05-test'
TIME_MS = 1793318400000  # a 1-minute window end (ADR-023)


def row(name='BTC', time_ms=TIME_MS, value='100'):
    return {'name': name, 'time': time_ms, 'data': {field: value for field in FIELDS}}


class DdlContractTests(unittest.TestCase):
    def setUp(self):
        self.ddl = DDL_PATH.read_text()

    def test_table_is_separate_from_the_six_analysis_tables(self):
        self.assertEqual(re.findall(r'CREATE TABLE IF NOT EXISTS `(\w+)`', self.ddl), ['coin_average'])

    def test_time_is_epoch_ms_bigint_not_datetime(self):
        self.assertRegex(self.ddl, r'`time` BIGINT NOT NULL')
        self.assertNotRegex(self.ddl.upper(), r'DATETIME|TIMESTAMP')
        self.assertIn('`time` % 60000 = 0', self.ddl)

    def test_name_time_unique_and_eight_fields(self):
        self.assertRegex(self.ddl, r'PRIMARY KEY \(`name`, `time`\)')
        for field in FIELDS:
            self.assertRegex(self.ddl, rf'`{field}` VARCHAR\(\d+\) NOT NULL')

    def test_upsert_updates_every_field_on_duplicate_key(self):
        self.assertEqual(UPSERT_SQL.count('?'), 2 + len(FIELDS))
        for field in FIELDS:
            self.assertIn(f'`{field}` = new.`{field}`', UPSERT_SQL)


class UpsertParamsTests(unittest.TestCase):
    def test_params_follow_column_order(self):
        params = upsert_params({'name': 'BTC', 'time': TIME_MS,
                                'data': {field: str(i) for i, field in enumerate(FIELDS)}})
        self.assertEqual(params, ('BTC', TIME_MS, *(str(i) for i in range(len(FIELDS)))))

    def test_rejects_time_that_is_not_a_window_end_in_epoch_ms(self):
        for bad in (1793318400, TIME_MS + 1, TIME_MS + 1000, TIME_MS + 59000,
                    float(TIME_MS), str(TIME_MS), True):
            with self.subTest(time=bad), self.assertRaises(ValueError):
                upsert_params(row(time_ms=bad))

    def test_rejects_missing_or_non_string_field(self):
        for bad in (None, 100, 100.0, ''):
            with self.subTest(value=bad), self.assertRaises(ValueError):
                upsert_params(row(value=bad))
        incomplete = row()
        del incomplete['data']['trade_price']
        with self.assertRaises(ValueError):
            upsert_params(incomplete)


def _sql_literal(value):
    if isinstance(value, int):
        return str(value)
    return "'" + value.replace('\\', '\\\\').replace("'", "''") + "'"


def upsert_script(rows):
    """Run UPSERT_SQL as a server-side prepared statement, the same text JDBC will send."""
    count = 2 + len(FIELDS)
    names = ', '.join(f'@p{i}' for i in range(count))
    lines = [f"PREPARE upsert FROM {_sql_literal(UPSERT_SQL)};"]
    for item in rows:
        params = upsert_params(item)
        lines.append('SET ' + ', '.join(f'@p{i} = {_sql_literal(v)}' for i, v in enumerate(params)) + ';')
        lines.append(f'EXECUTE upsert USING {names};')
    lines.append('DEALLOCATE PREPARE upsert;')
    return '\n'.join(lines)


@unittest.skipUnless(shutil.which('docker') and not os.environ.get('COIN_MARKET_SKIP_DOCKER'),
                     'docker not available')
class MysqlUpsertTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.container = f'coin-market-avg05-{uuid.uuid4().hex[:8]}'
        subprocess.run(
            ['docker', 'run', '-d', '--rm', '--name', cls.container, '--tmpfs', '/var/lib/mysql',
             '-e', f'MYSQL_ROOT_PASSWORD={ROOT_PASSWORD}', '-e', 'MYSQL_DATABASE=coin_market',
             '-v', f'{DDL_PATH}:/docker-entrypoint-initdb.d/{DDL_PATH.name}:ro', MYSQL_IMAGE],
            check=True, capture_output=True)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            logs = subprocess.run(['docker', 'logs', cls.container], capture_output=True, text=True)
            # The init server listens on port 0; the final server announces port 3306.
            if 'ready for connections' in logs.stderr and 'port: 3306' in logs.stderr:
                return
            time.sleep(1)
        cls.tearDownClass()
        raise RuntimeError('throwaway MySQL did not become ready')

    @classmethod
    def tearDownClass(cls):
        subprocess.run(['docker', 'rm', '-f', cls.container], capture_output=True)

    def setUp(self):
        self.mysql('TRUNCATE TABLE coin_average;')

    def mysql(self, script, *, check=True):
        result = subprocess.run(
            ['docker', 'exec', '-i', self.container, 'mysql', '-uroot', f'-p{ROOT_PASSWORD}',
             '--batch', '--skip-column-names', 'coin_market'],
            input=script, capture_output=True, text=True)
        if check and result.returncode != 0:
            self.fail(result.stderr)
        return result

    def rows(self):
        out = self.mysql('SELECT name, time, ' + ', '.join(f'`{f}`' for f in FIELDS)
                         + ' FROM coin_average ORDER BY name, time;').stdout
        return [line.split('\t') for line in out.splitlines()]

    def test_same_key_twice_keeps_one_row_with_latest_values(self):
        self.mysql(upsert_script([row(value='100')]))
        self.mysql(upsert_script([row(value='105.5')]))
        self.assertEqual(self.rows(), [['BTC', str(TIME_MS), *['105.5'] * len(FIELDS)]])

    def test_other_time_or_coin_is_a_new_row(self):
        self.mysql(upsert_script([row(), row(time_ms=TIME_MS + 60000), row(name='ETH')]))
        self.assertEqual([r[:2] for r in self.rows()],
                         [['BTC', str(TIME_MS)], ['BTC', str(TIME_MS + 60000)], ['ETH', str(TIME_MS)]])

    def test_decimal_string_is_stored_without_precision_loss(self):
        value = '0.1234567890123456789012345678'
        self.mysql(upsert_script([row(value=value)]))
        self.assertEqual(self.rows()[0][2:], [value] * len(FIELDS))

    def test_database_rejects_time_off_the_minute_grid_and_null_fields(self):
        columns = ', '.join(f'`{f}`' for f in FIELDS)
        values = ', '.join(["'1'"] * len(FIELDS))
        nulls = ', '.join(['NULL'] * len(FIELDS))
        for script in (f"INSERT INTO coin_average (name, time, {columns}) VALUES ('BTC', 1793318400, {values});",
                       f"INSERT INTO coin_average (name, time, {columns}) VALUES ('BTC', {TIME_MS + 1}, {values});",
                       f"INSERT INTO coin_average (name, time, {columns}) VALUES ('BTC', {TIME_MS + 1000}, {values});",
                       f"INSERT INTO coin_average (name, time, {columns}) VALUES ('BTC', {TIME_MS}, {nulls});"):
            with self.subTest(script=script[-60:]):
                self.assertNotEqual(self.mysql(script, check=False).returncode, 0)
        self.assertEqual(self.rows(), [])

    def test_one_second_of_35_coins_upserts_in_under_one_second(self):
        batch = [row(name=f'C{i:02d}', value=str(i)) for i in range(35)]
        script = upsert_script(batch)
        # Second pass is the steady-state case: every key already exists.
        for attempt in ('insert', 'update'):
            timed = ('SET @t0 = NOW(6);\nSTART TRANSACTION;\n' + script
                     + '\nCOMMIT;\nSELECT TIMESTAMPDIFF(MICROSECOND, @t0, NOW(6));')
            started = time.monotonic()
            server_us = int(self.mysql(timed).stdout.split()[-1])
            wall = time.monotonic() - started
            with self.subTest(attempt=attempt):
                self.assertLess(server_us, 1_000_000)
                print(f'\nAVG-05 {attempt} 35 rows: server {server_us / 1000:.1f} ms, '
                      f'wall incl. docker exec {wall * 1000:.1f} ms', file=sys.stderr)
        self.assertEqual(len(self.rows()), 35)


if __name__ == '__main__':
    unittest.main()
