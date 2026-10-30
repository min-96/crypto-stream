"""AVG-08: the foreachBatch writer over real JDBC (Spark image) into a throwaway MySQL.

Host test: starts MySQL 8.4.3 and the Spark image on a private docker network.
"""

import os
import shutil
import subprocess
import time
import unittest
import uuid
from pathlib import Path

from test_mysql_sink import DDL_PATH, FIELDS, MYSQL_IMAGE, ROOT_PASSWORD

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
SPARK_IMAGE = 'coin-market-spark:3.5.3-kafka-mysql'
END_MS = 1793318460000


def _spark_image_available():
    if not shutil.which('docker') or os.environ.get('COIN_MARKET_SKIP_DOCKER'):
        return False
    return subprocess.run(['docker', 'image', 'inspect', SPARK_IMAGE], capture_output=True).returncode == 0


@unittest.skipUnless(_spark_image_available(), 'docker or the Spark image is not available')
class CoinAverageJdbcTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        suffix = uuid.uuid4().hex[:8]
        cls.network, cls.container = f'coin-market-avg08-{suffix}', f'coin-market-avg08-mysql-{suffix}'
        subprocess.run(['docker', 'network', 'create', cls.network], check=True, capture_output=True)
        subprocess.run(
            ['docker', 'run', '-d', '--rm', '--name', cls.container, '--network', cls.network,
             '--tmpfs', '/var/lib/mysql', '-e', f'MYSQL_ROOT_PASSWORD={ROOT_PASSWORD}',
             '-e', 'MYSQL_DATABASE=coin_market',
             '-v', f'{DDL_PATH}:/docker-entrypoint-initdb.d/{DDL_PATH.name}:ro', MYSQL_IMAGE],
            check=True, capture_output=True)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            logs = subprocess.run(['docker', 'logs', cls.container], capture_output=True, text=True)
            if 'ready for connections' in logs.stderr and 'port: 3306' in logs.stderr:
                return
            time.sleep(1)
        cls.tearDownClass()
        raise RuntimeError('throwaway MySQL did not become ready')

    @classmethod
    def tearDownClass(cls):
        subprocess.run(['docker', 'rm', '-f', cls.container], capture_output=True)
        subprocess.run(['docker', 'network', 'rm', cls.network], capture_output=True)

    def rows(self):
        out = subprocess.run(
            ['docker', 'exec', self.container, 'mysql', '-uroot', f'-p{ROOT_PASSWORD}', '--batch',
             '--skip-column-names', 'coin_market', '-e',
             'SELECT name, time, ' + ', '.join(f'`{f}`' for f in FIELDS) + ' FROM coin_average ORDER BY name, time'],
            check=True, capture_output=True, text=True).stdout
        return [line.split('\t') for line in out.splitlines()]

    def test_replayed_batch_keeps_one_row_per_name_time_with_the_same_values(self):
        probe = subprocess.run(
            ['docker', 'run', '--rm', '--network', self.network, '-v', f'{REPO}:/repo:ro',
             '-w', '/repo/apps/average', '-e', 'PYTHONDONTWRITEBYTECODE=1',
             '-e', 'PYTHONPATH=/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip',
             SPARK_IMAGE, 'python3', 'tests/coin_average_jdbc_probe.py',
             f'jdbc:mysql://{self.container}:3306/coin_market', 'root', ROOT_PASSWORD],
            capture_output=True, text=True)
        self.assertEqual(probe.returncode, 0, probe.stderr[-3000:])
        self.assertEqual(self.rows(), [
            ['BTC', str(END_MS), *['100.333333333333333333'] * len(FIELDS)],
            # (MAX + -MAX / 2) / 2 = MAX / 4 rounds up through every 9.
            ['ETH', str(END_MS), *['25000000000000000000.000000000000000000'] * len(FIELDS)],
        ])


if __name__ == '__main__':
    unittest.main()
