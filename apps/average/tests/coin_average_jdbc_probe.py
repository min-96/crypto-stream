"""Run by test_coin_average_mysql.py inside the Spark image: write one AVG-07 batch twice over JDBC.

Usage: coin_average_jdbc_probe.py JDBC_URL USER PASSWORD
"""

import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from coin_average import batch_writer, jdbc_connector
from ticker_input import DECIMAL_TYPE, FIELDS

END_MS = 1793318460000
MAX = '99999999999999999999.999999999999999999'

SCHEMA = StructType([StructField('time', LongType()), StructField('market', StringType()),
                     StructField('coin_symbol', StringType()), StructField('message_count', LongType()),
                     *(StructField(f'{field}_sum', DECIMAL_TYPE) for field in FIELDS)])
ROWS = [
    (END_MS, 'UPBIT', 'BTC', 3, *[Decimal(301)] * len(FIELDS)),
    (END_MS, 'UPBIT', 'ETH', 1, *[Decimal(MAX)] * len(FIELDS)),
    # From the string: unary minus on a Decimal would round to the default 28-digit context.
    (END_MS, 'BITHUMB', 'ETH', 2, *[Decimal('-' + MAX)] * len(FIELDS)),
]

spark = SparkSession.builder.master('local[1]').config('spark.ui.enabled', 'false').getOrCreate()
batch = spark.createDataFrame(ROWS, SCHEMA)
write = batch_writer(jdbc_connector(spark, *sys.argv[1:4]))
write(batch, 0)
write(batch, 0)  # replay of the same batch after a restart
spark.stop()
