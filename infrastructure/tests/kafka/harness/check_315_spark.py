"""Read committed 9x35 benchmark Preprocessed records with Spark Kafka Source."""
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


TOPICS = (
    'RegionKorea_TickerPreprocessing', 'RegionNE_TickerPreprocessing',
    'RegionAsia_TickerPreprocessing', 'RegionKorea_OrderbookPreprocessing',
    'RegionNE_OrderbookPreprocessing', 'RegionAsia_OrderbookPreprocessing',
)
ARTIFACTS = Path('/artifacts')


def main():
    expected = json.loads((ARTIFACTS / 'bench_315_downstream.json').read_text())
    spark = SparkSession.builder.appName('bench-315-preprocessed-read').getOrCreate()
    try:
        source = (spark.read.format('kafka')
                  .option('kafka.bootstrap.servers', 'kafka-1:9092,kafka-2:9092,kafka-3:9092')
                  .option('subscribe', ','.join(TOPICS))
                  .option('startingOffsets', 'earliest')
                  .option('endingOffsets', 'latest')
                  .option('kafka.isolation.level', 'read_committed')
                  .load())
        rows = source.select('topic', 'key', 'value', 'partition', 'offset').cache()
        count = rows.count()
        by_topic = {row.topic: row['count'] for row in rows.groupBy('topic').count().collect()}
        distinct_keys = rows.select('key').distinct().count()
        assert count == expected['preprocessed_records'] == 1260, count
        assert by_topic == expected['preprocessed_topic_counts'], by_topic
        assert distinct_keys == 315, distinct_keys
        assert rows.filter(col('key').isNull() | col('value').isNull()).count() == 0
        result = {'read_committed_records': count, 'distinct_trade_pair_keys': distinct_keys,
                  'topic_counts': by_topic}
        (ARTIFACTS / 'bench_315_spark.json').write_text(json.dumps(result, indent=2) + '\n')
        print(f'BENCH_315_SPARK_OK records={count} distinct_keys={distinct_keys}')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
