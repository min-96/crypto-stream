"""Verify that only committed Preprocessed records contribute to an average."""
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, get_json_object


BOOTSTRAP = 'kafka-1:9092,kafka-2:9092,kafka-3:9092'
TOPIC = 'RegionAsia_TickerPreprocessing'
ARTIFACT = Path('/artifacts/exactly_once.json')


def main():
    spark = SparkSession.builder.appName('kaf-eos-average').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        source = (spark.read.format('kafka')
                  .option('kafka.bootstrap.servers', BOOTSTRAP)
                  .option('subscribe', TOPIC)
                  .option('startingOffsets', 'earliest')
                  .option('endingOffsets', 'latest')
                  .option('kafka.isolation.level', 'read_committed')
                  .load())
        prices = source.select(
            get_json_object(col('value').cast('string'),
                            '$.data.trade_price').cast('double').alias('price'))
        result = prices.agg(avg('price').alias('average')).collect()[0]
        count = prices.count()
        assert count == 2 and result.average == 15.0, (count, result.average)
        evidence = json.loads(ARTIFACT.read_text())
        evidence['spark_read_committed_count'] = count
        evidence['spark_read_committed_average'] = result.average
        ARTIFACT.write_text(json.dumps(evidence, indent=2))
        print('EOS_SPARK_AVERAGE_OK count=2 mean=15')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
