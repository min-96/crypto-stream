"""Verify a Spark checkpoint fails when retention removes its next Kafka offset."""
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession


BOOTSTRAP = 'kafka-1:9092,kafka-2:9092,kafka-3:9092'
TOPIC = 'RegionAsia_TickerPreprocessing'
CHECKPOINT = '/checkpoints/kaf11-retention'
ARTIFACT = Path('/artifacts/retention.json')


def run_query(spark, marker, seen):
    source = (spark.readStream.format('kafka')
              .option('kafka.bootstrap.servers', BOOTSTRAP)
              .option('subscribe', TOPIC)
              .option('startingOffsets', 'earliest')
              .option('failOnDataLoss', 'true')
              .load().select('key', 'value', 'offset'))

    def batch_check(batch, _batch_id):
        for row in batch.collect():
            if marker.encode() in row.value:
                seen.append(row.offset)

    query = (source.writeStream.foreachBatch(batch_check)
             .option('checkpointLocation', CHECKPOINT)
             .trigger(availableNow=True).start())
    query.awaitTermination(90)
    if query.isActive:
        query.stop()
        raise TimeoutError('retention Spark query did not finish')
    if query.exception() is not None:
        raise RuntimeError(str(query.exception()))


def main(phase):
    evidence = json.loads(ARTIFACT.read_text())
    spark = SparkSession.builder.appName('kaf11-retention-' + phase).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        seen = []
        if phase == 'first':
            run_query(spark, evidence['marker'], seen)
            assert seen == [0], seen
            evidence['spark_first_seen_offset'] = 0
            ARTIFACT.write_text(json.dumps(evidence, indent=2))
            print('SPARK_RETENTION_CHECKPOINT_OK consumed_offset=0 next_offset=1')
        elif phase == 'resume':
            try:
                run_query(spark, evidence['marker'], seen)
            except Exception as error:
                message = str(error)
                if 'data loss' not in message.lower() and 'out of range' not in message.lower():
                    raise
                evidence['spark_failure'] = message[:2000]
                evidence['spark_replay_offsets'] = seen
                ARTIFACT.write_text(json.dumps(evidence, indent=2))
                print('SPARK_RETENTION_FAILURE_OK checkpoint_next=1')
            else:
                raise AssertionError('Spark silently resumed after checkpoint offset expired')
        else:
            raise ValueError(phase)
    finally:
        spark.stop()


if __name__ == '__main__':
    main(sys.argv[1])
