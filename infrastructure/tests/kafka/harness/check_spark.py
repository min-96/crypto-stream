"""Verify all Preprocessed sources and per-query checkpoint resume across Spark processes."""
import json
import sys
import uuid
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import BinaryType, StructField, StructType


BOOTSTRAP = 'kafka-1:9092,kafka-2:9092,kafka-3:9092'
TOPICS = (
    'RegionKorea_TickerPreprocessing', 'RegionKorea_OrderbookPreprocessing',
    'RegionNE_TickerPreprocessing', 'RegionNE_OrderbookPreprocessing',
    'RegionAsia_TickerPreprocessing', 'RegionAsia_OrderbookPreprocessing',
)
KEY = b'BINANCE:BTC:USDT'
ARTIFACT = Path('/artifacts/spark.json')
CHECKPOINT_ROOT = Path('/checkpoints/preprocessed-source')


def checkpoint(topic):
    return str(CHECKPOINT_ROOT / topic)


def value_for(marker):
    return ('{"marker":"' + marker + '"}').encode()


def run_query(spark, topic, marker):
    seen = []
    marker_prefix = marker.rsplit('-', 1)[0].encode()
    stream = (spark.readStream.format('kafka')
              .option('kafka.bootstrap.servers', BOOTSTRAP)
              .option('subscribe', topic)
              .option('startingOffsets', 'earliest')
              .option('failOnDataLoss', 'true')
              .option('kafka.isolation.level', 'read_committed')
              .load().select('key', 'value', 'topic', 'partition', 'offset'))
    assert isinstance(stream.schema['key'].dataType, BinaryType)
    assert isinstance(stream.schema['value'].dataType, BinaryType)

    def check_batch(batch, batch_id):
        for record in batch.collect():
            if marker_prefix not in record.value:
                continue
            if record.topic != topic or record.partition != 0:
                raise AssertionError(f'Spark source mismatch: {record}')
            actual_marker = json.loads(bytes(record.value))['marker']
            if bytes(record.key) != KEY or bytes(record.value) != value_for(actual_marker):
                raise AssertionError(f'Spark Key/Value mismatch: {record}')
            seen.append({'marker': actual_marker, 'offset': record.offset, 'batch_id': batch_id})

    query = (stream.writeStream.foreachBatch(check_batch)
             .option('checkpointLocation', checkpoint(topic))
             .trigger(availableNow=True).start())
    query.awaitTermination(120)
    if query.isActive:
        query.stop()
        raise TimeoutError(f'Spark source query did not finish: {topic}')
    if query.exception() is not None:
        raise RuntimeError(str(query.exception()))
    return seen


def seed(spark, topic, marker):
    schema = StructType([StructField('key', BinaryType()), StructField('value', BinaryType())])
    spark.createDataFrame([(KEY, value_for(marker))], schema).write.format('kafka').option(
        'kafka.bootstrap.servers', BOOTSTRAP).option('topic', topic).save()


def main(phase):
    assert phase in ('first', 'resume')
    assert len({checkpoint(topic) for topic in TOPICS}) == len(TOPICS)
    spark = SparkSession.builder.appName('kafka-harness-preprocessed-source-' + phase).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        if phase == 'first':
            prefix = 'spark-' + uuid.uuid4().hex
            evidence = {'key': KEY.decode(), 'topics': {}}
            for topic in TOPICS:
                marker = f'{prefix}-{topic}-first'
                seed(spark, topic, marker)
                seen = run_query(spark, topic, marker)
                assert len(seen) == 1 and seen[0]['marker'] == marker, (topic, seen)
                evidence['topics'][topic] = {
                    'checkpoint': checkpoint(topic), 'first_marker': marker,
                    'first_value': value_for(marker).decode(), 'first_offset': seen[0]['offset'],
                }
            ARTIFACT.write_text(json.dumps(evidence, indent=2))
            print('SPARK_PREPROCESSED_FIRST_OK topics=6')
        else:
            evidence = json.loads(ARTIFACT.read_text())
            assert evidence['key'] == KEY.decode()
            assert set(evidence['topics']) == set(TOPICS)
            for topic in TOPICS:
                item = evidence['topics'][topic]
                assert item['checkpoint'] == checkpoint(topic)
                marker = item['first_marker'][:-len('-first')] + '-second'
                seed(spark, topic, marker)
                seen = run_query(spark, topic, marker)
                assert (len(seen) == 1 and seen[0]['marker'] == marker
                        and seen[0]['offset'] > item['first_offset']), (topic, seen)
                # A second start with no new data must not replay either marker.
                assert run_query(spark, topic, marker) == [], topic
                item['second_marker'] = marker
                item['second_value'] = value_for(marker).decode()
                item['second_offset'] = seen[0]['offset']
                item['replay_count'] = 0
                print(f'SPARK_CHECKPOINT_OK topic={topic} first_offset={item["first_offset"]} '
                      f'second_offset={item["second_offset"]} replay_count=0')
            ARTIFACT.write_text(json.dumps(evidence, indent=2))
            print('SPARK_PREPROCESSED_RESUME_OK topics=6')
    finally:
        spark.stop()


if __name__ == '__main__':
    main(sys.argv[1])
