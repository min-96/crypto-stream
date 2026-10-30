"""Publish the six Processed transport fixtures twice through Spark's Kafka sink."""
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, struct, to_json
from pyspark.sql.types import (
    DoubleType, FloatType, IntegerType, LongType, StringType, TimestampType,
)

sys.path.insert(0, '/schema')
import preprocess_schema
from processed_contract import TOPIC_CONTRACTS, identity_for, key_for


BOOTSTRAP = 'kafka-1:9092,kafka-2:9092,kafka-3:9092'
ARTIFACT = Path('/artifacts/processed_sink.json')
START = datetime(2026, 10, 29, 0, 0, tzinfo=timezone.utc)
END = datetime(2026, 10, 29, 0, 1, tzinfo=timezone.utc)


def fixture_value(field):
    if field.name == 'region':
        return 'ASIA'
    if field.name in ('coin_symbol', 'symbol'):
        return 'BTC'
    if field.name == 'market':
        return 'BINANCE'
    if field.name == 'start':
        return START
    if field.name == 'end':
        return END
    if isinstance(field.dataType, StringType):
        return 'fixture'
    if isinstance(field.dataType, (DoubleType, FloatType)):
        return 1.25
    if isinstance(field.dataType, (IntegerType, LongType)):
        return 1
    raise TypeError(f'unsupported fixture field: {field.name} {field.dataType}')


def main():
    spark = SparkSession.builder.appName('kaf08-processed-sink').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    evidence = {}
    try:
        for topic, contract in TOPIC_CONTRACTS.items():
            schema = getattr(preprocess_schema, contract['schema'])
            fixture = {field.name: fixture_value(field) for field in schema.fields}
            window = {
                **fixture,
                'start': '2026-10-29T00:00:00.000Z',
                'end': '2026-10-29T00:01:00.000Z',
            }
            key = key_for(topic, window)
            identity = identity_for(topic, key, window)
            data = spark.createDataFrame(
                [tuple(fixture[field.name] for field in schema.fields)], schema)
            payload = to_json(
                struct(*(col(field.name) for field in schema.fields)),
                {'timestampFormat': "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", 'timeZone': 'UTC',
                 'ignoreNullFields': 'false'},
            ).cast('binary')
            messages = data.select(lit(key.decode()).cast('binary').alias('key'),
                                   payload.alias('value'))
            expected_value = bytes(messages.collect()[0].value)
            decoded = json.loads(expected_value)
            assert set(decoded) == set(schema.fieldNames())
            assert decoded['start'] == window['start'] and decoded['end'] == window['end']
            for _ in range(2):
                messages.write.format('kafka').option(
                    'kafka.bootstrap.servers', BOOTSTRAP).option('topic', topic).save()
            evidence[topic] = {
                'schema': contract['schema'], 'fields': schema.fieldNames(),
                'field_types': {field.name: field.dataType.simpleString() for field in schema.fields},
                'key': key.decode(), 'identity': identity,
                'value_sha256': hashlib.sha256(expected_value).hexdigest(),
                'value': expected_value.decode('utf-8'),
            }
            print(f'PROCESSED_SPARK_SENT topic={topic} key={key.decode()} identity={identity}')
        ARTIFACT.write_text(json.dumps(evidence, indent=2))
        print('PROCESSED_SPARK_SINK_SENT topics=6 batches=2')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
