"""A representative average: RegionKorea_TickerPreprocessing → 1-minute windows → MySQL coin_average."""

import os

from pyspark.sql import SparkSession

from coin_average import batch_writer, jdbc_connector
from exchange_window import aggregate_exchange_window
from ticker_input import parse_preprocessed_ticker

TOPIC = 'RegionKorea_TickerPreprocessing'


def create_spark():
    spark = (SparkSession.builder.appName('coin-average-korea-ticker')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', '2').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    return spark


def kafka_source(spark, bootstrap_servers):
    return (spark.readStream.format('kafka')
            .option('kafka.bootstrap.servers', bootstrap_servers)
            .option('subscribe', TOPIC)
            .option('startingOffsets', 'earliest')
            .option('kafka.isolation.level', 'read_committed')
            .option('failOnDataLoss', 'true')
            .load())


def start_query(source, write_batch, checkpoint, trigger=None):
    """Start the average query; `write_batch` is batch_writer(...) or a test wrapper around it.

    Production passes no trigger: Spark's default starts the next batch when the previous one
    ends. `trigger` (e.g. '5 seconds') exists only for the AVG-09 comparison.
    """
    writer = (aggregate_exchange_window(parse_preprocessed_ticker(source)).writeStream
              .outputMode('append').foreachBatch(write_batch)
              .option('checkpointLocation', checkpoint))
    if trigger is not None:
        writer = writer.trigger(processingTime=trigger)
    return writer.start()


def main(wrap_connect=None, wrap_writer=None, trigger=None):
    """Run until the query stops. The wrap_* hooks are for test entrypoints only (AVG-09)."""
    spark = create_spark()
    connect = jdbc_connector(spark, os.environ['MYSQL_JDBC_URL'],
                             os.environ['MYSQL_USER'], os.environ['MYSQL_PASSWORD'])
    if wrap_connect is not None:
        connect = wrap_connect(connect)
    write_batch = batch_writer(connect)
    if wrap_writer is not None:
        write_batch = wrap_writer(write_batch)
    query = start_query(kafka_source(spark, os.environ['KAFKA_BOOTSTRAP_SERVERS']), write_batch,
                        os.environ.get('AVERAGE_CHECKPOINT', '/checkpoints/average'), trigger)
    query.awaitTermination()


if __name__ == '__main__':
    main()
