"""Infrastructure-only Structured Streaming smoke test; no market calculations."""
import os
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.appName('infra-structured-streaming-smoke').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
bootstrap = 'kafka-1:9092,kafka-2:9092,kafka-3:9092'
source = 'koreaSocketDataInBTC-ticker'
target = 'TimeMetricsProcessedCoin'
marker = 'structured-smoke-' + uuid.uuid4().hex
url = 'jdbc:mysql://mysql:3306/coin_market'
props = {'user': os.environ['MYSQL_USER'], 'password': os.environ['MYSQL_PASSWORD'], 'driver': 'com.mysql.cj.jdbc.Driver'}

# Seed one marker using the Kafka sink, then process it with readStream/writeStream.
spark.createDataFrame([(marker,)], ['value']).write.format('kafka').option('kafka.bootstrap.servers', bootstrap).option('topic', source).save()
stream = (spark.readStream.format('kafka')
          .option('kafka.bootstrap.servers', bootstrap)
          .option('subscribe', source)
          .option('startingOffsets', 'earliest')
          .load()
          .selectExpr('CAST(value AS STRING) AS value')
          .filter(col('value') == marker))

def write_batch(batch, batch_id):
    if batch.isEmpty():
        return
    batch.select('value').write.format('kafka').option('kafka.bootstrap.servers', bootstrap).option('topic', target).save()
    batch.withColumn('source', lit('structured-streaming-smoke')).write.jdbc(url, 'infra_smoke', mode='append', properties=props)

query = (stream.writeStream.foreachBatch(write_batch)
         .option('checkpointLocation', '/checkpoints/structured-smoke-' + marker)
         .trigger(availableNow=True)
         .start())
query.awaitTermination(120)
if query.isActive:
    query.stop()
    raise RuntimeError('Streaming query timed out')
if query.exception() is not None:
    raise RuntimeError(str(query.exception()))

kafka_count = (spark.read.format('kafka').option('kafka.bootstrap.servers', bootstrap)
               .option('subscribe', target).option('startingOffsets', 'earliest')
               .option('endingOffsets', 'latest').load()
               .selectExpr('CAST(value AS STRING) AS value')
               .filter(col('value') == marker).count())
mysql_count = spark.read.jdbc(url, 'infra_smoke', properties=props).filter(col('value') == marker).count()
if kafka_count != 1 or mysql_count != 1:
    raise RuntimeError(f'Unexpected output counts: Kafka={kafka_count}, MySQL={mysql_count}')
print('STRUCTURED_STREAMING_SMOKE_OK=' + marker)
spark.stop()
