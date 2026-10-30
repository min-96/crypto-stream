from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType
import os
import uuid

spark = SparkSession.builder.appName('infra-connection-smoke').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
bootstrap = 'kafka-1:9092,kafka-2:9092,kafka-3:9092'
marker = 'spark-infra-' + uuid.uuid4().hex
source = 'koreaSocketDataInBTC-ticker'
target = 'TimeMetricsProcessedCoin'

# Spark Kafka sink: send a smoke marker to an existing topic.
spark.createDataFrame([(marker,)], ['value']).selectExpr('CAST(value AS STRING)').write.format('kafka').option('kafka.bootstrap.servers', bootstrap).option('topic', source).save()

# Spark Kafka source: read the marker and write it to another existing topic.
rows = (spark.read.format('kafka').option('kafka.bootstrap.servers', bootstrap)
        .option('subscribe', source).option('startingOffsets', 'earliest').option('endingOffsets', 'latest').load()
        .selectExpr('CAST(value AS STRING) AS value').filter(col('value') == marker))
if rows.count() != 1:
    raise RuntimeError('Kafka source did not return the marker exactly once')
rows.write.format('kafka').option('kafka.bootstrap.servers', bootstrap).option('topic', target).save()

# A separate verification table avoids inventing analysis rows.
url = 'jdbc:mysql://mysql:3306/coin_market'
props = {'user': os.environ['MYSQL_USER'], 'password': os.environ['MYSQL_PASSWORD'], 'driver': 'com.mysql.cj.jdbc.Driver'}
rows.withColumn('source', lit('spark-smoke')).write.jdbc(url, 'infra_smoke', mode='append', properties=props)
check = spark.read.jdbc(url, 'infra_smoke', properties=props).filter(col('value') == marker)
if check.count() != 1:
    raise RuntimeError('JDBC read did not return the marker exactly once')
print('INFRA_SMOKE_OK=' + marker)
spark.stop()
