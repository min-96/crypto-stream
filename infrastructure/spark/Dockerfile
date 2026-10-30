FROM apache/spark:3.5.3

USER root
RUN mkdir -p /tmp/spark-ivy && chown 185:185 /tmp/spark-ivy
USER 185
RUN /opt/spark/bin/spark-submit \
    --master 'local[1]' \
    --conf spark.jars.ivy=/tmp/spark-ivy \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.mysql:mysql-connector-j:8.4.0 \
    --class org.apache.spark.examples.SparkPi \
    /opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar 1 \
    >/tmp/spark-dependency-build.log 2>&1
USER root
RUN cp /tmp/spark-ivy/jars/*.jar /opt/spark/jars/ && \
    rm -rf /tmp/spark-ivy /tmp/spark-dependency-build.log
USER 185
