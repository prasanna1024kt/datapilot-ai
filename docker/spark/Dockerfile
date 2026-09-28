FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu

USER root

# ---------------------------------------------------------
# Python dependencies
# ---------------------------------------------------------

RUN pip3 install --no-cache-dir PyYAML

# ---------------------------------------------------------
# Hadoop AWS support
# ---------------------------------------------------------

RUN curl -fL \
    -o /opt/spark/jars/hadoop-aws-3.4.2.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar \
    && curl -fL \
    -o /opt/spark/jars/bundle-2.29.52.jar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar \
    && curl -fL \
    -o /opt/spark/jars/hadoop-mapreduce-client-core-3.4.2.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/3.4.2/hadoop-mapreduce-client-core-3.4.2.jar

# ---------------------------------------------------------
# Apache Iceberg
# Spark 4.1 + Scala 2.13
# ---------------------------------------------------------

RUN curl -fL \
    -o /opt/spark/jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar

# ---------------------------------------------------------
# Optional: verify installed JARs
# ---------------------------------------------------------

RUN test -f /opt/spark/jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
    && test -f /opt/spark/jars/hadoop-aws-3.4.2.jar \
    && test -f /opt/spark/jars/bundle-2.29.52.jar \
    && echo "Spark AWS + Iceberg JARs installed successfully"

USER spark