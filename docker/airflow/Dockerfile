FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu AS spark

FROM apache/airflow:3.3.1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY --from=spark /opt/spark /opt/spark

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV SPARK_HOME=/opt/spark
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}"

RUN curl -fL \
    -o /opt/spark/jars/hadoop-aws-3.4.2.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar \
    && curl -fL \
    -o /opt/spark/jars/bundle-2.29.52.jar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar \
    && curl -fL \
    -o /opt/spark/jars/hadoop-mapreduce-client-core-3.4.2.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/3.4.2/hadoop-mapreduce-client-core-3.4.2.jar

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark