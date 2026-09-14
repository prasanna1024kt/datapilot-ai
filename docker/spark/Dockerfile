FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu

USER root

# Python dependencies
RUN pip3 install --no-cache-dir PyYAML

# Hadoop AWS support
RUN curl -fL \
    -o /opt/spark/jars/hadoop-aws-3.4.2.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar \
    && curl -fL \
    -o /opt/spark/jars/bundle-2.29.52.jar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar \
    && curl -fL \
    -o /opt/spark/jars/hadoop-mapreduce-client-core-3.4.2.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/3.4.2/hadoop-mapreduce-client-core-3.4.2.jar

USER spark