# Flink 1.18 + PyFlink + Kafka SQL connector. Pattern from the Flink docs "Using Flink Python on Docker".
ARG FLINK_IMAGE=flink:1.18.1-scala_2.12-java11
FROM ${FLINK_IMAGE}
ARG FLINK_VERSION=1.18.1
ARG KAFKA_CONNECTOR_VERSION=3.1.0-1.18
USER root
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends python3 python3-pip python3-dev curl \
 && rm -rf /var/lib/apt/lists/* \
 && ln -sf /usr/bin/python3 /usr/bin/python
RUN pip3 install --no-cache-dir "apache-flink==${FLINK_VERSION}" pyyaml==6.0.2
RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"
# Pre-create state dirs owned by flink so the named volumes inherit the ownership.
RUN mkdir -p /opt/flink/checkpoints /opt/flink/savepoints \
 && chown -R flink:flink /opt/flink/checkpoints /opt/flink/savepoints
USER flink
