# Spark 3.5 (Java 17, Python 3.10, Ubuntu jammy) + small Python deps used by our jobs.
ARG SPARK_IMAGE=spark:3.5.3-scala2.12-java17-python3-ubuntu
FROM ${SPARK_IMAGE}
USER root
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt
ENV PATH="/opt/spark/bin:${PATH}"
