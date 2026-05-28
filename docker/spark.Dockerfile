FROM eclipse-temurin:17-jdk-jammy

ARG SPARK_VERSION=3.4.2
ARG HADOOP_VERSION=3

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV SPARK_NO_DAEMONIZE=true

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    python3 \
    python3-pip \
    procps \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

# Log4j2 config for structured JSON logging
COPY docker/log4j2.properties ${SPARK_HOME}/conf/log4j2.properties

RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR ${SPARK_HOME}
