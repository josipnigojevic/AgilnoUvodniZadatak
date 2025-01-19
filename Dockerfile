# -------------------------------------------------------------------------
# Dockerfile: Bundled environment with Java, Hadoop, Spark, and Python + PySpark
# (plus the code for this assignment)
# -------------------------------------------------------------------------
# This Docker image includes:
#   - OpenJDK 8
#   - Hadoop 3.x (with HADOOP_HOME)
#   - Spark 3.x (with SPARK_HOME)
#   - Python 3 + pip
#   - PySpark
#   - The entire project folder (copied into /app)
#
# Made by Josip Nigojević
# -------------------------------------------------------------------------

FROM openjdk:8-jdk
ARG HADOOP_VERSION=3.3.5
ARG SPARK_VERSION=3.4.1
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      curl \
      python3 \
      python3-pip && \
    rm -rf /var/lib/apt/lists/*
RUN python3 -m pip install --no-cache-dir pyspark
RUN curl -fSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -o /tmp/hadoop.tgz \
    && tar -xzf /tmp/hadoop.tgz -C /opt \
    && mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
    && rm /tmp/hadoop.tgz
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
RUN curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark \
    && rm /tmp/spark.tgz
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
WORKDIR /app
COPY . /app
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3
ENTRYPOINT ["/bin/bash"]
