#!/usr/bin/env bash
#
# Copyright 2023- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache-2.0
#

ARG SPARK_VERSION=3.3.2

FROM debian:11.5 as java-prerequisites

ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN true

RUN apt-get clean && apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-11-jdk-headless git maven wget \
    && apt-get clean

###
# Profiling
FROM java-prerequisites as profiler-builder

RUN git clone https://github.com/uber-common/jvm-profiler \
    && cd /jvm-profiler \
    && mvn -P influxdb clean package

###
# SQL-Benchmarks
FROM java-prerequisites as sql-builder

RUN git clone https://github.com/zrlio/sql-benchmarks \
    && cd sql-benchmarks \
    && mvn package

# Result /sql-benchmarks/target/sql-benchmarks-1.0.jar

###
# S3-Shuffle plugin
FROM sbtscala/scala-sbt:eclipse-temurin-focal-11.0.17_8_1.9.1_2.12.18 as s3-shuffle-builder

ARG SPARK_VERSION
ENV SPARK_VERSION=${SPARK_VERSION}

COPY . /spark-s3-shuffle
RUN cd /spark-s3-shuffle \
    && sbt package \
    && ls target/scala-2.12/*.jar

###
# Spark Container
FROM docker.io/apache/spark-py:v${SPARK_VERSION}

ARG SPARK_VERSION
ARG S3_SHUFFLE_VERSION=0.9.4
ARG HADOOP_VERSION=3.3.2
ARG AWS_SDK_VERSION=1.11.1026

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar "${SPARK_HOME}/jars/hadoop-aws-${HADOOP_VERSION}.jar"
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-cloud-storage/${HADOOP_VERSION}/hadoop-cloud-storage-${HADOOP_VERSION}.jar "${SPARK_HOME}/jars/hadoop-cloud-storage-${HADOOP_VERSION}.jar"
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar "${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# JVM-Profiler
COPY --from=profiler-builder /jvm-profiler/target/jvm-profiler-1.0.0.jar "${SPARK_HOME}/jars/"

# SQL
COPY --from=sql-builder /sql-benchmarks/target/sql-benchmarks-1.0.jar "${SPARK_HOME}/jars/"

# S3-Shuffle plugin
COPY --from=s3-shuffle-builder /spark-s3-shuffle/target/scala-2.12/*.jar "${SPARK_HOME}/jars/"
# Release
# ADD https://github.com/IBM/spark-s3-shuffle/releases/download/v${S3_SHUFFLE_VERSION}/spark-s3-shuffle_2.12-${SPARK_VERSION}_${S3_SHUFFLE_VERSION}.jar "${SPARK_HOME}/jars/spark-s3-shuffle_2.12-${SPARK_VERSION}_${S3_SHUFFLE_VERSION}.jar"

ADD examples/profiler_config.yml /

USER root
RUN chmod +r ${SPARK_HOME}/jars/*.jar

USER ${SPARK_USER}