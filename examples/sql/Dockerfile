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
# SQL-Benchmarks
FROM java-prerequisites as sql-builder

RUN git clone https://github.com/zrlio/sql-benchmarks \
    && cd sql-benchmarks \
    && mvn package

# Result /sql-benchmarks/target/sql-benchmarks-1.0.jar

###
# Spark Container
FROM docker.io/apache/spark-py:v${SPARK_VERSION}

ARG SPARK_VERSION
ARG S3_SHUFFLE_VERSION=0.7
ARG HADOOP_VERSION=3.3.2
ARG AWS_SDK_VERSION=1.11.1026

ADD --chmod=444 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar "${SPARK_HOME}/jars/hadoop-aws-${HADOOP_VERSION}.jar"
ADD --chmod=444 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-cloud-storage/${HADOOP_VERSION}/hadoop-cloud-storage-${HADOOP_VERSION}.jar "${SPARK_HOME}/jars/hadoop-cloud-storage-${HADOOP_VERSION}.jar"
ADD --chmod=444 https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar "${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"
ADD --chmod=444 https://github.com/IBM/spark-s3-shuffle/releases/download/v${S3_SHUFFLE_VERSION}/spark-s3-shuffle_2.12-${SPARK_VERSION}_${S3_SHUFFLE_VERSION}.jar "${SPARK_HOME}/jars/spark-s3-shuffle_2.12-${SPARK_VERSION}_${S3_SHUFFLE_VERSION}.jar"

# SQL
COPY --from=sql-builder /sql-benchmarks/target/sql-benchmarks-1.0.jar "${SPARK_HOME}/jars/"

USER root
RUN chmod +r ${SPARK_HOME}/jars/*.jar

USER ${SPARK_USER}