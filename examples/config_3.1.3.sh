#!/usr/bin/env bash
#
# Copyright 2023- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache-2.0
#

## Spark installation
# wget https://archive.apache.org/dist/spark/spark-3.1.3/spark-3.1.3-bin-hadoop3.2.tgz
export SPARK_HOME="/home/${USER}/software/spark-3.1.3-bin-hadoop3.2"

## Build Configuration for the Spark Docker container
# Determine dependencies using https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.12/3.1.3
export SPARK_VERSION=3.1.3
export S3_SHUFFLE_VERSION=0.7-spark3.1
export HADOOP_VERSION=3.2.0
export AWS_SDK_VERSION=1.11.375

# Kubernetes Config
export KUBERNETES_SERVER="https://9.4.244.122:6443" # export CodeEngine: "https://proxy.us-south.codeengine.cloud.ibm.com"
export KUBERNETES_PULL_SECRETS_NAME="zac-registry"
export KUBERNETES_NAMESPACE=$(kubectl config view --minify -o jsonpath='{..namespace}')
export KUBERNETES_SERVICE_ACCOUNT="${KUBERNETES_NAMESPACE}-manager"

# Image configuration
export DOCKER_REGISTRY="zac32.zurich.ibm.com"
export DOCKER_IMAGE_PREFIX="${PREFIX:-"${USER}/"}"

# S3 Config
export S3A_ENDPOINT="http://10.40.0.29:9000"
export S3A_ACCESS_KEY=${S3A_ACCESS_KEY:-$AWS_ACCESS_KEY_ID}
export S3A_SECRET_KEY=${S3A_SECRET_KEY:-$AWS_SECRET_ACCESS_KEY}
export S3A_OUTPUT_BUCKET=${S3A_BUCKET:-"zrlio-tmp"}
export SHUFFLE_DESTINATION=${SHUFFLE_DESTINATION:-"s3a://zrlio-tmp"}

# Datasets
## Terasort
# Contains the following datasets:
# - 1g
# - 10g
# - 100g
export TERASORT_BUCKET=zrlio-terasort

## TPCDS
# Contains the following datasets:
# - sf1000_parquet
# - sf100_parquet
# - sf10_parquet
export TPCDS_BUCKET=zrlio-tpcds
