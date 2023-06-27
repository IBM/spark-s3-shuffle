#!/usr/bin/env bash
#
# Copyright 2023- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache-2.0
#
set -euo pipefail

# Make sure you adapt and source ../config.sh first.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SCRIPT_DIR}"
ROOT="$(pwd)"

REGISTRY="${REGISTRY:-zac32.zurich.ibm.com}"
PREFIX="${PREFIX:-${USER}}"

DOCKERFILE="Dockerfile"
[[ "${SPARK_VERSION}" =~ ^3.1. ]] && DOCKERFILE="Dockerfile_3.1"

IMAGE="${REGISTRY}/${PREFIX}/spark-sql:${SPARK_VERSION}"
docker build -t "${IMAGE}" \
    --build-arg SPARK_VERSION=${SPARK_VERSION} \
    --build-arg S3_SHUFFLE_VERSION=${S3_SHUFFLE_VERSION} \
    --build-arg HADOOP_VERSION=${HADOOP_VERSION} \
    --build-arg AWS_SDK_VERSION=${AWS_SDK_VERSION} \
    -f $DOCKERFILE .

docker push "${IMAGE}"
