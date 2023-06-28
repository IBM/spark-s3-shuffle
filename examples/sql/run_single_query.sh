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

QUERY=$1
SIZE=${SIZE:-1000}

# Shuffle on S3
export USE_S3_SHUFFLE=${USE_S3_SHUFFLE:-1}

export PROCESS_TAG=${PROCESS_TAG:-"sql-${QUERY}-${SIZE}"}

INPUT_DATA_PREFIX=s3a://${TPCDS_BUCKET}
OUTPUT_DATA_PREFIX=s3a://${S3A_OUTPUT_BUCKET}/output/sql-benchmarks/

./run_benchmark.sh \
    -t $QUERY \
    -i $INPUT_DATA_PREFIX/sf${SIZE}_parquet/ \
    -a save,${OUTPUT_DATA_PREFIX}/${QUERY}/${PROCESS_TAG}/${SIZE}
