#!/usr/bin/env bash
#
# Copyright 2023- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache-2.0
#
# A simple benchmark script.
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SCRIPT_DIR}/"

./terasort/build.sh
./sql/build.sh

REPEAT=${REPEAT:-20}
export CHECKSUM_ENABLED=${CHECKSUM_ENABLED:-false}
export USE_S3_SHUFFLE=0
export USE_NFS_SHUFFLE=0

# TeraSort experiments
export INSTANCES=4
TERASORT_SIZES=(
   1g
   10g
   100g
)
for size in "${TERASORT_SIZES[@]}"; 
do
    for ((i = 0 ; i < ${REPEAT} ; i++));
    do
       export SIZE=$size
       export USE_S3_SHUFFLE=0
        export USE_NFS_SHUFFLE=0
       ./terasort/run.sh || true
       mc rm -r --force zac/zrlio-tmp

       export USE_S3_SHUFFLE=0
       export USE_NFS_SHUFFLE=1
       ./terasort/run.sh || true
       mc rm -r --force zac/zrlio-tmp

       export USE_S3_SHUFFLE=1
       export USE_NFS_SHUFFLE=0
       ./terasort/run.sh || true
       mc rm -r --force zac/zrlio-tmp
   done
done

# SQL experiments
export INSTANCES=12
export SIZE=1000
SQL_QUERIES=(
    q5  #  9.6 GB shuffle data
    q49 #  1.1 GB shuffle data
    q75 #   20 GB shuffle data
    q67 #   66 GB shuffle data
)

for ((i = 0 ; i < ${REPEAT} ; i++));
do
    for query in "${SQL_QUERIES[@]}"; do
        export USE_S3_SHUFFLE=0
        export USE_NFS_SHUFFLE=1
        ./sql/run_single_query.sh $query || true
        
    
        export USE_S3_SHUFFLE=0
        export USE_NFS_SHUFFLE=1
        ./sql/run_single_query.sh $query || true

        export USE_S3_SHUFFLE=1
        export USE_NFS_SHUFFLE=0
        ./sql/run_single_query.sh $query || true
    done
done
