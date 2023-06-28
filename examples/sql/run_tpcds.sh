#!/usr/bin/env bash
#
# Copyright 2023- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache-2.0
#
set -euo pipefail

# Make sure you adapt and source ../config.sh first.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SCRIPT_DIR}"

export SIZE=${SIZE:-100}
export PROCESS_TAG=tpcds-${SIZE}
./run_single_query.sh tpcds