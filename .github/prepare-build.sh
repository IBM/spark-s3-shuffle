#!/usr/bin/env bash
#
# Copyright 2022- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache2.0
#
set -euo pipefail

ROOT="$(cd "`dirname $0`/../" && pwd)"
cd "${ROOT}"

SCALA_VERSION=${SCALA_VERSION:-""}
if [[ "${SCALA_VERSION:0:4}" == "2.13" ]]; then
  echo "Removing tests from build since ${SCALA_VERSION} is not supported!"
  sed -i "/TRAVIS_SCALA_WORKAROUND_REMOVE_LINE/d" "${ROOT}/build.sbt"
  rm -rf "${ROOT}/src/test"
fi
