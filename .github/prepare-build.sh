#!/usr/bin/env bash
#
# Copyright 2022- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache2.0
#
set -euo pipefail

ROOT="$(cd "`dirname $0`/../" && pwd)"
cd "${ROOT}"

VERSION=$(git describe --tags || echo "vrev-$(git rev-parse --short HEAD)")
VERSION=${VERSION:1}
echo "Version: ${VERSION}"

# Change revision.
FILES=(
  "${ROOT}/build.sbt"
  "${ROOT}/src/main/scala/org/apache/spark/shuffle/S3ShuffleManager.scala"
)

for i in "${FILES[@]}"; do
  echo "Replacing SNAPSHOT in ${i} with ${VERSION}"
  sed -i "s/SNAPSHOT/${VERSION}/g" "${i}"
done

SCALA_VERSION=${SCALA_VERSION:-""}
if [[ "${SCALA_VERSION:0:4}" == "2.13" ]]; then
  echo "Removing tests from build since ${SCALA_VERSION} is not supported!"
  sed -i "/TRAVIS_SCALA_WORKAROUND_REMOVE_LINE/d" "${ROOT}/build.sbt"
  rm -rf "${ROOT}/src/test"
fi
