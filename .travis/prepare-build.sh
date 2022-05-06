#!/usr/bin/env bash
#
# Copyright 2022- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache2.0
#
set -euo pipefail

ROOT="$(cd "`dirname $0`/../" && pwd)"
cd "${ROOT}"

VERSION=$(git rev-parse --short HEAD)
if [[ -n "${TRAVIS_TAG}" ]]; then
  VERSION="${TRAVIS_TAG}"
fi

# Change revision.
FILES=(
  "${ROOT}/pom.xml"
  "${ROOT}/src/main/scala/org/apache/spark/shuffle/sort/S3ShuffleManager.scala"
)

for i in "${FILES[@]}"; do
  echo "Replacing SNAPSHOT in ${i} with ${VERSION}"
  sed -i "s/SNAPSHOT/${VERSION}/g" "${i}"
done
