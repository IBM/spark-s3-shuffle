#!/usr/bin/env bash
#
# Copyright 2022- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache2.0
#
set -euo pipefail

ROOT="$(cd "`dirname $0`/../" && pwd)"
cd "${ROOT}"

VERSION=$(git describe --tags || echo "vrev-$(git rev-parse --short HEAD)")
VERSION="${VERSION:1}"
echo "Version: ${VERSION}"

if [[ "${SPARK_VERSION:-}" == "" ]]; then
  SPARK_VERSION=""
else
  SPARK_VERSION="-${SPARK_VERSION}"
fi

if [[ "${SCALA_VERSION:-}" == "" ]]; then
  SCALA_VERSION=""
else
  if [[ "${SCALA_VERSION}" =~ "2.13" ]]; then
    SCALA_VERSION="_2.13"
  elif [[ "${SCALA_VERSION}" =~ "2.12" ]]; then
    SCALA_VERSION="_2.12"
  else
    echo "Unknown scala version: ${SCALA_VERSION}"
    SCALA_VERSION="_UNKNOWN"
    exit 1
  fi
fi


FILE=$(ls target/scala*/sbt*/*.jar)
POM=$(ls target/scala*/sbt*/*.pom)

mvn deploy:deploy-file \
  -Durl=https://maven.pkg.github.com/IBM/spark-s3-shuffle \
  -DrepositoryId=github \
  -Dfile="${FILE}" \
  -Dpom="${POM}" \
  -DgroupId=com.ibm \
  -DartifactId="spark-s3-shuffle${SCALA_VERSION}" \
  -Dversion="${VERSION}${SPARK_VERSION}"
