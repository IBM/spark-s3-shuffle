#!/usr/bin/env bash
#
# Copyright 2022- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache2.0
#
set -euo pipefail

ROOT="$(cd "`dirname $0`/../" && pwd)"
cd "${ROOT}"

VERSION=$(git describe || echo "vrev-$(git rev-parse --short HEAD)")
VERSION=${VERSION:1}
echo "Version: ${VERSION}"

FILE=$(ls target/scala*/*.jar)
POM=$(ls target/scala*/*.pom)

mvn deploy:deploy-file \
  -Durl=https://maven.pkg.github.com/IBM/spark-s3-shuffle \
  -DrepositoryId=github \
  -Dfile=$FILE \
  -Dpom=$POM
  -DgroupId=com.yourcompany \
  -DartifactId=yourproject \
  -Dversion=$VERSION
