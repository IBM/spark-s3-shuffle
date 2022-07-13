#!/usr/bin/env bash
#
# Copyright 2022- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache2.0
#
set -euo pipefail

ROOT="$(cd "`dirname $0`/../" && pwd)"
cd "${ROOT}"

GIT_DESCRIBE=$(git describe || echo "v0.0.1-test")
VERSION=${GIT_DESCRIBE:1}
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
