#!/usr/bin/env bash
#
# Copyright 2023- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache-2.0
#
set -exuo pipefail

# Make sure you adapt and source ../config.sh first.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SCRIPT_DIR}"

ROOT="$(pwd)"
TIMESTAMP=$(date -u "+%FT%H%M%SZ")
PROCESS_TAG=${PROCESS_TAG}-${TIMESTAMP}

IMAGE="${DOCKER_REGISTRY}/${DOCKER_IMAGE_PREFIX}spark-sql:${SPARK_VERSION}"

DRIVER_CPU=${DRIVER_CPU:-4}
DRIVER_MEM=${DRIVER_MEM:-13000M}
DRIVER_MEMORY_OVERHEAD=${DRIVER_MEMORY_OVERHEAD:-3000M}
EXECUTOR_CPU=${EXECUTOR_CPU:-4}
EXECUTOR_MEM=${EXECUTOR_MEM:-13000M}
EXECUTOR_MEMORY_OVERHEAD=${EXECUTOR_MEMORY_OVERHEAD:-19000M} # 16G is allocated for spark.kubernetes.local.dirs.tmpfs
INSTANCES=${INSTANCES:-4}

CHECKSUM_ENABLED=${CHECKSUM_ENABLED:-"true"}
USE_FALLBACK_FETCH=${USE_FALLBACK_FETCH:-"false"}

EXTRA_CLASSPATHS='/opt/spark/jars/*'
EXECUTOR_JAVA_OPTIONS="-Dsun.nio.PageAlignDirectMemory=true"
DRIVER_JAVA_OPTIONS="-Dsun.nio.PageAlignDirectMemory=true"

export SPARK_EXECUTOR_CORES=$EXECUTOR_CPU
export SPARK_DRIVER_MEMORY=$DRIVER_MEM
export SPARK_EXECUTOR_MEMORY=$EXECUTOR_MEM

BLOCK_SIZE=${BLOCK_SIZE:-128}
LOGGING=(
    --conf spark.eventLog.enabled=true
    --conf spark.eventLog.dir=file:///spark-logs
)

SPARK_HADOOP_S3A_CONFIG=(
    # Required
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
    --conf spark.hadoop.fs.s3a.access.key=${S3A_ACCESS_KEY}
    --conf spark.hadoop.fs.s3a.secret.key=${S3A_SECRET_KEY}
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false
    --conf spark.hadoop.fs.s3a.endpoint=${S3A_ENDPOINT}
    --conf spark.hadoop.fs.s3a.path.style.access=true
    --conf spark.hadoop.fs.s3a.fast.upload=true
    --conf spark.hadoop.fs.s3a.block.size=$(($BLOCK_SIZE * 1024 * 1024))
    --conf spark.hadoop.fs.s3a.fast.upload.buffer=array
)

SPARK_S3_SHUFFLE_CONFIG=(
    --conf spark.shuffle.manager="org.apache.spark.shuffle.sort.S3ShuffleManager"
    --conf spark.shuffle.sort.io.plugin.class=org.apache.spark.shuffle.S3ShuffleDataIO
    --conf spark.shuffle.checksum.enabled=${CHECKSUM_ENABLED}
    --conf spark.shuffle.s3.rootDir=${SHUFFLE_DESTINATION}
    # --conf spark.shuffle.s3.useSparkShuffleFetch=${USE_FALLBACK_FETCH}
    # --conf spark.storage.decommission.fallbackStorage.path=${SHUFFLE_DESTINATION}
)

if (( "$USE_S3_SHUFFLE" == 0 )); then
    SPARK_S3_SHUFFLE_CONFIG=(
            --conf spark.shuffle.s3.rootDir=NONE
    )
else
    PROCESS_TAG="${PROCESS_TAG}-s3shuffle"
fi

SPARK_KUBERNETES_TEMPLATES=(
    --conf spark.kubernetes.executor.podTemplateFile=${SCRIPT_DIR}/../templates/executor.yml
    --conf spark.kubernetes.driver.podTemplateFile=${SCRIPT_DIR}/../templates/driver.yml
)

USE_NFS_SHUFFLE=${USE_NFS_SHUFFLE:-0}
if (( "$USE_NFS_SHUFFLE" == 1 )); then
    PROCESS_TAG="${PROCESS_TAG}-s3shuffle-nfs"
    SPARK_S3_SHUFFLE_CONFIG=(
        --conf spark.shuffle.manager="org.apache.spark.shuffle.sort.S3ShuffleManager"
        --conf spark.shuffle.sort.io.plugin.class=org.apache.spark.shuffle.S3ShuffleDataIO
        --conf spark.shuffle.checksum.enabled=${CHECKSUM_ENABLED}
        --conf spark.shuffle.s3.rootDir=file:///nfs/
        --conf spark.kubernetes.executor.podTemplateFile=${SCRIPT_DIR}/../templates/executor_nfs.yml
        --conf spark.kubernetes.driver.podTemplateFile=${SCRIPT_DIR}/../templates/driver_nfs.yml
        --conf spark.hadoop.fs.file.block.size=$((128*1024*1024))
        # --conf spark.shuffle.s3.useSparkShuffleFetch=${USE_FALLBACK_FETCH}
        # --conf spark.storage.decommission.fallbackStorage.path=file:///nfs/
    )

    SPARK_KUBERNETES_TEMPLATES=(
        --conf spark.kubernetes.executor.podTemplateFile=${SCRIPT_DIR}/../templates/executor_nfs.yml
        --conf spark.kubernetes.driver.podTemplateFile=${SCRIPT_DIR}/../templates/driver_nfs.yml
    )
fi

if [[ "${USE_FALLBACK_FETCH}" == "true" ]]; then
    PROCESS_TAG="${PROCESS_TAG}-fallback"
fi

EXTRA_OPTIONS=(
)
if (( "${USE_TRANSFER_TO:-1}" == 0 )); then
    PROCESS_TAG="${PROCESS_TAG}-transferTo_false"
    EXTRA_OPTIONS=(
        --conf spark.file.transferTo=false \
    )
fi

USE_PROFILER=${USE_PROFILER:-0}
if (( "${USE_PROFILER}" == 1 )); then
    PROFILER_CONFIG="reporter=com.uber.profiling.reporters.InfluxDBOutputReporter,configProvider=com.uber.profiling.YamlConfigProvider,configFile=/profiler_config.yml,metricInterval=5000,sampleInterval=5000,ioProfiling=true"
    DRIVER_JAVA_OPTIONS="${DRIVER_JAVA_OPTIONS} -javaagent:/opt/spark/jars/jvm-profiler-1.0.0.jar=${PROFILER_CONFIG}"
    EXECUTOR_JAVA_OPTIONS="${EXECUTOR_JAVA_OPTIONS} -javaagent:/opt/spark/jars/jvm-profiler-1.0.0.jar=${PROFILER_CONFIG}"
fi

JAVA_DEBUG=${JAVA_DEBUG:-0}
if (( "${JAVA_DEBUG}" == 1 )); then
    DRIVER_JAVA_OPTIONS="${DRIVER_JAVA_OPTIONS} -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
    EXECUTOR_JAVA_OPTIONS="${EXECUTOR_JAVA_OPTIONS} -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
fi

${SPARK_HOME}/bin/spark-submit \
    --master k8s://$KUBERNETES_SERVER \
    --deploy-mode cluster \
    \
        --conf "spark.driver.extraJavaOptions=${DRIVER_JAVA_OPTIONS}" \
        --conf "spark.executor.extraJavaOptions=${EXECUTOR_JAVA_OPTIONS}" \
    \
    --name ce-${PROCESS_TAG}-bs${BLOCK_SIZE}MiB-${INSTANCES}x${EXECUTOR_CPU}--${EXECUTOR_MEM} \
    --conf spark.serializer="org.apache.spark.serializer.KryoSerializer" \
    --conf spark.kryoserializer.buffer=128mb \
    --conf spark.executor.instances=$INSTANCES \
    "${EXTRA_OPTIONS[@]}" \
    "${LOGGING[@]}" \
    "${SPARK_HADOOP_S3A_CONFIG[@]}" \
    "${SPARK_S3_SHUFFLE_CONFIG[@]}" \
    "${SPARK_KUBERNETES_TEMPLATES[@]}" \
    --conf spark.ui.prometheus.enabled=true \
    --conf spark.network.timeout=10000 \
    --conf spark.executor.heartbeatInterval=20000 \
    --conf spark.kubernetes.local.dirs.tmpfs=true \
    --conf spark.kubernetes.appKillPodDeletionGracePeriod=5 \
    --conf spark.kubernetes.container.image.pullSecrets=${KUBERNETES_PULL_SECRETS_NAME} \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=${KUBERNETES_SERVICE_ACCOUNT} \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.driver.memoryOverhead=$DRIVER_MEMORY_OVERHEAD \
    --conf spark.kubernetes.driver.request.cores=$DRIVER_CPU \
    --conf spark.kubernetes.driver.limit.cores=$DRIVER_CPU \
    --conf spark.executor.memoryOverhead=$EXECUTOR_MEMORY_OVERHEAD \
    --conf spark.kubernetes.executor.request.cores=$EXECUTOR_CPU \
    --conf spark.kubernetes.executor.limit.cores=$EXECUTOR_CPU \
    --conf spark.kubernetes.container.image=$IMAGE \
    --conf spark.kubernetes.namespace=$KUBERNETES_NAMESPACE \
    --class com.ibm.crail.benchmarks.Main \
    local:///opt/spark/jars/sql-benchmarks-1.0.jar \
    "$@"
