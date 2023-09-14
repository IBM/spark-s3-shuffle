# Shuffle Plugin for Apache Spark and S3 compatible storage services

This plugin allows storing [Apache Spark](https://spark.apache.org/) shuffle data on S3 compatible object storage (e.g.
S3A, COS). It uses the Java Hadoop-Filesystem abstraction for interoperability for COS, S3A and even local file systems.

Builds for Spark 3.1.x are tagged with `vX.X-spark3.1` and tracked on
branch [spark-3.1](https://github.com/IBM/spark-s3-shuffle/tree/spark-3.1).

*Note*: This plugin is based on [Apache Spark Pull Request #34864](https://github.com/apache/spark/pull/34864/files). It
has since been significantly rewritten.

Examples are available [here](./examples).

## Building

```bash
sbt package  # Creates a minimal jar.
sbt assembly # Creates the full assembly with all dependencies, notably hadoop cloud.
 ```

## Required configuration

These configuration values need to be passed to Spark to load and configure the plugin:

- `spark.shuffle.manager`: The shuffle manager. Needs to be set to `org.apache.spark.shuffle.sort.S3ShuffleManager`.
- `spark.shuffle.sort.io.plugin.class`: The sort io plugin class. Needs to be set to
  `org.apache.spark.shuffle.S3ShuffleDataIO`.
- `spark.shuffle.s3.rootDir`: Root dir for the shuffle files. Examples:
    - `s3a://zrlio-tmp/` (Hadoop-AWS + AWS-SDK)
    - `cos://zrlio-tmp.resources/` (Hadoop-Cloud + Stocator)

  Individual blocks are prefixed in order to get improved performance when accessing them on the remote filesystem.
  The generated paths look like this: `${rootDir}/${mapId % 10}/${appId}/${shuffleId}/ShuffleBlock{.data / .index}`.

  The number of prefixes can be controlled with the option `spark.shuffle.s3.folderPrefixes`.

### Features

Changing these values might have an impact on performance.

- `spark.shuffle.s3.bufferSize`: Default buffer size when writing (default: `8388608`)
- `spark.shuffle.s3.maxBufferSizeTask`: Maximum size of the buffered output streams per task (default: `134217728`)
- `spark.shuffle.s3.maxConcurrencyTask`: Maximum per task concurrency. Computed by analysing the IO latencies (
  default: `10`).
- `spark.shuffle.s3.cachePartitionLengths`: Cache partition lengths in memory (default: `true`)
- `spark.shuffle.s3.cacheChecksums`: Cache checksums in memory (default: `true`)
- `spark.shuffle.s3.cleanup`: Cleanup the shuffle files (default: `true`)
- `spark.shuffle.s3.folderPrefixes`: The number of prefixes to use when storing files on S3
  (default: `10`, minimum: `1`).

  **Note**: This option can be used to optimize performance on object stores which have a prefix rate-limit.
- `spark.shuffle.checksum.enabled`: Enables checksums on Shuffle files (default: `true`)

  **Note**: This option creates additional overhead if active. Suggested configuration: `false`.

- `spark.shuffle.s3.useSparkShuffleFetch`: Uses the Spark shuffle fetch iterator.

  **Note**: This uses `spark.storage.decommission.fallbackStorage.path` instead of `spark.shuffle.s3.rootDir`.

### Debug configuration options

Configuration options used for debugging:

- `spark.shuffle.s3.alwaysCreateIndex`: Always create an index file, even if all partitions have empty length (
  default: `false`)

  **Note**: Creates additional overhead if active.

- `spark.shuffle.s3.useBlockManager`: Use the Spark block manager to compute blocks (default: `true`).

  **Note**: Disabling this feature uses the file system listing to determine which shuffle blocks should be read.

- `spark.shuffle.s3.forceBatchFetch`: Force batch fetch for Shuffle Blocks (default: `false`)

  **Note**: Can lead to invalid results.

## Testing

The tests store the shuffle data in `/tmp/spark-s3-shuffle`. The following configuration options need to be passed
to Java > 11:

```bash
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
  --add-opens=java.base/java.io=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED 
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED 
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED 
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED 
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED 
  --add-opens=java.base/sun.security.action=ALL-UNNAMED -
  -add-opens=java.base/sun.util.calendar=ALL-UNNAMED 
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED 
```

## Usage

Copy one of the following files to your spark path:

- `spark-s3-shuffle_2.12-SPARK_VERSION_SNAPSHOT-jar-with-dependencies.jar` (created by `sbt assembly`)
- `spark-s3-shuffle_2.12-SPARK_VERSION_SNAPSHOT.jar` (created by `sbt package`)

### With S3 Plugin

Add the following lines to your Spark configuration:

- `S3A_ACCESS_KEY`: S3 access key.
- `S3A_SECRET_KEY`: S3 secret key.
- `S3A_ENDPOINT`: The S3 endpoint e.g. `http://10.40.0.29:9000`
- `SHUFFLE_ROOT`: The Shuffle root for the shuffle plugin e.g. `s3a://zrlio-tmp/s3-benchmark-shuffle`.

```
    --conf spark.hadoop.fs.s3a.access.key=S3A_ACCESS_KEY
    --conf spark.hadoop.fs.s3a.secret.key=S3A_SECRET_KEY
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false # Optional if https is not enabled.
    --conf spark.hadoop.fs.s3a.endpoint=S3A_ENDPOINT
    --conf spark.hadoop.fs.s3a.path.style.access=true
    --conf spark.hadoop.fs.s3a.fast.upload=true
    
    --conf spark.shuffle.manager="org.apache.spark.shuffle.sort.S3ShuffleManager"
    --conf spark.shuffle.sort.io.plugin.class="org.apache.spark.shuffle.S3ShuffleDataIO"
    --conf spark.hadoop.fs.s3a.impl="org.apache.hadoop.fs.s3a.S3AFileSystem"
    --conf spark.shuffle.s3.rootDir=SHUFFLE_ROOT
```

**Optional**: Manually add the AWS SDK if you want to use `spark-s3-shuffle_2.12-1.0-SNAPSHOT.jar`:

```
    --conf spark.driver.extraClassPath='/opt/spark/jars/aws-java-sdk-bundle-1.11.375.jar,/opt/spark/jars/hadoop-aws-3.2.0.jar'
    --conf spark.executor.extraClassPath='/opt/spark/jars/aws-java-sdk-bundle-1.11.375.jar,/opt/spark/jars/hadoop-aws-3.2.0.jar'
```

#### Performance Tuning

Consider adapting the following configuration variables:

- The concurrency of the S3 prefetcher can be increased by configuring `spark.shuffle.s3.maxConcurrencyTask`. This
  value controls the number of threads prefetching blocks which are due to be read.

  Default: `10`.

- Increase the block size for the S3A filesystem with `spark.hadoop.fs.s3a.block.size`. This value
  effects the number of partitions created by Spark (if the input data is located on S3 as well).
  By default the block size is 32 MiB. If the value is increased to 128 MiB Spark will
  create less partitions and thus create bigger partition blocks.

  **Recommmended:** `134217728`, default `33554432`.

- Increase the number of connections and threads of the hadoop S3A plugin. This can be controlled with the following
  variables:

    - `spark.hadoop.fs.s3a.threads.max`. **Recommended**: `20`

    - `spark.hadoop.fs.s3a.connection.maximum` **Recommended**: `20`

  More information is
  available [here](https://hadoop.apache.org/docs/r3.2.0/hadoop-aws/tools/hadoop-aws/performance.html).

- Reduce the multi-part size to increase the upload speed for smaller-objects by configuring
  `spark.hadoop.fs.s3a.multipart.size`. Reason S3A shuffle partitions are typically below 128 MiB.
  Decreasing the multi-part size will allow S3A to upload more blocks concurrently.

  **Recommended**: `33554432`, default `67108864`

  **Note**: This has an effect on the maximum file size that can be stored on S3.

### With COS/Stocator Plugin

- `COS_ACCESS_KEY`: The key to access COS.
- `COS_SECRET_KEY`: The secret key to COS.
- `COS_ENDPOINT`: The COS endpoint e.g. `https://s3.direct.us-south.cloud-object-storage.appdomain.cloud`
- `SHUFFLE_ROOT`: The root dir for shuffle `cos://zrlio-tmp/s3-benchmark-shuffle`

```
    --conf spark.hadoop.fs.s3a.fast.upload=true
    --conf spark.hadoop.fs.cos.flat.list=false
    --conf spark.hadoop.fs.stocator.scheme.list=cos
    --conf spark.hadoop.fs.stocator.cos.scheme=cos
    --conf spark.hadoop.fs.stocator.cos.impl=com.ibm.stocator.fs.cos.COSAPIClient
    --conf spark.hadoop.fs.cos.impl=com.ibm.stocator.fs.ObjectStoreFileSystem
    --conf spark.hadoop.fs.cos.resources.access.key=COS_ACCESS_KEY
    --conf spark.hadoop.fs.cos.resources.endpoint=COS_ENDPOINT
    --conf spark.hadoop.fs.cos.resources.secret.key=COS_SECRET_KEY
    --conf spark.shuffle.manager="org.apache.spark.shuffle.sort.S3ShuffleManager"
    --conf spark.shuffle.sort.io.plugin.class="org.apache.spark.shuffle.S3ShuffleDataIO"
    --conf spark.shuffle.s3.rootDir=SHUFFLE_ROOT
```
