# Shuffle Plugin for Apache Spark and S3 compatible storage services

This plugin allows storing [Apache Spark](https://spark.apache.org/) shuffle data on S3 compatible object storage (e.g. S3A, COS).
It uses the Java Hadoop-Filesystem abstraction for interoperability for COS, S3A and even local file systems.

*Note*: This plugin is based on [Apache Spark Pull Request #34864](https://github.com/apache/spark/pull/34864/files). It has
since been significantly rewritten.

## Known issues

Some terasort implementations reuse the same byte buffer for each record. This produces invalid results with the
sort shuffle implementation for S3. The configuration option `spark.shuffle.s3.sort.cloneRecords` mitigates this 
issue by cloning the types `Array[T]`.

## Building

```bash
sbt package  # Creates a minimal jar.
sbt assembly # Creates the full assembly with all dependencies, notably hadoop cloud.
 ```

## Required configuration

These configuration values need to be passed to Spark to load and configure the plugin:
- `spark.shuffle.manager`: The shuffle manager. Needs to be set to `org.apache.spark.shuffle.S3ShuffleManager`.
- `spark.shuffle.sort.io.plugin.class`: The sort io plugin class. Needs to be set to 
  `org.apache.spark.shuffle.S3ShuffleDataIO`.
- `spark.shuffle.s3.rootDir`: Root dir for the shuffle files. Examples:
    - `s3a://zrlio-tmp/s3-benchmark-shuffle` (Hadoop-AWS + AWS-SDK)
    - `cos://zrlio-tmp.resources/s3-benchmark-shuffle` (Hadoop-Cloud + Stocator)

  The plugin will create an additional path based on the start time and the Spark app id.

### Debug options / optimizations

These are optional configuration values that control how s3-shuffle behaves.
- `spark.shuffle.s3.forceBypassMergeSort`: Bypass SortShuffle (default: `false`)
- `spark.shuffle.s3.allowSerializedShuffle`: Allow serialized shuffle (default `true`)
- `spark.shuffle.s3.sort.cloneRecords`: Clone records before sorting (workaround for terasort, default: `false`)
- `spark.shuffle.s3.cleanup`: Cleanup the shuffle files (default: `true`)
- `spark.shuffle.s3.alwaysCreateIndex`: Always create an index file, even if all partitions have empty length (
  default: `false`)
- `spark.shuffle.s3.useBlockManager`: Use the Spark block manager to compute blocks (default: `true`). Note: Disabling
  this feature might lead to invalid results for some workloads.
- `spark.shuffle.s3.forceBatchFetch`: Force batch fetch for Shuffle Blocks (default: `false`)
- `spark.shuffle.s3.supportsUnbuffer`: Streams can be unbuffered instead of closed (default: `true`,
  if Storage-backend is S3A, `false` otherwise).

## Testing

The tests require the following environment variables to be set:
- `AWS_ACCESS_KEY_ID`: Access key to use for the S3 Shuffle service.
- `AWS_SECRET_ACCESS_KEY`: Secret key to use for the S3 Shuffle service.
- `S3_ENDPOINT_URL`: Endpoint URL of the S3 Service (e.g. `http://10.40.0.29:9000` or
  `https://s3.direct.us-south.cloud-object-storage.appdomain.cloud`).
- `S3_ENDPOINT_USE_SSL`: Whether the endpoint supports SSL or not.
- `S3_SHUFFLE_ROOT`: The shuffle root (e.g `s3a://zrlio-tmp/S3ShuffleManagerTests`)

## Usage

Copy one of the following files to your spark path:

- `spark-s3-shuffle_2.12-1.0-SNAPSHOT-jar-with-dependencies.jar` (created by `sbt assembly`)
- `spark-s3-shuffle_2.12-1.0-SNAPSHOT.jar` (created by `sbt package`)

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
    
    --conf spark.shuffle.manager="org.apache.spark.shuffle.S3ShuffleManager"
    --conf spark.shuffle.sort.io.plugin.class="org.apache.spark.shuffle.S3ShuffleDataIO"
    --conf spark.hadoop.fs.s3a.impl="org.apache.hadoop.fs.s3a.S3AFileSystem"
    --conf spark.shuffle.s3.rootDir=SHUFFLE_ROOT
```

**Optional**: Manually add the AWS SDK if you want to use `spark-s3-shuffle_2.12-1.0-SNAPSHOT.jar`:

```
    --conf spark.driver.extraClassPath='/opt/spark/jars/aws-java-sdk-bundle-1.11.375.jar,/opt/spark/jars/hadoop-aws-3.2.0.jar'
    --conf spark.executor.extraClassPath='/opt/spark/jars/aws-java-sdk-bundle-1.11.375.jar,/opt/spark/jars/hadoop-aws-3.2.0.jar'
```

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
    --conf spark.shuffle.manager="org.apache.spark.shuffle.S3ShuffleManager"
    --conf spark.shuffle.sort.io.plugin.class="org.apache.spark.shuffle.S3ShuffleDataIO"
    --conf spark.shuffle.s3.rootDir=SHUFFLE_ROOT
```

## Running the tests in 