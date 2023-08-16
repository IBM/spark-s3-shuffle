# Readme

This folder contains a set of examples for the spark-s3-shuffle plugin.

## Prequisites

1. A Apache Spark installation
2. A Kubernetes Cluster
3. A S3 Bucket / or a local S3 installation (see `config.sh` for the configuration). 
4. Configure `s3cmd` with the S3 credentials to enable cleanup.

## Steps

Modify `config_X.X.X.sh` based on your setup.

Run the following commands to build the docker containers:

```bash
source config_X.X.X.sh
./terasort/build.sh
./sql/build.sh
```

## Running

Use the following environment variables to configure the Spark:
- `USE_S3_SHUFFLE=1` Enable Shuffle on S3 (default: on)
- `USE_S3_SHUFFLE=0` Disable Shuffle on S3 (default: on)

### TeraSort

```bash
export SIZE=1g           # Options: 1g (default), 10g, 100g
export USE_S3_SHUFFLE=1  # Options: 0, 1 (default)
./terasort/run.sh
```

### SQL

Run a single query ([full list](https://github.com/zrlio/sql-benchmarks/blob/master/src/main/scala/com/ibm/crail/benchmarks/tests/tpcds/TPCDSQueries.scala) - omit `.sql`).
```bash
export SIZE=1000        # Options: 10, 100, 1000 (default)
export USE_S3_SHUFFLE=1 # Options: 0, 1 (default)
./sql/run_single_query.sh q67
```

Run tpcds:
```bash
export SIZE=100         # Options: 10, 100 (default), 1000
export USE_S3_SHUFFLE=1 # Options: 0, 1 (default) (enable shuffle on S3)
./sql/run_tpcds.sh
```

## Profiling

Deploy influxdb and adapt configuration in `profiler_config.yml`. A sample configuration can be
found in `influxdb_kubernetes.yml`.

To enable profiling set the environment variable `USE_PROFILER` to `1`:
```
export USE_PROFILER=1
```
and run the examples above.

See [this InfoQ article](https://www.infoq.com/articles/spark-application-monitoring-influxdb-grafana/) 
how influxdb and the JVM-Profiler interact with Grafana.

### Grafana Queries Sample Queries

Assumption: Configure a `sampleinterval` interval variable in the Grafana page.

Show Max(CPU) usage by process:

```
SELECT max("processCpuLoad") FROM "CpuAndMemory" WHERE "role" = 'executor' AND $timeFilter  GROUP BY time($sampleinterval), processUuid fill(none)```
```

Executor heap memory consumption (sum): 
```
SELECT SUM("heapMemoryTotalUsed") as Used, SUM("heapMemoryCommitted") as Committed from "autogen"."CpuAndMemory" where "role" = 'executor' AND $timeFilter GROUP BY time($sampleinterval)
```

Per-executor heap memory consumption (max):
```
select MAX("heapMemoryTotalUsed") as Used, Max("heapMemoryCommitted") as Committed from "autogen"."CpuAndMemory" where "role" = 'executor' AND $timeFilter GROUP BY time($sampleinterval), processUuid fill(none)```


Measure the S3ShuffleReader threads:
```
SELECT time, COUNT(host) AS S3ShuffleReader FROM Stacktrace WHERE "role" = 'executor' AND "stacktrace" =~ /S3ShuffleReader/ AND $timeFilter GROUP BY time($sampleinterval) fill(none)
```

Measure the S3ShuffleWriter threads:
```
SELECT time, COUNT(processUuid) AS S3ShuffleWriter FROM Stacktrace WHERE "role" = 'executor' AND "stacktrace" =~ /S3ShuffleWriter/ AND $timeFilter GROUP BY time($sampleinterval) fill(none)
```

Measure the ExternalSorter threads:
```
SELECT time, COUNT(processUuid) AS ExternalSorter FROM Stacktrace WHERE "role" = 'executor' AND "stacktrace" =~ /ShuffleExternalSorter/ AND $timeFilter GROUP BY time($sampleinterval) 
```
