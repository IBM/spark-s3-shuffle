# Readme

This folder contains a set of examples for the HDFS connector.

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
