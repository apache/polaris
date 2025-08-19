<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
 
   http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Getting Started with Apache Polaris and MinIO

## Overview

This example uses MinIO as a storage provider with Polaris.

Spark is used as a query engine. This example assumes a local Spark installation.
See the [Spark Notebooks Example](../spark/README.md) for a more advanced Spark setup.

## Starting the Example

1. Build the Polaris server image if it's not already present locally:

    ```shell
    ./gradlew \
       :polaris-server:assemble \
       :polaris-server:quarkusAppPartsBuild --rerun \
       -Dquarkus.container-image.build=true
    ```

2. Start the docker compose group by running the following command from the root of the repository:

    ```shell
    docker compose -f getting-started/minio/docker-compose.yml up
    ```

## Connecting From Spark

```shell
bin/spark-sql \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0,org.apache.iceberg:iceberg-aws-bundle:1.9.0 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.polaris.type=rest \
    --conf spark.sql.catalog.polaris.uri=http://localhost:8181/api/catalog \
    --conf spark.sql.catalog.polaris.token-refresh-enabled=false \
    --conf spark.sql.catalog.polaris.warehouse=quickstart_catalog \
    --conf spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL \
    --conf spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation=vended-credentials \
    --conf spark.sql.catalog.polaris.credential=root:s3cr3t
```

Note: `s3cr3t` is defined as the password for the `root` users in the `docker-compose.yml` file.

## Running Queries

Run inside the Spark SQL shell:

```
spark-sql (default)> use polaris;
Time taken: 0.837 seconds

spark-sql ()> create namespace ns;
Time taken: 0.374 seconds

spark-sql ()> create table ns.t1 as select 'abc';
Time taken: 2.192 seconds

spark-sql ()> select * from ns.t1;
abc
Time taken: 0.579 seconds, Fetched 1 row(s)
```

## MinIO Endpoints

Note that the catalog configuration defined in the `docker-compose.yml` contains
different endpoints for the Polaris Server and the client (Spark). Specifically,
the client endpoint is `http://localhost:9000`, but `endpointInternal` is `http://minio:9000`.

This is necessary because clients running on `localhost` do not normally see service
names (such as `minio`) that are internal to the docker compose environment.
