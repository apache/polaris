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

# Getting Started with Apache Polaris and Apache Ozone

## Overview

This example uses [Apache Ozone](https://ozone.apache.org/) as a storage provider with Polaris.

Spark is used as a query engine. This example assumes a local Spark installation.
See the [Spark Notebooks Example](../spark/README.md) for a more advanced Spark setup.

## Starting the Example

Start the docker compose group by running the following command from the root of the repository:

```shell
docker compose -f getting-started/minio/docker-compose.yml up
```

Note: this example pulls the `apache/polaris:latest` image, but assumes the image is `1.2.0-incubating` or later. 

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
    --conf spark.sql.catalog.polaris.credential=root:s3cr3t \
    --conf spark.sql.catalog.polaris.client.region=irrelevant
```

Note: `s3cr3t` is defined as the password for the `root` user in the `docker-compose.yml` file.

Note: The `client.region` configuration is required for the AWS S3 client to work, but it is not used in
this example since Ozone does not require a specific region.

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

## Lack of Credential Vending

Notice that the Spark configuration does not contain a `X-Iceberg-Access-Delegation` header.
This is because Ozone does not support the STS API and consequently cannot produce session
credentials to be vended to Polaris clients.

The lack of STS API is represented in the Catalog storage configuration by the 
`stsUnavailable=false` property.

## S3 Credentials

In this example Ozone does not require credentials for accessing its S3 API. Therefore, neither 
Polaris, not Spark use any S3 access keys.

If Ozone were configured to require credentials, Spark and Polaris would have to their own separate
S3 access key / secret properties because credential vending is not possible with Ozone 2.0.0.

## S3 Endpoints

Note that the catalog configuration defined in the `docker-compose.yml` contains
different endpoints for the Polaris Server and the client (Spark). Specifically,
the client endpoint is `http://localhost:9878`, but `endpointInternal` is `http://ozone-s3g:9878`.

This is necessary because clients running on `localhost` do not normally see service
names (such as `ozone-s3g`) that are internal to the docker compose environment.
