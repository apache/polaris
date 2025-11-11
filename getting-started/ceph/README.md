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

# Getting Started with Apache Polaris and Ceph

## Overview

This guide describes how to spin up a **single-node Ceph cluster** with **RADOS Gateway (RGW)** for S3-compatible storage and configure it for use by **Polaris**.

This example cluster is configured for basic access key authentication only.
It does not include STS (Security Token Service) or temporary credentials.
All access to the Ceph RGW (RADOS Gateway) and Polaris integration uses static S3-style credentials (as configured via radosgw-admin user create).

Spark is used as a query engine. This example assumes a local Spark installation.
See the [Spark Notebooks Example](../spark/README.md) for a more advanced Spark setup.

## Starting the Example

Before starting the Ceph + Polaris stack, you’ll need to configure environment variables that define network settings, credentials, and cluster IDs.

The services are started **in sequence**:
1. Monitor + Manager
2. OSD
3. RGW
4. Polaris

Note: this example pulls the `apache/polaris:latest` image, but assumes the image is `1.2.0-incubating` or later. 

### 1. Copy the example environment file
```shell
cp .env.example .env
```

### 2. Prepare Network
```shell
# Optional: force runtime (docker or podman)
export RUNTIME=docker

./getting-started/ceph/prepare-network.sh
```

### 3. Start monitor and manager
```shell
docker compose up -d mon1 mgr
```

### 4. Start OSD
```shell
docker compose up -d osd1
```

### 5. Start RGW
```shell
docker compose up -d rgw1
```
#### Check status
```shell
docker exec --interactive --tty ceph-mon1-1 ceph -s
```
You should see something like:
```yaml
cluster:
  id:     b2f59c4b-5f14-4f8c-a9b7-3b7998c76a0e
  health: HEALTH_WARN
          mon is allowing insecure global_id reclaim
          1 monitors have not enabled msgr2
          6 pool(s) have no replicas configured

services:
  mon: 1 daemons, quorum mon1 (age 49m)
  mgr: mgr(active, since 94m)
  osd: 1 osds: 1 up (since 36m), 1 in (since 93m)
  rgw: 1 daemon active (1 hosts, 1 zones)
```

### 6. Create bucket for Polaris storage
```shell
docker compose up -d setup_bucket
```

### 7. Run Polaris service
```shell
docker compose up -d polaris
```

### 8. Setup polaris catalog
```shell
docker compose up -d polaris-setup
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
    --conf spark.sql.catalog.polaris.credential=root:s3cr3t \
    --conf spark.sql.catalog.polaris.client.region=irrelevant
```

Note: `s3cr3t` is defined as the password for the `root` user in the `docker-compose.yml` file.

Note: The `client.region` configuration is required for the AWS S3 client to work, but it is not used in this example
since Ceph does not require a specific region.

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
This is because example cluster does not include STS (Security Token Service) or temporary credentials. 

The lack of STS API is represented in the Catalog storage configuration by the 
`stsUnavailable=true` property.
