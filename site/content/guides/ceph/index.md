---
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
linkTitle: "Storage: Ceph"
title: "Getting Started with Apache Polaris and Ceph"
weight: 200
tags:
  - ceph
  - object storage
cascade:
    type: guides
menus:
    main:
        parent: Guides
        weight: 200
---

## Overview

This guide describes how to spin up a **single-node Ceph cluster** with **RADOS Gateway (RGW)** for S3-compatible storage and configure it for use by **Polaris**.

This example cluster is configured for basic access key authentication only.
It does not include STS (Security Token Service) or temporary credentials.
All access to the Ceph RGW (RADOS Gateway) and Polaris integration uses static S3-style credentials (as configured via radosgw-admin user create).

Spark is used as a query engine. This example assumes a local Spark installation.
See the [Spark Notebooks Example](../spark/) for a more advanced Spark setup.

## Starting the Example

Before starting the Ceph + Polaris stack, you’ll need to configure environment variables that define network settings, credentials, and cluster IDs.

The services are started **in sequence**:
1. Monitor + Manager
2. OSD
3. RGW
4. Polaris

Note: this example pulls the `apache/polaris:latest` image, but assumes the image is `1.2.0-incubating` or later. 

<!-- Guide testing: go to the guide's directory, instead of the workspace directory
```shell
cd "$SITE_TEST_GUIDE_DIR"
```
-->

### 1. Copy the example environment file
```shell
cp dot-env.example .env
```

### 2. Start the docker compose group by running the following command:
```shell
docker compose up -d
```

#### Check status
```shell
docker exec ceph-mon1-1 ceph -s
```
You should see something like:
```
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

### 3. Connecting From Spark

```shell
bin/spark-sql \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1,org.apache.iceberg:iceberg-aws-bundle:1.10.1 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.polaris.type=rest \
    --conf spark.sql.catalog.polaris.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.polaris.uri=http://localhost:8181/api/catalog \
    --conf spark.sql.catalog.polaris.token-refresh-enabled=true \
    --conf spark.sql.catalog.polaris.warehouse=quickstart_catalog \
    --conf spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL \
    --conf spark.sql.catalog.polaris.credential=root:s3cr3t \
    --conf spark.sql.catalog.polaris.client.region=irrelevant \
    --conf spark.sql.catalog.polaris.s3.access-key-id=POLARIS123ACCESS \
    --conf spark.sql.catalog.polaris.s3.secret-access-key=POLARIS456SECRET
```

Note: `s3cr3t` is defined as the password for the `root` user in the `docker-compose.yml` file.

Note: The `client.region` configuration is required for the AWS S3 client to work, but it is not used in this example
since Ceph does not require a specific region.

### 4. Running Queries

Run inside the Spark SQL shell:

```sql
USE polaris;

CREATE NAMESPACE ns;

CREATE TABLE ns.t1 AS SELECT 'abc';

SELECT * FROM ns.t1;
-- abc
```

## Lack of Credential Vending

Notice that the Spark configuration does not contain a `X-Iceberg-Access-Delegation` header.
This is because example cluster does not include STS (Security Token Service) or temporary credentials. 

The lack of STS API is represented in the Catalog storage configuration by the 
`stsUnavailable=true` property.
