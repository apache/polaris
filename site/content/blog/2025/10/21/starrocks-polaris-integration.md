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
title: "StarRocks and Apache Polaris Integration: Building a Unified, High-Performance Data Lakehouse"
date: 2025-10-21
author: Wayne Yang
---
## Introduction: Why StarRocks + Apache Polaris?

### Modern Data-Architecture Pain Points: Silos & Engine Lock-in
Today’s data-driven enterprises face two chronic ailments:

* **Data silos** : transactional data sits in RDBMS, click-streams in an S3 data lake, CRM data in a SaaS vault, etc. Cross-domain analysis requires expensive ETL and still arrives stale.

* **Engine lock-in** : every OLAP engine optimises for its own metadata layer and file layout. Migrating to a faster or cheaper engine means re-formatting, re-writing and re-governing years of data.

An open, interoperable architecture is no longer a luxury, it is a survival requirement.

### StarRocks at a Glance: Speed & Simplicity
[StarRocks](https://www.starrocks.io/) is a modern MPP database designed for high-performance analytics and real-time data processing. Key features include:

* MPP, fully-vectorised execution, CBO that thrives on complex multi-table joins.

* Sub-second response on TB-scale data without pre-aggregation.

* Compute-storage separation since v3.0: scale stateless compute pods in seconds, keep data in cheap object storage.

### Apache Polaris (Incubating): Vendor-Neutral Iceberg Catalog
* 100% open-source implementation of the Iceberg REST Catalog API.

* Pluggable metadata backend (PostgreSQL, in-mem) and multi-cloud storage support.

* Fine-grained RBAC + credential-vending (temporary STS tokens) for secure, governed sharing across engines.

### Business Value of the Pairing
**StarRocks** delivers performance; **Polaris** delivers openness. Together they let you:
* Keep **ONE** copy of data in Iceberg on S3, queryable by Spark, Flink, Trino, and StarRocks concurrently.

* Maintain **ONE** set of role-based permissions in Polaris that apply to every engine.

* Deliver BI dashboards, ad-hoc exploration and lightweight ETL operations on the same fresh dataset, zero data movement, zero lock-in.

## Architecture
![](/img/blog/2025/10/21/fig1-polaris-starrocks-architecture.png)

**Polaris** acts as the single source of truth for:

* Table schema, partitions, snapshots

* Role-based access control (RBAC)

* Short-lived, scoped cloud credentials (credential vending)

**StarRocks** acts as a stateless compute layer:

* Discover Iceberg metadata via REST calls

* Directly reads Parquet/ORC files from cloud storage using the vendored credentials

* Applies its own CBO and vectorised execution for query acceleration

## Deploy and Configure Polaris

User can refer to [Polaris Quickstart](https://polaris.apache.org/releases/1.1.0/getting-started/) to deploy Polaris,
Here we will compile from source code and deploy Polaris via a standalone process.

### Clone Source Code and Start Polaris

1. Clone source code and checkout to released version

User can get the latest released version from https://github.com/apache/polaris/releases
```bash
# download the latest released version 1.1.0-incubating
wget https://dlcdn.apache.org/incubator/polaris/1.1.0-incubating/apache-polaris-1.1.0-incubating.tar.gz
tar -xvzf apache-polaris-1.1.0-incubating.tar.gz
cd apache-polaris-1.1.0-incubating
```

2. Build Polaris
```bash
./gradlew \
  :polaris-server:assemble \
  :polaris-server:quarkusAppPartsBuild --rerun \
  :polaris-admin:assemble \
  :polaris-admin:quarkusAppPartsBuild --rerun
```

3. Run Polaris

Ensure you have Java 21+, and export aws access key and secret key first.
```bash
export AWS_ACCESS_KEY_ID=<access_key>
export AWS_SECRET_ACCESS_KEY=<secret_key>

./gradlew run
```

When Polaris is run using the./gradlew run command, the root principal credentials are **root** and **s3cr3t** for the **CLIENT_ID** and **CLIENT_SECRET**, respectively.

When using a Gradle-launched Polaris instance, it’ll launch an instance of Polaris that stores entities only **in-memory**. This means that any entities that you define will be destroyed when Polaris is shut down.

We suggest that users refer to this section (https://polaris.apache.org/releases/1.1.0/metastores/) to configure the metastore to persist Polaris entities.

```bash
export POLARIS_PERSISTENCE_TYPE=relational-jdbc
export QUARKUS_DATASOURCE_USERNAME=<your-username>
export QUARKUS_DATASOURCE_PASSWORD=<your-password>
export QUARKUS_DATASOURCE_JDBC_URL=<jdbc-url-of-postgres>

export AWS_ACCESS_KEY_ID=<access_key>
export AWS_SECRET_ACCESS_KEY=<secret_key>

./gradlew run
```

Using **Admin Tool** (https://polaris.apache.org/releases/1.1.0/admin-tool/) to bootstrap realms and create the necessary principal credentials for the Polaris server.

For example, to bootstrap the POLARIS realm and create its root principal credential with the client ID **root** and client secret **root_secret**, you can run the following command:

```bash
java -jar runtime/admin/build/quarkus-app/quarkus-run.jar bootstrap -r POLARIS -c POLARIS,root,root_secret
```

### Creating a Principal and Assigning it Privileges

Use the **Polaris CLI** (already built in the same folder):

1. Export CLIENT_ID and CLIENT_SECRET
```bash
export CLIENT_ID=root
export CLIENT_SECRET=root_secret
```

2. Create catalog
```bash
./polaris 
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  catalogs create \
  --storage-type s3 \
  --default-base-location ${DEFAULT_BASE_LOCATION} \
  --role-arn ${ROLE_ARN} \
  polaris_catalog
```

The DEFAULT_BASE_LOCATION you provide will be the default location that objects in this catalog should be stored in, and the ROLE_ARN you provide should be a Role ARN with access to read and write data in that location.
These credentials will be provided to engines reading data from the catalog once they have authenticated with Polaris using credentials that have access to those resources.

3. Creating a Principal and Assigning it Privileges

Use below commands to create principal, principal role and catalog role
```bash
./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  principals \
  create \
  jack

./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  principal-roles \
  create \
  test_user_role

./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  catalog-roles \
  create \
  --catalog polaris_catalog \
  test_catalog_role
```

When the **principals create** commands successfully, it will return the credentials for this new principal，save it.

Use below command to grant privileges

```bash
./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  principal-roles \
  grant \
  --principal jack \
  test_user_role

./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  catalog-roles \
  grant \
  --catalog polaris_catalog \
  --principal-role test_user_role \
  test_catalog_role
  
  ./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  privileges \
  catalog \
  grant \
  --catalog polaris_catalog \
  --catalog-role test_catalog_role \
  CATALOG_MANAGE_CONTENT
```

We grant **CATALOG_MANAGE_CONTENT** privilege to the catalog role `test_catalog_role`, and assign the principal role `test_user_role` to principal `jack`, then assign the catalog role `test_catalog_role` to principal role `test_user_role`.

## Configure StarRocks Iceberg Catalog

First, you need to have a StarRocks cluster up and running. Please refer to the [StarRocks Quick Start Guide](https://docs.starrocks.io/docs/quick_start) for instructions on setting up a StarRocks cluster.
Then you can create an external Iceberg catalog in StarRocks that connects to Polaris.

### Create External Catalog
1. Use credentials vending

It's recommended to use Polaris's credential vending feature to enhance security by avoiding long-lived static credentials.

Here is an example of creating an external catalog in StarRocks that connects to Polaris using credential vending:

```sql
CREATE EXTERNAL CATALOG polaris_catalog
PROPERTIES (   
    "iceberg.catalog.uri"  = "http://<POLARIS_HOST>:<POLARIS_PORT>/api/catalog",   
    "type"  =  "iceberg",   
    "iceberg.catalog.type"  =  "rest",   
    "iceberg.catalog.warehouse" = "polaris_catalog",
    "iceberg.catalog.security" = "oauth2",
    "iceberg.catalog.oauth2.credential" = "<jack_client_id>:<jack_client_secret>",
    "iceberg.catalog.oauth2.scope"='PRINCIPAL_ROLE:ALL',
    "aws.s3.region" = "us-west-2",
    "iceberg.catalog.vended-credentials-enabled" = "true"
 );
```
We use jack's credential(client_id and client_secret) created above to access polaris_catalog.

Users will have the permissions of user jack when accessing Iceberg tables in polaris_catalog.

2. Use S3 storage credentials

If you prefer to use static S3 credentials instead of credential vending, you can create the external catalog in StarRocks as follows:
```sql
CREATE EXTERNAL CATALOG polaris_catalog
PROPERTIES (   
    "iceberg.catalog.uri"  = "http://<POLARIS_HOST>:<POLARIS_PORT>/api/catalog",   
    "type"  =  "iceberg",   
    "iceberg.catalog.type"  =  "rest",   
    "iceberg.catalog.warehouse" = "polaris_catalog",
    "iceberg.catalog.security" = "oauth2",
    "iceberg.catalog.oauth2.credential" = "<jack_client_id>:<jack_client_secret>",
    "iceberg.catalog.oauth2.scope"='PRINCIPAL_ROLE:ALL',
    "aws.s3.region" = "us-west-2",
    "aws.s3.access_key" = "<access_key>", 
    "aws.s3.secret_key" = "<secret_key>",
    "iceberg.catalog.vended-credentials-enabled" = "false"
 );
```

### Manage iceberg tables through StarRocks
Connect to StarRocks and run the following commands to create and query an Iceberg table through the Polaris catalog:

```sql
-- switch to external iceberg catalog 
StarRocks>set catalog polaris_catalog;
Query OK, 0 rows affected (0.00 sec)

-- create database
StarRocks>create database polaris_db;
Query OK, 0 rows affected (0.06 sec)

StarRocks>use polaris_db;
Database changed

-- create iceberg table taxis 
StarRocks>CREATE TABLE taxis
    -> (
    ->   trip_id bigint,
    ->   trip_distance float,
    ->   fare_amount double,
    ->   store_and_fwd_flag string,
    ->   vendor_id bigint
    -> )
    -> PARTITION BY (vendor_id);
Query OK, 0 rows affected

-- insert data
StarRocks>INSERT INTO taxis
    -> VALUES (1000371, 1.8, 15.32, 'N', 1), (1000372, 2.5, 22.15, 'N', 2), (1000373, 0.9, 9.01, 'N', 2), (1000374, 8.4, 42.13, 'Y', 1);
Query OK, 4 rows affected

-- query iceberg table
StarRocks>select * from taxis;
+---------+---------------+-------------+--------------------+-----------+
| trip_id | trip_distance | fare_amount | store_and_fwd_flag | vendor_id |
+---------+---------------+-------------+--------------------+-----------+
| 1000372 |           2.5 |       22.15 | N                  |         2 |
| 1000373 |           0.9 |        9.01 | N                  |         2 |
| 1000371 |           1.8 |       15.32 | N                  |         1 |
| 1000374 |           8.4 |       42.13 | Y                  |         1 |
+---------+---------------+-------------+--------------------+-----------+
4 rows in set

```

For more information about using Iceberg table with StarRocks, please refer to:

https://docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_catalog