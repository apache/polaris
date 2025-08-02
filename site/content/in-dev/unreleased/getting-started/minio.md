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
Title: Using MinIO
type: docs
weight: 500
---

In this guide we walk through setting up a simple Polaris Server with local [MinIO](https://www.min.io/) storage.

Similar configurations are expected to work with other S3-compatible systems that also have the
[STS](https://docs.aws.amazon.com/STS/latest/APIReference/welcome.html) API.

# Setup

Start MinIO. Here's a `docker` example.

```shell
docker run -p 9100:9000 -p 9101:9001 --name minio \
 -e "MINIO_ROOT_USER=miniouser" -e "MINIO_ROOT_PASSWORD=miniopwd" \
 quay.io/minio/minio:latest server /data --console-address ":9001"
```

Create a bucket called "test" in MinIO UI or CLI.

Edit `~/.aws/credentials` and create a `minio` profile.

```
[minio]
aws_access_key_id = miniouser
aws_secret_access_key = miniopwd
region=us-west-2
```

In the shell where the Polaris Server will be started `export AWS_PROFILE=minio`.

Build and run Polaris Server.

```shell
./gradlew assemble

env POLARIS_BOOTSTRAP_CREDENTIALS=POLARIS,root,s3cr3t \ 
 java -jar runtime/server/build/quarkus-app/quarkus-run.jar
```

Note: Apache Polaris 1.0.0-incubating does not yet have all the code required for proper operation with MinIO.
The next release is expected to provide full support for S3-compatible storage with STS.

Create a catalog named "polaris".

```shell
./polaris --client-id root --client-secret s3cr3t \
 catalogs create polaris --storage-type S3 \
 --default-base-location 's3://test' \
 --role-arn arn:aws:iam::123456789012:role/dummy \
 --region us-west-2 \
 --endpoint http://127.0.0.1:9100 \
 --path-style-access
```

Note: the role and region parameters need to be set to avoid runtime errors in Polaris,
but they will be ignored by MinIO. 

For the sake of simplicity, grant `CATALOG_MANAGE_CONTENT` directly to the `catalog_admin` role
using `polaris` CLI.

```shell
./polaris --client-id root --client-secret s3cr3t \
 privileges catalog grant --catalog polaris \
 --catalog-role catalog_admin CATALOG_MANAGE_CONTENT
```

# Connecting from Spark

Start Spark.

```shell
bin/spark-sql \
 --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0,org.apache.iceberg:iceberg-aws-bundle:1.9.0 \
 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
 --conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog \
 --conf spark.sql.catalog.polaris.type=rest \
 --conf spark.sql.catalog.polaris.uri=http://localhost:8181/api/catalog \
 --conf spark.sql.catalog.polaris.token-refresh-enabled=false \
 --conf spark.sql.catalog.polaris.warehouse=polaris \
 --conf spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL \
 --conf spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation=vended-credentials \
 --conf spark.sql.catalog.polaris.credential=root:s3cr3t
```

Create a table in Spark.

```sql
use polaris;
create namespace ns;
create table ns.t1 as select 'abc';
select * from ns.t1;
```
