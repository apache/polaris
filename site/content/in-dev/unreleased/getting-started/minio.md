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
Title: Deploying Polaris on MinIO
type: docs
weight: 350
---

In this guide we walk through setting up a simple Polaris Server with local [MinIO](https://www.min.io/) storage.

Similar configurations are expected to work with other S3-compatible systems that also have the
[STS](https://docs.aws.amazon.com/STS/latest/APIReference/welcome.html) API.

# Setup

Clone the Polaris source repository, then build a docker image for Polaris.

```shell
./gradlew :polaris-server:assemble -Dquarkus.container-image.build=true
```

Start MinIO with Polaris using the `docker compose` example.

```shell
docker compose -f getting-started/minio/docker-compose.yml up
```

The compose script will start MinIO on default ports (API on 9000, UI on 9001)
plus a Polaris Server pre-configured to that MinIO instance. 

In this example the `root` principal has its password set to `s3cr3t`.

# Connecting from Spark

Start Spark.

```shell
export AWS_REGION=us-west-2

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

Note: `AWS_REGION` is required by the AWS SDK used by Spark, but the value is irrelevant in this case.

Create a table in Spark.

```sql
use polaris;
create namespace ns;
create table ns.t1 as select 'abc';
select * from ns.t1;
```

# Connecting from MinIO client

```shell
mc alias set pol http://localhost:9000 minio_root m1n1opwd
mc ls pol/bucket123/ns/t1
[2025-08-13 18:52:38 EDT]     0B data/
[2025-08-13 18:52:38 EDT]     0B metadata/
```

Note: the values of `minio_root`, `m1n1opwd` and `bucket123` are defined in the docker compose file. 

# Notes on Storage Configuation

In this example the Polaris Catalog is defined as (excluding uninteresting properties):

```json
    {
      "name": "quickstart_catalog",
      "storageConfigInfo": {
        "endpoint": "http://localhost:9000",
        "endpointInternal": "http://minio:9000",
        "pathStyleAccess": true,
        "storageType": "S3",
        "allowedLocations": [
          "s3://bucket123"
        ]
      }
    }
```

Note that the `roleArn` parameter, which is required for AWS storage, does not need to be set for MinIO.

Note the two endpoint values. `endpointInternal` is used by the Polaris Server, while `endpoint` is communicated
to clients (such as Spark) in Iceberg REST API responses. This distinction allows the system to work smoothly
when the clients and the server have different views of the network (in this example the host name `minio` is
resolvable only inside the docker compose environment). 