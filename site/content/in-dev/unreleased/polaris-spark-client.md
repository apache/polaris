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
Title: Polaris Spark Client
type: docs
weight: 400
---

Apache Polaris now provides Catalog support for Generic Tables (non-iceberg tables), please check out 
the [Catalog API Spec]({{% ref "polaris-catalog-service" %}}) for Generic Table API specs.

Along with the Generic Table Catalog support, Polaris is also releasing a Spark Client, which help
providing an end-to-end solution for Apache Spark to manage Delta tables using Polaris.

Note the Polaris Spark Client is able to handle both Iceberg and Delta tables, not just Delta.

This pages documents how to build and use the Apache Polaris Spark Client before formal release.

## Prerequisite
1. Check out the polaris repo
```shell
cd ~
git clone https://github.com/apache/polaris.git
```
2. Spark with version >= 3.5.3 and <= 3.5.5, recommended with 3.5.5.
```shell
cd ~
wget https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz 
mkdir spark-3.5
tar xzvf spark-3.5.5-bin-hadoop3.tgz  -C spark-3.5 --strip-components=1
cd spark-3.5
```

All Spark Client code is available under plugins/spark of the polaris repo, and we currently only provide 
support for Spark 3.5.

## Quick Start with Local Polaris Service
If you want to quickly try out the functionality with a local Polaris service, you can follow the instructions
in plugins/spark/v3.5/getting-started/README.md.

The getting-started will start two containers:
1) The `polaris` service for running Apache Polaris using an in-memory metastore
2) The `jupyter` service for running Jupyter notebook with PySpark (Spark 3.5.5 is used)

The notebook SparkPolaris.ipynb provided under plugins/spark/v3.5/getting-started/notebooks provides examples 
of basic commands, includes:
1) Connect to Polaris using Python client to create Catalog and Roles
2) Start Spark session using the Polaris Spark Client
3) Using Spark to perform table operations for both Delta and Iceberg

## Start Spark against a deployed Polaris Service
If you want to start Spark with a deployed Polaris service, you can follow the following instructions.

Before start, Make sure the service deployed is up-to-date, and Spark 3.5 with at least version 3.5.3 is installed. 

### Build Spark Client Jars
A task createPolarisSparkJar is added to project polaris-spark to help building jars for the Polaris Spark plugin, 
the jar is named as:
`polaris-iceberg-<icebergVersion>-spark-runtime-<sparkVersion>_<scalaVersion>-<polarisVersion>.jar`. 
For example:
`polaris-iceberg-1.8.1-spark-runtime-3.5_2.12-0.10.0-beta-incubating-SNAPSHOT.jar`.

Run the following to build a Spark Client jar that is compatible with Spark 3.5 and Scala 3.12.
```shell
cd ~/polaris
./gradlew :polaris-spark-3.5_2.12:createPolarisSparkJar
```
If you want to build a Scala 2.13 compatible jar, you can use the following command:
- `./gradlew :polaris-spark-3.5_2.13:createPolarisSparkJar` -- build jar for Spark 3.5 with Scala version 2.13.

The result jar is located at plugins/spark/v3.5/build/<scala_version>/libs after the build. You can also copy the
corresponding jar to any location your Spark will have access.

### Connecting with Spark Using the built jar
The following CLI command can be used to start the spark with connection to the deployed Polaris service using
the Polaris Spark Client.

```shell
bin/spark-shell \
--jars <path-to-spark-client-jar> \
--packages org.apache.hadoop:hadoop-aws:3.4.0,io.delta:delta-spark_2.12:3.3.1 \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
--conf spark.sql.catalog.<spark-catalog-name>.warehouse=<polaris-catalog-name> \
--conf spark.sql.catalog.<spark-catalog-name>.header.X-Iceberg-Access-Delegation=vended-credentials \
--conf spark.sql.catalog.<spark-catalog-name>=org.apache.polaris.spark.SparkCatalog \
--conf spark.sql.catalog.<spark-catalog-name>.uri=<polaris-service-uri> \
--conf spark.sql.catalog.<spark-catalog-name>.credential='<client-id>:<client-secret>' \
--conf spark.sql.catalog.<spark-catalog-name>.scope='PRINCIPAL_ROLE:ALL' \
--conf spark.sql.catalog.<spark-catalog-name>.token-refresh-enabled=true
```

Replace `path-to-spark-client-jar` to where the built jar is located. The `spark-catalog-name` is the catalog name you
wil use with spark, and `polaris-catalog-name` is the catalog name used by Polaris service, for simplicity, you can use
the same name. Replace the `polaris-service-uri`, `client-id` and `client-secret` accordingly, you can refer to
[Quick Start]({{% ref "../0.9.0/quickstart" %}}) for more details about those fields.

Or you can star