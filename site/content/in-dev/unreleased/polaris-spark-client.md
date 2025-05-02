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
weight: 650
---

Apache Polaris now provides Catalog support for Generic Tables (non-Iceberg tables), please check out 
the [Catalog API Spec]({{% ref "polaris-catalog-service" %}}) for Generic Table API specs.

Along with the Generic Table Catalog support, Polaris is also releasing a Spark client, which helps to 
provide an end-to-end solution for Apache Spark to manage Delta tables using Polaris.

Note the Polaris Spark client is able to handle both Iceberg and Delta tables, not just Delta.

This page documents how to connect Spark with Polaris Service using the Polaris Spark client.

## Prerequisite
Check out the Polaris repo:
```shell
cd ~
git clone https://github.com/apache/polaris.git
```

All Spark client code is available under `plugins/spark` of the Polaris repo.

## Quick Start with Local Polaris service
If you want to quickly try out the functionality with a local Polaris service, you can follow the instructions
in `plugins/spark/v3.5/getting-started/README.md`.

The getting-started will start two containers:
1) The `polaris` service for running Apache Polaris using an in-memory metastore
2) The `jupyter` service for running Jupyter notebook with PySpark (Spark 3.5.5 is used)

The notebook `SparkPolaris.ipynb` provided under `plugins/spark/v3.5/getting-started/notebooks` provides examples 
with basic commands, includes:
1) Connect to Polaris using Python client to create a Catalog and Roles
2) Start Spark session using the Polaris Spark client
3) Using Spark to perform table operations for both Delta and Iceberg

## Start Spark against a deployed Polaris service
Before starting, make sure the service deployed is up-to-date, and that Spark 3.5 with at least version 3.5.3 is installed.
Spark 3.5.5 is recommended, and you can follow the instructions below to get a Spark 3.5.5 distribution.
```shell
cd ~
wget https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz 
mkdir spark-3.5
tar xzvf spark-3.5.5-bin-hadoop3.tgz  -C spark-3.5 --strip-components=1
cd spark-3.5
```

### Connecting with Spark using the Polaris Spark client
The following CLI command can be used to start the Spark with connection to the deployed Polaris service using
a released Polaris Spark client.

```shell
bin/spark-shell \
--packages <polaris-spark-client-package>,org.apache.hadoop:hadoop-aws:3.4.0,io.delta:delta-spark_2.12:3.3.1 \
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
Assume the released Polaris Spark client you want to use is `org.apache.polaris:polaris-iceberg-1.8.1-spark-runtime-3.5_2.12:1.0.0`,
replace the `polaris-spark-client-package` field with the release.

The `spark-catalog-name` is the catalog name you will use with Spark, and `polaris-catalog-name` is the catalog name used 
by Polaris service, for simplicity, you can use the same name. 

Replace the `polaris-service-uri` with the uri of the deployed Polaris service. For example, with a locally deployed
Polaris service, the uri would be `http://localhost:8181/api/catalog`.

For `client-id` and `client-secret` values, you can refer to [Using Polaris]({{% ref "getting-started/using-polaris" %}}) 
for more details.

You can also start the connection by programmatically initialize a SparkSession, following is an example with PySpark:
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder
  .config("spark.jars.packages", "<polaris-spark-client-package>,org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.3.1")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension")
  .config("spark.sql.catalog.<spark-catalog-name>", "org.apache.polaris.spark.SparkCatalog")  
  .config("spark.sql.catalog.<spark-catalog-name>.uri", <polaris-service-uri>)
  .config("spark.sql.catalog.<spark-catalog-name>.token-refresh-enabled", "true")
  .config("spark.sql.catalog.<spark-catalog-name>.credential", "<client-id>:<client_secret>")
  .config("spark.sql.catalog.<spark-catalog-name>.warehouse", <polaris_catalog_name>)
  .config("spark.sql.catalog.polaris.scope", 'PRINCIPAL_ROLE:ALL')
  .config("spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation", 'vended-credentials')
  .getOrCreate()
```
Similar as the CLI command, make sure the corresponding fields are replaced correctly.

### Create tables with Spark
After Spark is started, you can use it to create and access Iceberg and Delta tables, for example:
```python
spark.sql("USE polaris")
spark.sql("CREATE NAMESPACE IF NOT EXISTS DELTA_NS")
spark.sql("CREATE NAMESPACE IF NOT EXISTS DELTA_NS.PUBLIC")
spark.sql("USE NAMESPACE DELTA_NS.PUBLIC")
spark.sql("""CREATE TABLE IF NOT EXISTS PEOPLE (
    id int, name string)
USING delta LOCATION 'file:///tmp/var/delta_tables/people';
""")
```

## Connecting with Spark using local Polaris Spark client
If you would like to use a version of the Spark client that is currently not yet released, you can
build a Spark client jar locally from source.

The polaris-spark project provides a task createPolarisSparkJar to help building jars for the Polaris Spark client,
The built jar is named as:
`polaris-iceberg-<icebergVersion>-spark-runtime-<sparkVersion>_<scalaVersion>-<polarisVersion>.jar`. 

For example: `polaris-iceberg-1.8.1-spark-runtime-3.5_2.12-0.10.0-beta-incubating-SNAPSHOT.jar`.

Run the following commands to build a Spark client jar that is compatible with Spark 3.5 and Scala 2.12.
```shell
cd ~/polaris
./gradlew :polaris-spark-3.5_2.12:createPolarisSparkJar
```
If you want to build a Scala 2.13 compatible jar, you can use the following command:
- `./gradlew :polaris-spark-3.5_2.13:createPolarisSparkJar` 

The result jar is located at `plugins/spark/v3.5/build/<scala_version>/libs` after the build. You can also copy the
corresponding jar to any location your Spark will have access.

When starting Spark or create Spark session, instead of providing the Polaris Spark client as a `packages` configuration, we
need to provide the `jars` configuration as follows:
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
Replace `path-to-spark-client-jar` with where the built jar is located.

## Limitations
The Polaris Spark client has the following functionality limitations:
1) Create table as select (CTAS) is not supported for Delta tables. As a result, the `saveAsTable` method of `Dataframe`
   is also not supported, since it relies on the CTAS support.
2) Create a Delta table without explicit location is not supported.
3) Rename a Delta table is not supported.
4) ALTER TABLE ... SET LOCATION/SET FILEFORMAT/ADD PARTITION is not supported for DELTA table.
5) For other non-Iceberg tables like csv, it is not supported.
