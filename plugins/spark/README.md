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

# Polaris Spark Plugin

The Polaris Spark plugin provides a SparkCatalog class, which communicates with the Polaris
REST endpoints, and provides implementations for Apache Spark's
[TableCatalog](https://github.com/apache/spark/blob/v3.5.6/sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/TableCatalog.java),
[ViewCatalog](https://github.com/apache/spark/blob/v3.5.6/sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/ViewCatalog.java) classes.
[SupportsNamespaces](https://github.com/apache/spark/blob/v3.5.6/sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/SupportsNamespaces.java),

Right now, the plugin only provides support for Spark 3.5, Scala version 2.12 and 2.13,
and depends on iceberg-spark-runtime 1.9.1.

# Start Spark with local Polaris service using the Polaris Spark plugin
The following command starts a Polaris server for local testing, it runs on localhost:8181 with default
realm `POLARIS` and root credentials `root:s3cr3t`:
```shell
./gradlew run
```

Once the local server is running, you can start Spark with the Polaris Spark plugin using either the `--packages`
option with the Polaris Spark package, or the `--jars` option with the Polaris Spark bundle JAR.

The following sections explain how to build and run Spark with both the Polaris package and the bundle JAR.

# Build and run with Polaris spark package locally
The Polaris Spark client source code is located in plugins/spark/v3.5/spark. To use the Polaris Spark package 
with Spark, you first need to publish the source JAR to your local Maven repository.

Run the following command to build the Polaris Spark project and publish the source JAR to your local Maven repository:
- `./gradlew assemble` -- build the whole Polaris project without running tests
- `./gradlew publishToMavenLocal` -- publish Polaris project source JAR to local Maven repository

```shell
bin/spark-shell \
--packages org.apache.polaris:polaris-spark-<spark_version>_<scala_version>:<polaris_version>,org.apache.iceberg:iceberg-aws-bundle:1.9.1,io.delta:delta-spark_2.12:3.3.1 \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
--conf spark.sql.catalog.<catalog-name>.warehouse=<catalog-name> \
--conf spark.sql.catalog.<catalog-name>.header.X-Iceberg-Access-Delegation=vended-credentials \
--conf spark.sql.catalog.<catalog-name>=org.apache.polaris.spark.SparkCatalog \
--conf spark.sql.catalog.<catalog-name>.uri=http://localhost:8181/api/catalog \
--conf spark.sql.catalog.<catalog-name>.credential="root:secret" \
--conf spark.sql.catalog.<catalog-name>.scope='PRINCIPAL_ROLE:ALL' \
--conf spark.sql.catalog.<catalog-name>.token-refresh-enabled=true \
--conf spark.sql.sources.useV1SourceList=''
```

The Polaris version is defined in the `versions.txt` file located in the root directory of the Polaris project.
Assume the following values:
- `spark_version`: 3.5
- `scala_version`: 2.12
- `polaris_version`: 1.1.0-incubating-SNAPSHOT
- `catalog-name`: `polaris`
The Spark command would look like following:

```shell
bin/spark-shell \
--packages org.apache.polaris:polaris-spark-3.5_2.12:1.1.0-incubating-SNAPSHOT,org.apache.iceberg:iceberg-aws-bundle:1.9.1,io.delta:delta-spark_2.12:3.3.1 \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
--conf spark.sql.catalog.polaris.warehouse=polaris \
--conf spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation=vended-credentials \
--conf spark.sql.catalog.polaris=org.apache.polaris.spark.SparkCatalog \
--conf spark.sql.catalog.polaris.uri=http://localhost:8181/api/catalog \
--conf spark.sql.catalog.polaris.credential="root:secret" \
--conf spark.sql.catalog.polaris.scope='PRINCIPAL_ROLE:ALL' \
--conf spark.sql.catalog.polaris.token-refresh-enabled=true \
--conf spark.sql.sources.useV1SourceList=''
```

# Build and run with Polaris spark bundle JAR
The polaris-spark project also provides a Spark bundle JAR for the `--jars` use case. The resulting JAR will follow this naming format:
polaris-spark-<spark_version>_<scala_version>-<polaris_version>-bundle.jar
For example:
polaris-spark-bundle-3.5_2.12-1.1.0-incubating-SNAPSHOT-bundle.jar

Run `./gradlew assemble` to build the entire Polaris project without running tests. After the build completes, 
the bundle JAR can be found under: plugins/spark/v3.5/spark/build/<scala_version>/libs/.
To start Spark using the bundle JAR, specify it with the `--jars` option as shown below:

```shell
bin/spark-shell \
--jars <path-to-spark-client-jar> \
--packages org.apache.iceberg:iceberg-aws-bundle:1.9.1,io.delta:delta-spark_2.12:3.3.1 \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
--conf spark.sql.catalog.<catalog-name>.warehouse=<catalog-name> \
--conf spark.sql.catalog.<catalog-name>.header.X-Iceberg-Access-Delegation=vended-credentials \
--conf spark.sql.catalog.<catalog-name>=org.apache.polaris.spark.SparkCatalog \
--conf spark.sql.catalog.<catalog-name>.uri=http://localhost:8181/api/catalog \
--conf spark.sql.catalog.<catalog-name>.credential="root:secret" \
--conf spark.sql.catalog.<catalog-name>.scope='PRINCIPAL_ROLE:ALL' \
--conf spark.sql.catalog.<catalog-name>.token-refresh-enabled=true \
--conf spark.sql.sources.useV1SourceList=''
```

# Limitations
The Polaris Spark client supports catalog management for both Iceberg and Delta tables, it routes all Iceberg table
requests to the Iceberg REST endpoints, and routes all Delta table requests to the Generic Table REST endpoints.

The Spark Client requires at least delta 3.2.1 to work with Delta tables, which requires at least Apache Spark 3.5.3.
Following describes the current functionality limitations of the Polaris Spark client:
1) Create table as select (CTAS) is not supported for Delta tables. As a result, the `saveAsTable` method of `Dataframe`
   is also not supported, since it relies on the CTAS support.
2) Create a Delta table without explicit location is not supported.
3) Rename a Delta table is not supported.
4) ALTER TABLE ... SET LOCATION is not supported for DELTA table.
5) For other non-Iceberg tables like csv, it is not supported today.
