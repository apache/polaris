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
[TableCatalog](https://github.com/apache/spark/blob/v3.5.5/sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/TableCatalog.java),
[SupportsNamespaces](https://github.com/apache/spark/blob/v3.5.5/sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/SupportsNamespaces.java),
[ViewCatalog](https://github.com/apache/spark/blob/v3.5.5/sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/ViewCatalog.java) classes.

Right now, the plugin only provides support for Spark 3.5, Scala version 2.12 and 2.13,
and depends on iceberg-spark-runtime 1.9.0.

# Build Plugin Jar
A task createPolarisSparkJar is added to build a jar for the Polaris Spark plugin, the jar is named as:
`polaris-spark-<sparkVersion>_<scalaVersion>-<polarisVersion>-bundle.jar`. For example:
`polaris-spark-3.5_2.12-0.11.0-beta-incubating-SNAPSHOT-bundle.jar`.

- `./gradlew :polaris-spark-3.5_2.12:createPolarisSparkJar` -- build jar for Spark 3.5 with Scala version 2.12.
- `./gradlew :polaris-spark-3.5_2.13:createPolarisSparkJar` -- build jar for Spark 3.5 with Scala version 2.13.

The result jar is located at plugins/spark/v3.5/build/<scala_version>/libs after the build.

# Start Spark with Local Polaris Service using built Jar
Once the jar is built, we can manually test it with Spark and a local Polaris service.

The following command starts a Polaris server for local testing, it runs on localhost:8181 with default
realm `POLARIS` and root credentials `root:secret`:
```shell
./gradlew run
```

Once the local server is running, the following command can be used to start the spark-shell with the built Spark client
jar, and to use the local Polaris server as a Catalog.

```shell
bin/spark-shell \
--jars <path-to-spark-client-jar> \
--packages org.apache.iceberg:iceberg-aws-bundle:1.9.0,io.delta:delta-spark_2.12:3.3.1 \
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

Assume the path to the built Spark client jar is
`/polaris/plugins/spark/v3.5/spark/build/2.12/libs/polaris-spark-3.5_2.12-0.11.0-beta-incubating-SNAPSHOT-bundle.jar`
and the name of the catalog is `polaris`. The cli command will look like following:

```shell
bin/spark-shell \
--jars /polaris/plugins/spark/v3.5/spark/build/2.12/libs/polaris-spark-3.5_2.12-0.11.0-beta-incubating-SNAPSHOT-bundle.jar \
--packages org.apache.iceberg:iceberg-aws-bundle:1.9.0,io.delta:delta-spark_2.12:3.3.1 \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
--conf spark.sql.catalog.polaris.warehouse=<catalog-name> \
--conf spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation=vended-credentials \
--conf spark.sql.catalog.polaris=org.apache.polaris.spark.SparkCatalog \
--conf spark.sql.catalog.polaris.uri=http://localhost:8181/api/catalog \
--conf spark.sql.catalog.polaris.credential="root:secret" \
--conf spark.sql.catalog.polaris.scope='PRINCIPAL_ROLE:ALL' \
--conf spark.sql.catalog.polaris.token-refresh-enabled=true \
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
