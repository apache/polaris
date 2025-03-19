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

Right now, the plugin only provides support for Spark 3.5 and depends on iceberg-spark-runtime 1.8.1.

# Build Plugin Jar
A task createPolarisSparkJar is added to build a jar for the Polaris Spark plugin, the jar is named as:
"polaris-iceberg-<iceberg-version>-spark-runtime-<spark_major_version>_<scala_version>"

The jar is built when building the Polaris project, and can also be built alone with the following command:
- `./gradlew :polaris-spark:createPolarisSparkJar` - Build a jar for the Polaris Spark plugin, the jar also includes iceberg-spark-runtime 1.8.1.

The result jar is located at plugins/spark/build/libs after the build, and the default scala version is 2.12.
To build a jar for scala 2.13, use the following command:
- `./gradlew :polaris-spark:createPolarisSparkJar -DscalaVersion=2.13` 
