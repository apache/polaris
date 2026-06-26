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

# Apache Polaris – Custom Server Assembly

This module builds a custom Apache Polaris server with a user-selected set of plugins and
extra dependencies. It mirrors `runtime/server` but every plugin is opt-in rather than
always included.

## Quick start

Use the helper script at the project root:

```bash
./create-polaris-assembly.sh --auth ranger --federation hadoop
```

Or invoke Gradle directly:

```bash
./gradlew :polaris-custom-assembly:quarkusBuild \
  -Pcustom.authPlugins=ranger \
  -Pcustom.federationPlugins=hadoop
```

The built server lands in `runtime/custom-assembly/build/quarkus-app/` and is started with:

```bash
java -jar runtime/custom-assembly/build/quarkus-app/quarkus-run.jar
```

## Available plugins

### Auth plugins (`--auth` / `-Pcustom.authPlugins`)

| Value    | Module                              | Description                        |
|----------|-------------------------------------|------------------------------------|
| `ranger` | `:polaris-extensions-auth-ranger`   | Apache Ranger authorization        |
| `opa`    | `:polaris-extensions-auth-opa`      | Open Policy Agent authorization    |

When no auth plugin is selected the built-in internal RBAC authorizer is used.

### Federation plugins (`--federation` / `-Pcustom.federationPlugins`)

| Value      | Module                                    | Description              |
|------------|-------------------------------------------|--------------------------|
| `hadoop`   | `:polaris-extensions-federation-hadoop`   | Hadoop catalog           |
| `hive`     | `:polaris-extensions-federation-hive`     | Hive Metastore catalog   |
| `bigquery` | `:polaris-extensions-federation-bigquery` | BigQuery Metastore catalog |

### Extra Maven dependencies (`--dep` / `-Pcustom.mavenDeps`)

Any artifact resolvable from Maven Central can be added. Common use cases:

| Use case              | Coordinate                                  |
|-----------------------|---------------------------------------------|
| MySQL JDBC driver     | `com.mysql:mysql-connector-j:9.0.0`         |
| MariaDB JDBC driver   | `org.mariadb.jdbc:mariadb-java-client:3.5.1`|
| Custom plugin library | `com.example:my-polaris-plugin:1.0.0`       |

> **Note:** Adding a JDBC driver only puts it on the classpath. The relational persistence
> layer (`polaris-relational-jdbc`) currently supports PostgreSQL, CockroachDB, and H2.
> MySQL / MariaDB schema files and `DatabaseType` support would need to be added separately.

### Extra JAR files (`--jar` / `-Pcustom.extraJars`)

Local JAR files (e.g. proprietary or in-house plugins) can be bundled directly:

```bash
./create-polaris-assembly.sh --jar /opt/plugins/acme-auth-1.0.jar
```

## Gradle properties reference

| Property                  | Example value                          | Description                              |
|---------------------------|----------------------------------------|------------------------------------------|
| `custom.authPlugins`      | `ranger,opa`                           | Comma-separated auth plugin IDs          |
| `custom.federationPlugins`| `hadoop,hive`                          | Comma-separated federation plugin IDs    |
| `custom.mavenDeps`        | `com.mysql:mysql-connector-j:9.0.0`   | Comma-separated Maven coordinates        |
| `custom.extraJars`        | `/abs/path/a.jar,/abs/path/b.jar`      | Comma-separated absolute JAR paths       |

## Gradle tasks

| Task                    | Description                                                        |
|-------------------------|--------------------------------------------------------------------|
| `quarkusBuild`          | Builds the Quarkus fast-jar into `build/quarkus-app/`             |
| `packageCustomAssembly` | Packages `quarkus-app/` + launch script into a `tar.gz`           |
| `run`                   | Runs the server locally (development mode)                         |

### Build and package as a tar.gz

```bash
./gradlew :polaris-custom-assembly:packageCustomAssembly \
  -Pcustom.authPlugins=ranger \
  -Pcustom.federationPlugins=hadoop,bigquery
```

The archive is written to `runtime/custom-assembly/build/distributions/polaris-custom-*.tar.gz`.
Extract and run:

```bash
tar -xzf polaris-custom-*.tar.gz
./bin/server
```

## Examples

```bash
# Ranger auth only
./create-polaris-assembly.sh --auth ranger

# All auth plugins + all federation plugins
./create-polaris-assembly.sh --auth ranger,opa --federation hadoop,hive,bigquery

# Add MySQL JDBC driver (classpath only – see note above)
./create-polaris-assembly.sh --dep com.mysql:mysql-connector-j:9.0.0

# Multiple extra deps
./create-polaris-assembly.sh \
  --dep com.mysql:mysql-connector-j:9.0.0 \
  --dep org.example:custom-credentials-provider:2.1.0

# Bundle a proprietary JAR and copy result to a target directory
./create-polaris-assembly.sh \
  --jar /opt/plugins/acme-auth.jar \
  --output /opt/polaris-custom

# Build, package as tar.gz, and copy to a staging directory
./create-polaris-assembly.sh \
  --auth ranger \
  --federation hadoop \
  --package \
  --output /tmp/staging
```
