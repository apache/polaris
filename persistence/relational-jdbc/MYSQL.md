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

# MySQL support for the Relational JDBC backend (custom downstream build)

The Relational JDBC persistence backend can run on MySQL 8.0+, alongside the officially supported PostgreSQL, CockroachDB and H2 options.

> **The MySQL JDBC driver is GPL-licensed (ASF Category X) and is intentionally not bundled in the official Apache Polaris release artifacts** (see [issue #2491](https://github.com/apache/polaris/issues/2491)). The Java code that supports MySQL ships in the official source release, but enabling it requires building your own runner from source with the opt-in flag described below. The resulting runner is a *custom downstream derivative*, not an official Apache Polaris artifact, and the builder is responsible for satisfying the driver's GPL v2 + FOSS Exception obligations.

## Building a MySQL-capable server

Add the MySQL JDBC driver to the runner with the `-PincludeMysqlDriver=true` Gradle property:

```shell
./gradlew :polaris-server:assemble -PincludeMysqlDriver=true
```

The Docker image can be built the same way with `-Dquarkus.container-image.build=true`. To avoid overwriting the GPL-free image, pass a distinct image name:

```shell
./gradlew \
  :polaris-server:assemble \
  :polaris-server:quarkusAppPartsBuild --rerun \
  -PincludeMysqlDriver=true \
  -Dquarkus.container-image.build=true \
  -Dquarkus.container-image.name=polaris-mysql
```

### Verification contract under `-PincludeMysqlDriver=true`

Supported tasks: `:polaris-server:assemble` and the Quarkus image build (`quarkusAppPartsBuild` with `-Dquarkus.container-image.build=true`).

`check`, `build`, `publishToMavenLocal`, and the `generateLicenseReport` / `checkLicense` / `licenseReportZip` tasks will fail by design because the GPL MySQL JDBC driver is intentionally absent from `LICENSE`. No license tasks are disabled — the default (GPL-free) build continues to run and pass all license checks unchanged. Downstream builders distributing the resulting (non-official) artifact are responsible for satisfying the driver's GPL v2 + FOSS Exception obligations.

## Runtime configuration

MySQL is exposed as a Quarkus *named* datasource (`mysql`) that is declared inactive by default. To use the MySQL backend at runtime, set the following properties (or the equivalent environment variables, e.g. `QUARKUS_DATASOURCE_MYSQL_JDBC_URL`):

```properties
polaris.persistence.type=relational-jdbc
polaris.persistence.relational.jdbc.database-type=mysql

quarkus.datasource.mysql.active=true
quarkus.datasource.mysql.jdbc.url=jdbc:mysql://<host>:3306/POLARIS_SCHEMA
quarkus.datasource.mysql.username=<your-username>
quarkus.datasource.mysql.password=<your-password>
```

PostgreSQL, CockroachDB and H2 continue to use the default (unnamed) datasource and require no changes to existing configuration.

If `database-type=mysql` is configured but the `mysql` named datasource is not available (for example, the runner was built without `-PincludeMysqlDriver=true`, or `quarkus.datasource.mysql.active` is left at its default of `false`), the server fails fast at startup with a message pointing back to the build flag and the `active` / connection properties above, rather than silently falling back to the default datasource.

## Bootstrapping the MySQL backend

The Polaris admin tool does not currently bundle the MySQL JDBC driver, so the standard admin-tool `bootstrap` flow does not apply. Instead, configure the Polaris server to bootstrap itself on first startup:

```properties
polaris.persistence.auto-bootstrap-types=in-memory,relational-jdbc
polaris.realm-context.realms=<your-realm>
```

```shell
POLARIS_BOOTSTRAP_CREDENTIALS=<realm>,<client-id>,<client-secret>
```

The server will execute `mysql/schema-v4.sql` and create the root principal automatically. Run a single replica for the initial bootstrap to avoid races; the bootstrap log line confirms completion.

**Auto-bootstrap on persistent backends is not yet idempotent** ([apache/polaris#2324](https://github.com/apache/polaris/issues/2324)): re-running the server with these settings against an already-bootstrapped database fails on startup with `metastore manager has already been bootstrapped`. Treat the auto-bootstrap startup as a one-time operation until #2324 is resolved.
