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
title: Relational JDBC
type: docs
weight: 100
---

The Relational JDBC metastore supports PostgreSQL, CockroachDB, and H2. H2 is intended for testing
and development only.

Polaris can use either a Quarkus-managed datasource or a Polaris-managed JDBC connection pool. When
`polaris.persistence.relational.jdbc.jdbc-url` is set, Polaris creates the JDBC pool directly;
otherwise, Polaris uses the Quarkus datasource configuration. Both forms support configuration
through environment variables or JVM `-D` flags at startup. For more information, refer to the
[Quarkus configuration reference](https://quarkus.io/guides/config-reference#env-file).

We have 3 options for configuring the persistence backend:

## 1. Quarkus-managed Relational JDBC metastore with username and password

Using environment variables:

```properties
POLARIS_PERSISTENCE_TYPE=relational-jdbc

QUARKUS_DATASOURCE_USERNAME=<your-username>
QUARKUS_DATASOURCE_PASSWORD=<your-password>
QUARKUS_DATASOURCE_JDBC_URL=<jdbc-url-of-postgres>
```

Using properties file:

```properties
polaris.persistence.type=relational-jdbc
quarkus.datasource.username=<your-username>
quarkus.datasource.password=<your-password>
quarkus.datasource.jdbc.url=<jdbc-url-of-postgres>
```

## 2. Polaris-managed Relational JDBC metastore with username and password

Using environment variables:

```properties
POLARIS_PERSISTENCE_TYPE=relational-jdbc

POLARIS_PERSISTENCE_RELATIONAL_JDBC_DATABASE_TYPE=postgresql
POLARIS_PERSISTENCE_RELATIONAL_JDBC_JDBC_URL=<jdbc-url-of-postgres>
POLARIS_PERSISTENCE_RELATIONAL_JDBC_DRIVER=org.postgresql.Driver
POLARIS_PERSISTENCE_RELATIONAL_JDBC_USERNAME=<your-username>
POLARIS_PERSISTENCE_RELATIONAL_JDBC_PASSWORD=<your-password>
```

Using properties file:

```properties
polaris.persistence.type=relational-jdbc
polaris.persistence.relational.jdbc.database-type=postgresql
polaris.persistence.relational.jdbc.jdbc-url=<jdbc-url-of-postgres>
polaris.persistence.relational.jdbc.driver=org.postgresql.Driver
polaris.persistence.relational.jdbc.username=<your-username>
polaris.persistence.relational.jdbc.password=<your-password>
```

When using the binary distribution, place third-party JDBC driver jars under
`server/jdbc-drivers`. Polaris loads jars from that directory before creating the managed
datasource. If the jars live elsewhere, set
`polaris.persistence.relational.jdbc.driver-directory=/path/to/jdbc-drivers` or
`POLARIS_PERSISTENCE_RELATIONAL_JDBC_DRIVER_DIRECTORY=/path/to/jdbc-drivers`. The admin tool runs
from `admin`, so bootstrap and purge commands need the same driver jar under `admin/jdbc-drivers` or
the same explicit `driver-directory` setting.

For CockroachDB, use the PostgreSQL JDBC driver and set the database type explicitly:

```properties
polaris.persistence.type=relational-jdbc
polaris.persistence.relational.jdbc.database-type=cockroachdb
polaris.persistence.relational.jdbc.jdbc-url=jdbc:postgresql://cockroachdb:26257/polaris?sslmode=disable
polaris.persistence.relational.jdbc.driver=org.postgresql.Driver
polaris.persistence.relational.jdbc.username=<your-username>
polaris.persistence.relational.jdbc.password=<your-password>
```

## 3. AWS Aurora PostgreSQL metastore using IAM AWS authentication

```properties
polaris.persistence.type=relational-jdbc
quarkus.datasource.jdbc.url=jdbc:postgresql://polaris-cluster.cluster-xyz.us-east-1.rds.amazonaws.com:6160/polaris
quarkus.datasource.jdbc.additional-jdbc-properties.wrapperPlugins=iam
quarkus.datasource.username=dbusername
quarkus.datasource.db-kind=postgresql
quarkus.datasource.jdbc.additional-jdbc-properties.ssl=true
quarkus.datasource.jdbc.additional-jdbc-properties.sslmode=require
quarkus.datasource.credentials-provider=aws

quarkus.rds.credentials-provider.aws.use-quarkus-client=true
quarkus.rds.credentials-provider.aws.username=dbusername
quarkus.rds.credentials-provider.aws.hostname=polaris-cluster.cluster-xyz.us-east-1.rds.amazonaws.com
quarkus.rds.credentials-provider.aws.port=6160
```

This is the basic configuration. For more details, please refer to the [Quarkus plugin documentation](https://docs.quarkiverse.io/quarkus-amazon-services/dev/amazon-rds.html#_configuration_reference).

The retries and Polaris-managed datasource pool can be configured via
`polaris.persistence.relational.jdbc.*` properties; please refer to the
[Configuring Polaris]({{% ref "../configuration" %}}) section.

## Bootstrapping Polaris

Before using Polaris with the Relational JDBC backend, you must bootstrap the metastore to create the necessary schema and initial realm. This is done using the [Admin Tool]({{% ref "../admin-tool" %}}).

Using Docker:

```bash
docker run --rm -it \
  --env="polaris.persistence.type=relational-jdbc" \
  --env="quarkus.datasource.username=<your-username>" \
  --env="quarkus.datasource.password=<your-password>" \
  --env="quarkus.datasource.jdbc.url=<jdbc-url-of-postgres>" \
  apache/polaris-admin-tool:latest bootstrap -r <realm-name> -c <realm-name>,<client-id>,<client-secret>
```

Using the standalone JAR:

```bash
java \
  -Dpolaris.persistence.type=relational-jdbc \
  -Dquarkus.datasource.username=<your-username> \
  -Dquarkus.datasource.password=<your-password> \
  -Dquarkus.datasource.jdbc.url=<jdbc-url-of-postgres> \
  -jar polaris-admin-tool.jar bootstrap -r <realm-name> -c <realm-name>,<client-id>,<client-secret>
```

When using the Polaris-managed JDBC pool, pass the corresponding
`polaris.persistence.relational.jdbc.*` settings to the admin tool instead of the Quarkus datasource
settings.

For more details on the bootstrap command and other administrative operations, see the [Admin Tool]({{% ref "../admin-tool" %}}) documentation.

## Schema upgrades

Polaris does not run automated schema migrations. Bootstrapping applies a full `schema-vN.sql`
script and records the schema version in the `polaris_schema.version` table; upgrading an existing
database to a newer schema version is a manual, operator-driven step.

### Upgrading to schema v5

Schema v5 makes the `events.catalog_id` column nullable: events that are not scoped to a catalog
(principal, policy, rate-limiting, etc.) store `NULL` instead of the legacy placeholder string
`__realm__` that pre-v5 schemas required (the placeholder only ever existed in 1.6.0 release
candidates).

Until you upgrade, the server keeps working: it detects a schema version below 5 from the
`polaris_schema.version` table at startup and continues writing the legacy placeholder for events
that are not catalog-scoped. To upgrade an existing v3/v4 database, run the following one-time SQL
(adjust the `ALTER` syntax to your database if needed — the statements below work on PostgreSQL,
CockroachDB, and H2), then restart Polaris:

```sql
ALTER TABLE polaris_schema.events ALTER COLUMN catalog_id DROP NOT NULL;
UPDATE polaris_schema.events SET catalog_id = NULL WHERE catalog_id = '__realm__';
UPDATE polaris_schema.version SET version_value = 5 WHERE version_key = 'version';
```
