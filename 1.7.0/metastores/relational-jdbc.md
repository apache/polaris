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

This implementation leverages Quarkus for datasource management and supports configuration through
environment variables or JVM -D flags at startup. For more information, refer to the [Quarkus configuration reference](https://quarkus.io/guides/config-reference#env-file).

We have 2 options for configuring the persistence backend:

## 1. Relational JDBC metastore with username and password

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
quarkus.datasource.jdbc.username=<your-username>
quarkus.datasource.jdbc.password=<your-password>
quarkus.datasource.jdbc.jdbc-url=<jdbc-url-of-postgres>
```

## 2. AWS Aurora PostgreSQL metastore using IAM AWS authentication

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

The Relational JDBC metastore currently relies on a Quarkus-managed datasource and supports only PostgresSQL and H2 databases. At this time, official documentation is provided exclusively for usage with PostgreSQL.
Please refer to the documentation here:
[Configure data sources in Quarkus](https://quarkus.io/guides/datasource).

Additionally, the retries can be configured via `polaris.persistence.relational.jdbc.*` properties; please refer to the [Configuring Polaris]({{% ref "../configuration" %}}) section.

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
