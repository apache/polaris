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

## Named Datasources and Runtime Activation

Polaris uses Quarkus [named datasources](https://quarkus.io/guides/datasource#multiple-datasources) to
allow selecting which database connection to use at runtime without rebuilding the application.

Two named datasources are built into the Polaris distribution:

| Name         | Database                    | Suitable for    |
|--------------|-----------------------------|-----------------|
| `postgresql` | PostgreSQL (or CockroachDB) | Production use  |
| `h2`         | H2                          | Testing only    |

The `polaris.persistence.relational.jdbc.datasource` property selects which datasource Polaris
activates. Polaris automatically activates the selected datasource and deactivates all others —
**do not set `quarkus.datasource.*.active` manually**.

### Using custom datasources

You can also define a custom named datasource (for example, to use a different JDBC driver) by
configuring its Quarkus properties and pointing `polaris.persistence.relational.jdbc.datasource` at
it:

```properties
polaris.persistence.type=relational-jdbc
polaris.persistence.relational.jdbc.datasource=mydb

# Register the custom datasource with Quarkus
quarkus.datasource.mydb.db-kind=postgresql
quarkus.datasource.mydb.jdbc.url=jdbc:postgresql://myhost:5432/polaris
quarkus.datasource.mydb.username=<your-username>
quarkus.datasource.mydb.password=<your-password>
```

{{< alert warning >}}
The property `quarkus.datasource.<name>.db-kind` is a build-time property! If you intend to use
custom datasources, you must rebuild the Polaris server image.
{{< /alert >}}

### CockroachDB

CockroachDB uses the PostgreSQL wire protocol and JDBC driver, so it works with the built-in
`postgresql` named datasource. However, Polaris uses CockroachDB-specific schema DDL, so you must
set `polaris.persistence.relational.jdbc.database-type=cockroachdb` explicitly — auto-detection via
JDBC metadata is not reliable in all deployment configurations.

```properties
polaris.persistence.type=relational-jdbc
polaris.persistence.relational.jdbc.datasource=postgresql
polaris.persistence.relational.jdbc.database-type=cockroachdb

quarkus.datasource.postgresql.jdbc.url=jdbc:postgresql://<cockroachdb-host>:26257/<db-name>?sslmode=disable
quarkus.datasource.postgresql.username=<your-username>
quarkus.datasource.postgresql.password=<your-password>
```

We have 2 options for configuring the persistence backend with the built-in datasources:

## 1. Relational JDBC metastore with username and password

Using environment variables:

```properties
POLARIS_PERSISTENCE_TYPE=relational-jdbc
POLARIS_PERSISTENCE_RELATIONAL_JDBC_DATASOURCE=postgresql

QUARKUS_DATASOURCE_POSTGRESQL_USERNAME=<your-username>
QUARKUS_DATASOURCE_POSTGRESQL_PASSWORD=<your-password>
QUARKUS_DATASOURCE_POSTGRESQL_JDBC_URL=<jdbc-url-of-postgres>
```

Using properties file:

```properties
polaris.persistence.type=relational-jdbc
polaris.persistence.relational.jdbc.datasource=postgresql

quarkus.datasource.postgresql.username=<your-username>
quarkus.datasource.postgresql.password=<your-password>
quarkus.datasource.postgresql.jdbc.url=<jdbc-url-of-postgres>
```

## 2. AWS Aurora PostgreSQL metastore using IAM AWS authentication

```properties
polaris.persistence.type=relational-jdbc
polaris.persistence.relational.jdbc.datasource=postgresql

quarkus.datasource.postgresql.jdbc.url=jdbc:postgresql://polaris-cluster.cluster-xyz.us-east-1.rds.amazonaws.com:6160/polaris
quarkus.datasource.postgresql.jdbc.additional-jdbc-properties.wrapperPlugins=iam
quarkus.datasource.postgresql.username=dbusername
quarkus.datasource.postgresql.jdbc.additional-jdbc-properties.ssl=true
quarkus.datasource.postgresql.jdbc.additional-jdbc-properties.sslmode=require
quarkus.datasource.postgresql.credentials-provider=aws

quarkus.rds.credentials-provider.aws.use-quarkus-client=true
quarkus.rds.credentials-provider.aws.username=dbusername
quarkus.rds.credentials-provider.aws.hostname=polaris-cluster.cluster-xyz.us-east-1.rds.amazonaws.com
quarkus.rds.credentials-provider.aws.port=6160
```

This is the basic configuration. For more details, please refer to the [Quarkus plugin documentation](https://docs.quarkiverse.io/quarkus-amazon-services/dev/amazon-rds.html#_configuration_reference).

The Relational JDBC metastore currently relies on Quarkus-managed named datasources. The built-in datasources support PostgreSQL and H2; custom datasources can be registered for other JDBC-compatible databases. At this time, official documentation is provided exclusively for usage with PostgreSQL.
Please refer to the documentation here:
[Configure data sources in Quarkus](https://quarkus.io/guides/datasource).

Additionally, the retries can be configured via `polaris.persistence.relational.jdbc.*` properties; please refer to the [Configuring Polaris]({{% ref "../configuration" %}}) section.

## Bootstrapping Polaris

Before using Polaris with the Relational JDBC backend, you must bootstrap the metastore to create the necessary schema and initial realm. This is done using the [Admin Tool]({{% ref "../admin-tool" %}}).

Using Docker:

```bash
docker run --rm -it \
  --env="polaris.persistence.type=relational-jdbc" \
  --env="polaris.persistence.relational.jdbc.datasource=postgresql" \
  --env="quarkus.datasource.postgresql.username=<your-username>" \
  --env="quarkus.datasource.postgresql.password=<your-password>" \
  --env="quarkus.datasource.postgresql.jdbc.url=<jdbc-url-of-postgres>" \
  apache/polaris-admin-tool:latest bootstrap -r <realm-name> -c <realm-name>,<client-id>,<client-secret>
```

Using the standalone JAR:

```bash
java \
  -Dpolaris.persistence.type=relational-jdbc \
  -Dpolaris.persistence.relational.jdbc.datasource=postgresql \
  -Dquarkus.datasource.postgresql.username=<your-username> \
  -Dquarkus.datasource.postgresql.password=<your-password> \
  -Dquarkus.datasource.postgresql.jdbc.url=<jdbc-url-of-postgres> \
  -jar polaris-admin-tool.jar bootstrap -r <realm-name> -c <realm-name>,<client-id>,<client-secret>
```

For more details on the bootstrap command and other administrative operations, see the [Admin Tool]({{% ref "../admin-tool" %}}) documentation.
