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

The Relational JDBC metastore relies on a Quarkus-managed datasource. For more information, refer to
the [Quarkus datasource documentation] and [Quarkus configuration reference].

{{< alert important >}}
The JDBC metastore currently supports only PostgreSQL and CockroachDB databases.
{{< /alert >}}

## Configuration

Check below two configuration examples:

1. Regular JDBC connection with username and password.
2. AWS Aurora PostgreSQL metastore using IAM AWS authentication.

The examples show the basic configuration, but many other options are available. For more details, 
please refer to the [Quarkus configuration reference].

### 1. Relational JDBC metastore with username and password

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

### 2. AWS Aurora PostgreSQL metastore using IAM AWS authentication

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

For more details about how to configure the AWS Aurora PostgreSQL metastore using IAM AWS
authentication, please refer to the [Quarkus Amazon RDS Client docs].

[Quarkus Amazon RDS Client docs]: https://docs.quarkiverse.io/quarkus-amazon-services/dev/amazon-rds.html#_configuration_reference

### Additional configuration

The following `polaris.persistence.relational.jdbc.*` properties tune retry behavior and database
type detection:

```properties
# Maximum number of attempts for retryable operations (default: 1, i.e. no retries)
polaris.persistence.relational.jdbc.max-retries=3

# Maximum total time to spend on retries in milliseconds (default: 5000)
polaris.persistence.relational.jdbc.max-duration-in-ms=10000

# Initial delay between retries in milliseconds, doubled on each attempt (default: 100)
polaris.persistence.relational.jdbc.initial-delay-in-ms=200

# Explicitly set the database type instead of inferring it from JDBC metadata.
# Supported values: postgresql, cockroachdb, h2
polaris.persistence.relational.jdbc.database-type=postgresql
```

For the full list of available properties with their types and defaults, please refer to the
[Configuring Polaris]({{% ref "../configuration" %}}) section.

## SQL Schema

### Schema Name

By default, Polaris stores its tables in a schema named `POLARIS_SCHEMA`. The schema is selected
entirely through the datasource configuration — the persistence code itself is agnostic of the
schema name. Polaris ships the following default in `application.properties`:

```properties
quarkus.datasource.jdbc.additional-jdbc-properties.currentSchema=POLARIS_SCHEMA
```

This sets the PostgreSQL (and CockroachDB) driver's `currentSchema` connection property on every
connection. To use a different schema — for example, to run multiple Polaris deployments in the
same database or to comply with a schema-naming policy — override this property:

```properties
quarkus.datasource.jdbc.additional-jdbc-properties.currentSchema=<your-schema-name>
```

Alternatively, set `currentSchema` directly in the JDBC URL; the URL takes precedence over the
property. The name is passed to the driver unquoted, so the database applies its usual identifier
case folding (for example, PostgreSQL folds it to lowercase).

{{< alert important >}}
**Upgrading an existing, pre-1.8.0 deployment to Polaris 1.8.0 or higher:** if your JDBC URL already
sets `currentSchema`, check it before upgrading. Earlier versions qualified every query with
`POLARIS_SCHEMA`, so the setting had no effect on where Polaris read and wrote. It is now what
selects the schema, and a value in the URL takes precedence over the shipped default — so an upgrade
can point Polaris away from its existing tables, and a subsequent bootstrap would create a second,
empty set of tables in the other schema. Either remove the setting from the URL, or point it at the
schema that already holds your Polaris tables. Deployments that never set `currentSchema` are
unaffected.
{{< /alert >}}

### Schema Structure

Starting with Polaris 1.8.0, each Polaris binary ships a single `schema.sql` script for each
supported database type.

For this release ({{< releaseVersion >}}), the corresponding SQL files are:

* PostgreSQL: [schema.sql]({{< github-polaris "persistence/relational-jdbc/src/main/resources/postgres/schema.sql" >}})
* CockroachDB: [schema.sql]({{< github-polaris "persistence/relational-jdbc/src/main/resources/cockroachdb/schema.sql" >}})

The `schema.sql` file is versioned, and contains the SQL statements to create the Polaris schema and
tables, as well as the initial data for the `version` table that tracks the schema version.

{{< alert note >}}
Prior to Polaris 1.8.0, each schema version used to have its own SQL file, e.g. `schema-v1.sql`,
`schema-v2.sql`, etc.
{{< /alert >}}

Polaris checks the `version` table on the first request to each realm, and fails fast with an
actionable error message if the recorded version does not match what the binary expects.

## Setting Up a Fresh Deployment

Setting up a fresh deployment is a two-step procedure:

1. **A database administrator creates the schema.** Polaris does not issue `CREATE SCHEMA`, since
   that is a privileged operation best performed by a DBA. For example:
   ```sql
   CREATE SCHEMA polaris_schema;
   ```
   The database user that Polaris runs with needs `USAGE` (and, for bootstrap, `CREATE`) privileges
   on that schema only.

2. **The [Admin Tool]({{% ref "../admin-tool" %}}) bootstraps the realm.** The bootstrap command
   creates the necessary SQL tables (using the `schema.sql` shipped with the binary) and the initial
   realm. The datasource must be configured with the same schema as the one created in step 1.

Using Docker:

```bash
docker run --rm -it \
  --env="polaris.persistence.type=relational-jdbc" \
  --env="quarkus.datasource.username=<your-username>" \
  --env="quarkus.datasource.password=<your-password>" \
  --env="quarkus.datasource.jdbc.url=<jdbc-url-of-postgres>" \
  --env="quarkus.datasource.jdbc.additional-jdbc-properties.currentSchema=POLARIS_SCHEMA" \
  apache/polaris-admin-tool:latest bootstrap -r <realm-name> -c <realm-name>,<client-id>,<client-secret>
```

Using the standalone JAR:

```bash
java \
  -Dpolaris.persistence.type=relational-jdbc \
  -Dquarkus.datasource.username=<your-username> \
  -Dquarkus.datasource.password=<your-password> \
  -Dquarkus.datasource.jdbc.url=<jdbc-url-of-postgres> \
  "-Dquarkus.datasource.jdbc.additional-jdbc-properties.currentSchema=POLARIS_SCHEMA" \
  -jar polaris-admin-tool.jar bootstrap -r <realm-name> -c <realm-name>,<client-id>,<client-secret>
```

Replace `POLARIS_SCHEMA` with the schema name created in step 1 if you chose a different name. See
the [Schema name](#schema-name) section for details. Alternatively, set `currentSchema` directly in
the JDBC URL; the URL takes precedence over the property.

For more details on the bootstrap command and other administrative operations, see the
[Admin Tool]({{% ref "../admin-tool" %}}) documentation.

## Upgrading Polaris

Upgrading Polaris on an existing installation requires two steps, in this order:

1. **Apply schema migrations** for each schema version between your current version and the target
   version (see the migration SQL in the sections below). Schema migrations must be applied
   **before** starting the new Polaris binary. Starting the binary with an outdated schema will
   cause it to fail fast with an actionable error message.

2. **Bootstrap new realms** if needed. Re-bootstrapping an existing installation is not required
   unless you want to add new realms. The bootstrap command is idempotent — already-bootstrapped
   realms are simply ignored — so it can be safely run again if needed.

{{< alert important >}}
**Re-bootstrapping is not sufficient to upgrade the SQL schema.** Always apply the SQL migration
first.
{{< /alert >}}

The SQL statements below are for PostgreSQL; other databases may require syntax changes.

### Migration From Schema v5 to v6

```sql
DROP INDEX IF EXISTS idx_locations;
CREATE INDEX IF NOT EXISTS idx_locations ON entities 
    USING btree (realm_id, catalog_id, location_without_scheme)
    WHERE location_without_scheme IS NOT NULL;
UPDATE version SET version_value = 6 WHERE version_key = 'version';
```

### Migration From Schema v4 to v5

```sql
DROP INDEX IF EXISTS idx_idemp_realm_expires;
DROP TABLE IF EXISTS idempotency_records;
ALTER TABLE events ALTER COLUMN catalog_id DROP NOT NULL;
UPDATE events SET catalog_id = NULL WHERE catalog_id = '__realm__';
DROP TABLE IF EXISTS idempotency_records;
UPDATE version SET version_value = 5 WHERE version_key = 'version';
```

### Migration From Schema v3 to v4

```sql
CREATE INDEX IF NOT EXISTS idx_entities_catalog_id_id
    ON entities (catalog_id, id);
CREATE INDEX IF NOT EXISTS idx_grants_realm_grantee
    ON grant_records (realm_id, grantee_id);
CREATE INDEX IF NOT EXISTS idx_grants_realm_securable
    ON grant_records (realm_id, securable_id);
CREATE TABLE IF NOT EXISTS idempotency_records (
    realm_id TEXT NOT NULL, idempotency_key TEXT NOT NULL, operation_type TEXT NOT NULL,
    resource_id TEXT NOT NULL, http_status INTEGER, error_subtype TEXT,            
    response_summary TEXT, response_headers TEXT, finalized_at TIMESTAMP,        
    created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL,  heartbeat_at TIMESTAMP,        
    executor_id TEXT, expires_at TIMESTAMP,
    PRIMARY KEY (realm_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_idemp_realm_expires
    ON idempotency_records (realm_id, expires_at);
CREATE TABLE IF NOT EXISTS scan_metrics_report (
    report_id TEXT NOT NULL, realm_id TEXT NOT NULL, catalog_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL, timestamp_ms BIGINT NOT NULL, principal_name TEXT,
    request_id TEXT, otel_trace_id TEXT, otel_span_id TEXT, report_trace_id TEXT,
    snapshot_id BIGINT, schema_id INTEGER, filter_expression TEXT,
    projected_field_ids TEXT, projected_field_names TEXT,
    result_data_files BIGINT DEFAULT 0, result_delete_files BIGINT DEFAULT 0,
    total_file_size_bytes BIGINT DEFAULT 0, total_data_manifests BIGINT DEFAULT 0,
    total_delete_manifests BIGINT DEFAULT 0, scanned_data_manifests BIGINT DEFAULT 0,
    scanned_delete_manifests BIGINT DEFAULT 0, skipped_data_manifests BIGINT DEFAULT 0,
    skipped_delete_manifests BIGINT DEFAULT 0, skipped_data_files BIGINT DEFAULT 0,
    skipped_delete_files BIGINT DEFAULT 0, total_planning_duration_ms BIGINT DEFAULT 0,
    equality_delete_files BIGINT DEFAULT 0, positional_delete_files BIGINT DEFAULT 0,
    indexed_delete_files BIGINT DEFAULT 0, total_delete_file_size_bytes BIGINT DEFAULT 0,
    metadata JSONB DEFAULT '{}', PRIMARY KEY (realm_id, report_id)
);
CREATE INDEX IF NOT EXISTS idx_scan_report_timestamp
    ON scan_metrics_report (realm_id, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_scan_report_lookup
    ON scan_metrics_report (realm_id, catalog_id, table_id, timestamp_ms);
CREATE TABLE IF NOT EXISTS commit_metrics_report (
    report_id TEXT NOT NULL, realm_id TEXT NOT NULL, catalog_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL, timestamp_ms BIGINT NOT NULL, principal_name TEXT,
    request_id TEXT, otel_trace_id TEXT, otel_span_id TEXT, report_trace_id TEXT,
    snapshot_id BIGINT NOT NULL, sequence_number BIGINT, operation TEXT NOT NULL,
    added_data_files BIGINT DEFAULT 0, removed_data_files BIGINT DEFAULT 0,
    total_data_files BIGINT DEFAULT 0, added_delete_files BIGINT DEFAULT 0,
    removed_delete_files BIGINT DEFAULT 0, total_delete_files BIGINT DEFAULT 0,
    added_equality_delete_files BIGINT DEFAULT 0, removed_equality_delete_files BIGINT DEFAULT 0,
    added_positional_delete_files BIGINT DEFAULT 0, removed_positional_delete_files BIGINT DEFAULT 0,
    added_records BIGINT DEFAULT 0, removed_records BIGINT DEFAULT 0,
    total_records BIGINT DEFAULT 0, added_file_size_bytes BIGINT DEFAULT 0,
    removed_file_size_bytes BIGINT DEFAULT 0, total_file_size_bytes BIGINT DEFAULT 0,
    total_duration_ms BIGINT DEFAULT 0, attempts INTEGER DEFAULT 1,
    metadata JSONB DEFAULT '{}', PRIMARY KEY (realm_id, report_id)
);
CREATE INDEX IF NOT EXISTS idx_commit_report_timestamp
    ON commit_metrics_report (realm_id, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_commit_report_lookup
    ON commit_metrics_report (realm_id, catalog_id, table_id, timestamp_ms);
UPDATE version SET version_value = 4 WHERE version_key = 'version';
```

### Migration From Schema v2 to v3

```sql
DROP TABLE IF EXISTS policy_mapping_record;
CREATE TABLE IF NOT EXISTS policy_mapping_record (
    realm_id TEXT NOT NULL,
    target_catalog_id BIGINT NOT NULL,
    target_id BIGINT NOT NULL,
    policy_type_code INTEGER NOT NULL,
    policy_catalog_id BIGINT NOT NULL,
    policy_id BIGINT NOT NULL,
    parameters JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (realm_id, target_catalog_id, target_id, policy_type_code, policy_catalog_id, policy_id)
);
CREATE TABLE IF NOT EXISTS events (
    realm_id TEXT NOT NULL,
    catalog_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    request_id TEXT,
    event_type TEXT NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    principal_name TEXT,
    resource_type TEXT NOT NULL,
    resource_identifier TEXT NOT NULL,
    additional_properties JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (event_id)
);
UPDATE version SET version_value = 3 WHERE version_key = 'version';
```

### Migration From Schema v1 to v2

```sql
ALTER TABLE entities ADD COLUMN IF NOT EXISTS location_without_scheme TEXT;
CREATE INDEX IF NOT EXISTS idx_locations
    ON entities USING btree (realm_id, parent_id, location_without_scheme)
    WHERE location_without_scheme IS NOT NULL;
UPDATE version SET version_value = 2 WHERE version_key = 'version';
```

## Schema Version Reference

The table below maps each released Polaris version to the JDBC schema version it ships with:

| Schema version | Polaris version                                 | Notable schema changes                                                                                                                             | Full schema SQL             |
|----------------|-------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------|
| v1             | 1.0.0&#8209;incubating                          | Initial schema                                                                                                                                     | [PG][v1-pg]                 |
| v2             | 1.1.0&#8209;incubating                          | Added `entities.location_without_scheme` column and `idx_locations` index                                                                          | [PG][v2-pg]                 |
| v3             | 1.2.0&#8209;incubating – 1.3.0&#8209;incubating | Added `events` table                                                                                                                               | [PG][v3-pg]                 |
| v4             | 1.4.0 – 1.6.0                                   | Added `scan_metrics_report` and `commit_metrics_report` tables; added `idempotency_records` table; added indexes on `entities` and `grant_records` | [PG][v4-pg] · [CRD][v4-crd] |
| v5             | 1.7.0                                           | `events.catalog_id` made nullable; `idempotency_records` removed                                                                                   | [PG][v5-pg] · [CRD][v5-crd] |
| v6             | In Development                                  | Switched to single `schema.sql` script; changed `idx_locations` index definition                                                                   |                             |

Since schema changes are versioned in git, you can use regular diff tools to compare the two files
and see the differences between two release tags. Polaris release git tags are of the form:
`apache-polaris-<version>`, e.g. `apache-polaris-1.8.0`.

For example, to get the diff between the PostgreSQL schema SQL file between Polaris 1.8.0 and 1.9.0,
you can run the following command:

```bash
git diff apache-polaris-1.8.0 apache-polaris-1.9.0 -- persistence/relational-jdbc/src/main/resources/postgres/schema.sql
```

{{< alert important >}}
Git diffs of `schema.sql` are only possible starting from schema v6 (Polaris 1.8.0).
{{< /alert >}}

[Quarkus configuration reference]: https://quarkus.io/guides/config-reference
[Quarkus datasource documentation]: https://quarkus.io/guides/datasource

[v1-pg]: https://github.com/apache/polaris/blob/apache-polaris-1.0.0-incubating/persistence/relational-jdbc/src/main/resources/postgres/schema-v1.sql
[v2-pg]: https://github.com/apache/polaris/blob/apache-polaris-1.1.0-incubating/persistence/relational-jdbc/src/main/resources/postgres/schema-v2.sql
[v3-pg]: https://github.com/apache/polaris/blob/apache-polaris-1.2.0-incubating/persistence/relational-jdbc/src/main/resources/postgres/schema-v3.sql
[v4-pg]: https://github.com/apache/polaris/blob/apache-polaris-1.4.0/persistence/relational-jdbc/src/main/resources/postgres/schema-v4.sql
[v4-crd]: https://github.com/apache/polaris/blob/apache-polaris-1.4.0/persistence/relational-jdbc/src/main/resources/cockroachdb/schema-v4.sql
[v5-pg]: https://github.com/apache/polaris/blob/apache-polaris-1.7.0/persistence/relational-jdbc/src/main/resources/postgres/schema-v5.sql
[v5-crd]: https://github.com/apache/polaris/blob/apache-polaris-1.7.0/persistence/relational-jdbc/src/main/resources/cockroachdb/schema-v5.sql
