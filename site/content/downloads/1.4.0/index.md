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
title: "Apache Polaris 1.4.0"
linkTitle: "1.4.0"
release_version: "1.4.0"
release_date: "2026-04-21"
weight: -10400
hide_summary: true
exclude_search: false
type: downloads
menus:
  main:
    parent: releases
    weight: -10400
    identifier: releases-1.4.0
---

Released on April 21st, 2026.

### Downloads

| Artifact                                                                                                                                                                              | PGP Sig                                                                                                                            | SHA-512                                                                                                                                  |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| [source tar.gz](https://dlcdn.apache.org/polaris/1.4.0/apache-polaris-1.4.0.tar.gz)                                                                                                   | [.asc](https://dlcdn.apache.org/polaris/1.4.0/apache-polaris-1.4.0.tar.gz.asc)                                                     | [.sha512](https://dlcdn.apache.org/polaris/1.4.0/apache-polaris-1.4.0.tar.gz.sha512)                                                     |
| [binary tgz](https://dlcdn.apache.org/polaris/1.4.0/polaris-bin-1.4.0.tgz)                                                                                                            | [.asc](https://dlcdn.apache.org/polaris/1.4.0/polaris-bin-1.4.0.tgz.asc)                                                           | [.sha512](https://dlcdn.apache.org/polaris/1.4.0/polaris-bin-1.4.0.tgz.sha512)                                                           |
| [binary zip](https://dlcdn.apache.org/polaris/1.4.0/polaris-bin-1.4.0.zip)                                                                                                            | [.asc](https://dlcdn.apache.org/polaris/1.4.0/polaris-bin-1.4.0.zip.asc)                                                           | [.sha512](https://dlcdn.apache.org/polaris/1.4.0/polaris-bin-1.4.0.zip.sha512)                                                           |
| [Spark 3.5 with Scala 2.12 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.4.0/polaris-spark-3.5_2.12-1.4.0-bundle.jar)                        | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.4.0/polaris-spark-3.5_2.12-1.4.0-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.4.0/polaris-spark-3.5_2.12-1.4.0-bundle.jar.sha512) |
| [Spark 3.5 with Scala 2.13 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.4.0/polaris-spark-3.5_2.13-1.4.0-bundle.jar)                        | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.4.0/polaris-spark-3.5_2.13-1.4.0-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.4.0/polaris-spark-3.5_2.13-1.4.0-bundle.jar.sha512) |

### Release Notes

#### Upgrade notes

- The custom token-bucket based rate limiter has been replaced with Guava's rate limiter implementation.
- The Helm chart now includes a JSON schema file for easy validation of values files. Because types
  are now validated, existing values files may need to be updated to match the new schema.


#### Breaking changes

- The (Before/After)CommitViewEvent has been removed.
- The (Before/After)CommitTableEvent has been removed.
- The `PolarisMetricsReporter.reportMetric()` method signature has been extended to include a `receivedTimestamp` parameter of type `java.time.Instant`.
- The `ExternalCatalogFactory.createCatalog()` and `createGenericCatalog()` method signatures have been extended to include a `catalogProperties` parameter of type `Map<String, String>` for passing through proxy and timeout settings to federated catalog HTTP clients.
- Metrics reporting now requires the `TABLE_READ_DATA` privilege on the target table for read (scan) metrics and `TABLE_WRITE_DATA` for write (commit) metrics.
- The `REVOKE_CATALOG_ROLE_FROM_PRINCIPAL_ROLE` operation no longer requires the `PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE` privilege. Only `CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE` on the catalog role is now required, making revoke symmetric with assign. This allows catalog administrators to fully manage catalog role assignments without requiring elevated privileges on principal roles.

#### New Features

- Added `envFrom` support in Helm chart.
- Added `deploymentAnnotations` support in Helm chart.
- Added KMS properties (optional) to catalog storage config to enable S3 data encryption.
- Added `topologySpreadConstraints` support in Helm chart.
- Added `priorityClassName` support in Helm chart.
- Added support for including principal name in subscoped credentials. `INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL` (default: false) can be used to toggle this feature. If enabled, cached credentials issued to one principal will no longer be available for others.
- Added per-field selection for AWS STS session tags in credential vending. The new `SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL` configuration accepts a comma-separated list of fields to include as session tags (supported: `realm`, `catalog`, `namespace`, `table`, `principal`, `roles`, `trace_id`). This replaces the previous `INCLUDE_SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL` and `INCLUDE_TRACE_ID_IN_SESSION_TAGS` boolean flags. Selecting only the fields needed helps avoid AWS STS packed policy size limit errors (2048 characters) caused by long namespace paths. Note: including `trace_id` disables credential caching, which may increase STS calls and latency.
- Added support for [Kubernetes Gateway API](https://gateway-api.sigs.k8s.io/) to the Helm Chart.
- Added `hierarchical` flag to `AzureStorageConfigInfo` to allow more precise SAS token down-scoping in ADLS when
  the [hierarchical namespace](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-namespace)
  feature is enabled in Azure.
- Relaxed `client_id`, `client_secret` regex/pattern validation on reset endpoint call
- Added support for S3-compatible storage that does not have KMS (use `kmsUavailable: true` in catalog storage configuration)
- Added support for storage-scoped AWS credentials, allowing different AWS credentials to be configured per named storage. Enable with the `RESOLVE_CREDENTIALS_BY_STORAGE_NAME` feature flag (default: false). Storage names can be set explicitly via the `storageName` field on storage configuration, or inferred from the first allowed location's host.
- Added support for persisting Iceberg metrics (ScanReport, CommitReport) to the database. Enable by setting `polaris.iceberg-metrics.reporting.type=persisting` in configuration. Metrics tables are included in the main JDBC schema.
- Added setup options to Polaris CLI.
- Added CockroachDB as a supported database for the relational JDBC persistence backend. Set `polaris.persistence.relational.jdbc.database-type` to `cockroachdb` to get started.

#### Changes

- Changed deprecated APIs in JUnit 5. This change will force downstream projects that pull in the Polaris test packages to adopt JUnit 6.
- The `gcpServiceAccount` configuration value now affects Polaris behavior (enables service account impersonation). This value was previously defined but unused. This change may affect existing deployments that have populated this property.
- (Before/After)UpdateTableEvent is emitted for all table updates within a transaction.
- Added KMS options to Polaris CLI.
- Changed from Poetry to UV for Python package management.
- Exclude KMS policies when KMS is not being used for S3.
- Improved default KMS permission handling to better distinguish read-only and read-write access.

#### Deprecations

- The configuration option `polaris.rate-limiter.token-bucket.window` is no longer supported and should be removed.
- `PolarisConfigurationStore` has been deprecated for removal.

#### Fixes

- Fixed error propagation in drop operations (`dropTable`, `dropView`, `dropNamespace`). Server errors now return appropriate HTTP status codes based on persistence result instead of always returning NotFound
- Enable non-AWS STS role ARNs
- Helm chart: fixed a bug that prevented CORS settings to be properly applied. A new setting `cors.enabled` has been introduced in the chart as part of the fix.


