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
title: "Apache Polaris 1.6.0"
linkTitle: "1.6.0"
release_version: "1.6.0"
release_date: "2026-07-08"
weight: -10600
hide_summary: true
exclude_search: false
type: downloads
menus:
  main:
    parent: releases
    weight: -10600
    identifier: releases-1.6.0
---

Released on July 8th, 2026.

### Downloads

| Artifact                                                                                                                                                                              | PGP Sig                                                                                                                            | SHA-512                                                                                                                                  |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| [source tar.gz](https://dlcdn.apache.org/polaris/1.6.0/apache-polaris-1.6.0.tar.gz)                                                                                                   | [.asc](https://dlcdn.apache.org/polaris/1.6.0/apache-polaris-1.6.0.tar.gz.asc)                                                     | [.sha512](https://dlcdn.apache.org/polaris/1.6.0/apache-polaris-1.6.0.tar.gz.sha512)                                                     |
| [binary tgz](https://dlcdn.apache.org/polaris/1.6.0/polaris-bin-1.6.0.tgz)                                                                                                            | [.asc](https://dlcdn.apache.org/polaris/1.6.0/polaris-bin-1.6.0.tgz.asc)                                                           | [.sha512](https://dlcdn.apache.org/polaris/1.6.0/polaris-bin-1.6.0.tgz.sha512)                                                           |
| [binary zip](https://dlcdn.apache.org/polaris/1.6.0/polaris-bin-1.6.0.zip)                                                                                                            | [.asc](https://dlcdn.apache.org/polaris/1.6.0/polaris-bin-1.6.0.zip.asc)                                                           | [.sha512](https://dlcdn.apache.org/polaris/1.6.0/polaris-bin-1.6.0.zip.sha512)                                                           |
| [Spark 3.5 with Scala 2.12 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.6.0/polaris-spark-3.5_2.12-1.6.0-bundle.jar)                        | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.6.0/polaris-spark-3.5_2.12-1.6.0-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.6.0/polaris-spark-3.5_2.12-1.6.0-bundle.jar.sha512) |
| [Spark 3.5 with Scala 2.13 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.6.0/polaris-spark-3.5_2.13-1.6.0-bundle.jar)                        | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.6.0/polaris-spark-3.5_2.13-1.6.0-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.6.0/polaris-spark-3.5_2.13-1.6.0-bundle.jar.sha512) |

### Release Notes

#### Upgrade notes

- Event listeners are now executed on a dedicated executor. **This executor does not propagate the original request's CDI context**; listeners that were improperly relying on that should instead manage their own CDI request scope from now on. Furthermore, two new configuration options were introduced to configure the executor: 
  - `polaris.event-listener.executor.pool-size` configures the thread pool size.
  - `polaris.event-listener.executor.queue-size` configures the queue size for pending events when all threads are busy.

#### Breaking changes

- The `MaintenanceService.performMaintenance()` signature now requires an explicit `OptionalLong overrideRunId` argument to supersede the latest unfinished maintenance run.
- The REST layer now enforces stricter validation for entity names (including namespaces, tables, views, and generic tables). Requests containing invalid names will be rejected with an HTTP 400 error. Existing clients should verify and rename entities before upgrading if their names fall into the following forbidden categories:
    - Empty strings
    - Names consisting solely of `.` or `..`
    - Names containing control (invisible) characters
    - Names with leading or trailing whitespace
    - Names containing any of these characters: <code>/\:*?"<>|#+`</code>

#### New Features

- Added `SESSION_NAME_FIELDS_IN_SUBSCOPED_CREDENTIAL` feature flag for AWS credential vending. Operators can now configure an ordered list of fields (`realm`, `catalog`, `namespace`, `table`, `principal`) to compose structured STS role session names (e.g. `p-acme-hr_catalog-employee-etl_writer`). Session names are sanitized and proportionally truncated to the AWS 64-character limit. When unset, existing `INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL` behaviour is preserved.
- Added `hostUsers` support in Helm chart.
- Added documentation for BigQuery Metastore Catalog federation. Build with `-PNonRESTCatalogs=BIGQUERY` to include the BigQueryMetastoreCatalog federation extension. See `site/content/in-dev/unreleased/federation/bigquery-metastore-federation.md`.
- Support for view registration has been added.
- Python client: added support for Python 3.14
- Added support for `register table` overwrite semantics in the Iceberg REST catalog flow (`overwrite=true`) for internal Polaris catalogs. With overwrite enabled, existing table pointers can be updated to a new metadata location while preserving default behavior for `overwrite=false`.
- Added `REGISTER_TABLE_OVERWRITE` authorization operation mapped to `TABLE_FULL_METADATA` for deterministic overwrite authorization.
- Added Polaris Spark 4.0 client.

#### Changes

- Added REPL support to Polaris CLI.
- The `nosql maintenance-run` admin command now rejects a new run when the latest recorded maintenance run is still unfinished, unless the operator explicitly passes `--supersede-run=<run-id>`.
- Added version option to Polaris CLI.
- The token broker now builds the JWT `Algorithm` and `JWTVerifier` once per realm in the `TokenBrokerFactory` and reuses them across requests, instead of rebuilding them on every `verify()`/`sign()` call on the request-scoped broker. For deployments using file-based symmetric secrets, the secret is now read once per realm (at first use) rather than on every JWT operation; rotating the on-disk secret requires a restart.
