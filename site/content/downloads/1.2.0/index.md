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
title: "1.2.0"
linkTitle: "1.2.0"
weight: 960
hide_summary: true
exclude_search: true
type: docs
menus:
  main:
    parent: releases
    weight: 960
    identifier: downloads-1.2.0
---

## Apache Polaris 1.2.0-incubating

Released on October 23rd, 2025.

### Downloads

| Artifact                                                                                                                                                                             | PGP Sig                                                                                                                                                  | SHA-512                                                                                                                                                        |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [source tar.gz](https://archive.apache.org/dist/incubator/polaris/1.2.0-incubating/apache-polaris-1.2.0-incubating.tar.gz)                                                           | [.asc](https://archive.apache.org/dist/incubator/polaris/1.2.0-incubating/apache-polaris-1.2.0-incubating.tar.gz.asc)                                    | [.sha512](https://archive.apache.org/dist/incubator/polaris/1.2.0-incubating/apache-polaris-1.2.0-incubating.tar.gz.sha512)                                    |
| [binary tgz](https://archive.apache.org/dist/incubator/polaris/1.2.0-incubating/polaris-bin-1.2.0-incubating.tgz)                                                                    | [.asc](https://archive.apache.org/dist/incubator/polaris/1.2.0-incubating/polaris-bin-1.2.0-incubating.tgz.asc)                                          | [.sha512](https://archive.apache.org/dist/incubator/polaris/1.2.0-incubating/polaris-bin-1.2.0-incubating.tgz.sha512)                                          |
| [binary zip](https://archive.apache.org/dist/incubator/polaris/1.2.0-incubating/polaris-bin-1.2.0-incubating.zip)                                                                    | [.asc](https://archive.apache.org/dist/incubator/polaris/1.2.0-incubating/polaris-bin-1.2.0-incubating.zip.asc)                                          | [.sha512](https://archive.apache.org/dist/incubator/polaris/1.2.0-incubating/polaris-bin-1.2.0-incubating.zip.sha512)                                           |
| [Spark 3.5 with Scala 2.12 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.2.0-incubating/polaris-spark-3.5_2.12-1.2.0-incubating-bundle.jar) | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.2.0-incubating/polaris-spark-3.5_2.12-1.2.0-incubating-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.2.0-incubating/polaris-spark-3.5_2.12-1.2.0-incubating-bundle.jar.sha512) |
| [Spark 3.5 with Scala 2.13 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.2.0-incubating/polaris-spark-3.5_2.13-1.2.0-incubating-bundle.jar) | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.2.0-incubating/polaris-spark-3.5_2.13-1.2.0-incubating-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.2.0-incubating/polaris-spark-3.5_2.13-1.2.0-incubating-bundle.jar.sha512) |

### Release Notes

#### Upgrade Notes
- Amazon RDS plugin enabled, this allows polaris to connect to AWS Aurora PostgreSQL using IAM authentication.

#### Breaking changes
- Creating or altering a namespace with a custom location outside its parent location is now prohibited by default. To restore the old behavior, set the `ALLOW_NAMESPACE_CUSTOM_LOCATION` flag to true.

#### New Features
- Added a finer grained authorization model for UpdateTable requests. Existing privileges continue to work for granting UpdateTable, such as `TABLE_WRITE_PROPERTIES`.
However, you can now instead grant privileges just for specific operations, such as `TABLE_ADD_SNAPSHOT`
- Added a Management API endpoint to reset principal credentials, controlled by the `ENABLE_CREDENTIAL_RESET` (default: true) feature flag.
- The `ENABLE_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS` was added to support sub-catalog (initially namespace and table) RBAC for federated catalogs.
  The setting can be configured on a per-catalog basis by setting the catalog property: `polaris.config.enable-sub-catalog-rbac-for-federated-catalogs`.
  The realm-level feature flag `ALLOW_SETTING_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS` (default: true) controls whether this functionality can be enabled or modified at the catalog level.
- Added support for S3-compatible storage that does not have STS (use `stsUnavailable: true` in catalog storage configuration)
- Added a Management API endpoint to reset principal credentials, controlled by the `ENABLE_CREDENTIAL_RESET` (default: true) feature flag.
- **Events Persistence (Preview)**: Introduced new event types and added support for persisting events to both Relational JDBC Persistence and AWS CloudWatch.

  **Note**: This is a preview feature. The persistence schema is subject to change in future releases, and previously stored event data MAY become unreadable (i.e., dropped) after an upgrade.

#### Changes
- The following APIs will now return the newly-created objects as part of the successful 201 response: createCatalog, createPrincipalRole, createCatalogRole.

#### Deprecations

- The property `polaris.active-roles-provider.type` is deprecated and has no effect anymore.
- The EclipseLink Persistence implementation has been deprecated since 1.0.0 and will be completely removed
in 1.3.0 or in 2.0.0 (whichever happens earlier).
- The legacy management endpoints at `/metrics` and `/healthcheck` have been deprecated in 1.2.0 and will be
completely removed in 1.3.0 or in 2.0.0 (whichever happens earlier). Please use the standard management
endpoints at `/q/metrics` and `/q/health` instead.

