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
title: "Apache Polaris 1.5.0"
linkTitle: "1.5.0"
release_version: "1.5.0"
release_date: "2026-05-18"
weight: -10500
hide_summary: true
exclude_search: false
type: downloads
menus:
  main:
    parent: releases
    weight: -10500
    identifier: releases-1.5.0
---

Released on May 18th, 2026.

### Downloads

| Artifact                                                                                                                                                                              | PGP Sig                                                                                                                            | SHA-512                                                                                                                                  |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| [source tar.gz](https://dlcdn.apache.org/polaris/1.5.0/apache-polaris-1.5.0.tar.gz)                                                                                                   | [.asc](https://dlcdn.apache.org/polaris/1.5.0/apache-polaris-1.5.0.tar.gz.asc)                                                     | [.sha512](https://dlcdn.apache.org/polaris/1.5.0/apache-polaris-1.5.0.tar.gz.sha512)                                                     |
| [binary tgz](https://dlcdn.apache.org/polaris/1.5.0/polaris-bin-1.5.0.tgz)                                                                                                            | [.asc](https://dlcdn.apache.org/polaris/1.5.0/polaris-bin-1.5.0.tgz.asc)                                                           | [.sha512](https://dlcdn.apache.org/polaris/1.5.0/polaris-bin-1.5.0.tgz.sha512)                                                           |
| [binary zip](https://dlcdn.apache.org/polaris/1.5.0/polaris-bin-1.5.0.zip)                                                                                                            | [.asc](https://dlcdn.apache.org/polaris/1.5.0/polaris-bin-1.5.0.zip.asc)                                                           | [.sha512](https://dlcdn.apache.org/polaris/1.5.0/polaris-bin-1.5.0.zip.sha512)                                                           |
| [Spark 3.5 with Scala 2.12 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.5.0/polaris-spark-3.5_2.12-1.5.0-bundle.jar)                        | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.5.0/polaris-spark-3.5_2.12-1.5.0-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.5.0/polaris-spark-3.5_2.12-1.5.0-bundle.jar.sha512) |
| [Spark 3.5 with Scala 2.13 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.5.0/polaris-spark-3.5_2.13-1.5.0-bundle.jar)                        | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.5.0/polaris-spark-3.5_2.13-1.5.0-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.5.0/polaris-spark-3.5_2.13-1.5.0-bundle.jar.sha512) |

### Release Notes

#### Upgrade notes

#### Breaking changes

- The `ConnectionCredentials.of()` method now throws an exception when more than one expiration timestamp property is present in the credentials map. Only a single expiration timestamp is allowed per credentials bundle.
- Entity names (namespaces, tables, views, generic tables) submitted to the REST layer are now rejected with HTTP 400 if they are empty, contain a `/`, or have leading/trailing whitespace. Clients that were previously able to create such entities must rename them before upgrading.
- `renameTable` now returns HTTP 204 (No Content) instead of 200, as required by the Iceberg REST Catalog spec. Clients that expect a 200 response must be updated.

#### New Features

- Added `envFrom` support in Helm chart.
- Added summarize subcommand to Polaris CLI.
- Added find and tables options to Polaris CLI.
- Added support for multiple event listeners. Set `polaris.event-listener.types` to a comma-separated list of event listener types to enable multiple event listeners.
- Added support for enabling only a subset of event types and event categories per event listener. Set `polaris.event-listener.`_`<name>`_`.enabled-event-types` or `polaris.event-listener.`_`<name>`_`.enabled-event-categories` to the list of event types or categories for the specified event listener to only consume the selected subset of events.
- Added support for **Apache Ranger** as an external authorizer (Beta).

#### Changes

- Improved Python CLI error messages and exit codes for invalid arguments and configuration errors.
- Removed unused `PolarisAuthorizableOperation` values: `REVOKE_PRINCIPAL_GRANT_FROM_PRINCIPAL_ROLE`, `REVOKE_PRINCIPAL_ROLE_GRANT_FROM_PRINCIPAL_ROLE`, `LIST_GRANTS_ON_ROOT`, `ADD_PRINCIPAL_GRANT_TO_PRINCIPAL_ROLE`, `LIST_GRANTS_ON_PRINCIPAL`, `ADD_PRINCIPAL_ROLE_GRANT_TO_PRINCIPAL_ROLE`, `LIST_GRANTS_ON_PRINCIPAL_ROLE`, `ADD_CATALOG_ROLE_GRANT_TO_CATALOG_ROLE`, `REVOKE_CATALOG_ROLE_GRANT_FROM_CATALOG_ROLE`, `LIST_GRANTS_ON_CATALOG_ROLE`, `LIST_GRANTS_ON_CATALOG`, `LIST_GRANTS_ON_NAMESPACE`, `LIST_GRANTS_ON_TABLE`, `LIST_GRANTS_ON_VIEW`.
- Changed deprecated APIs in JUnit 5. This change will force downstream projects that pull in the Polaris test packages to adopt JUnit 6.
- Added client id collision check during reset.
- The ExternalCatalogFactory interface has been renamed to FederatedCatalogFactory. Its createCatalog() and createGenericCatalog() method signatures have been extended to include a `catalogProperties` parameter of type `Map<String, String>` for passing through proxy and timeout settings to federated catalog HTTP clients.

#### Deprecations

- The configuration option `polaris.event-listener.type` is deprecated and will be removed later. Please use `polaris.event-listener.types` instead.
