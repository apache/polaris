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
title: "Apache Polaris 1.3.0-incubating"
linkTitle: "1.3.0"
release_version: "1.3.0"
weight: 950
hide_summary: true
exclude_search: true
type: downloads
menus:
  main:
    parent: downloads
    weight: 950
    identifier: downloads-1.3.0
---

Released on January 16th, 2026.

### Downloads

| Artifact                                                                                                                                                                             | PGP Sig                                                                                                                                                  | SHA-512                                                                                                                                                        |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [source tar.gz](https://dlcdn.apache.org/incubator/polaris/1.3.0-incubating/apache-polaris-1.3.0-incubating.tar.gz)                                                                  | [.asc](https://downloads.apache.org/incubator/polaris/1.3.0-incubating/apache-polaris-1.3.0-incubating.tar.gz.asc)                                       | [.sha512](https://downloads.apache.org/incubator/polaris/1.3.0-incubating/apache-polaris-1.3.0-incubating.tar.gz.sha512)                                       |
| [binary tgz](https://dlcdn.apache.org/incubator/polaris/1.3.0-incubating/polaris-bin-1.3.0-incubating.tgz)                                                                           | [.asc](https://downloads.apache.org/incubator/polaris/1.3.0-incubating/polaris-bin-1.3.0-incubating.tgz.asc)                                             | [.sha512](https://downloads.apache.org/incubator/polaris/1.3.0-incubating/polaris-bin-1.3.0-incubating.tgz.sha512)                                             |
| [binary zip](https://dlcdn.apache.org/incubator/polaris/1.3.0-incubating/polaris-bin-1.3.0-incubating.zip)                                                                           | [.asc](https://downloads.apache.org/incubator/polaris/1.3.0-incubating/polaris-bin-1.3.0-incubating.zip.asc)                                             | [.sha512](https://downloads.apache.org/incubator/polaris/1.3.0-incubating/polaris-bin-1.3.0-incubating.zip.sha512)                                             |
| [Spark 3.5 with Scala 2.12 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.3.0-incubating/polaris-spark-3.5_2.12-1.3.0-incubating-bundle.jar) | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.3.0-incubating/polaris-spark-3.5_2.12-1.3.0-incubating-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.3.0-incubating/polaris-spark-3.5_2.12-1.3.0-incubating-bundle.jar.sha512) |
| [Spark 3.5 with Scala 2.13 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.3.0-incubating/polaris-spark-3.5_2.13-1.3.0-incubating-bundle.jar) | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.3.0-incubating/polaris-spark-3.5_2.13-1.3.0-incubating-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.3.0-incubating/polaris-spark-3.5_2.13-1.3.0-incubating-bundle.jar.sha512) |

### Release Notes

#### Highlights
- Support for [Iceberg Metrics Reporting] has been introduced in Polaris. Out of the box, metrics can be printed to the logs by setting the `org.apache.polaris.service.reporting` logger level to `INFO` (it's set to `OFF` by default). Custom reporters can be implemented and configured to send metrics to external systems for further analysis and monitoring.
- Support for [Open Policy Agent (OPA)] integration has been added to Polaris. This enables delegating authorization decisions to external policy decision points, allowing organizations to centralize policy management and implement complex authorization rules. OPA integration can be enabled by setting `polaris.authorization.type=opa` in the Polaris configuration.

#### Upgrade Notes
- The legacy management endpoints at `/metrics` and `/healthcheck` have been removed. Please use the standard management endpoints at `/q/metrics` and `/q/health` instead.

#### Breaking changes
- The EclipseLink Persistence implementation has been completely removed.
- The default request ID header name has changed from `Polaris-Request-Id` to `X-Request-ID`.

#### New Features
- Added `--no-sts` flag to CLI to support S3-compatible storage systems that do not have Security Token Service available.
- Support credential vending for federated catalogs. `ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING` (default: true) was added to toggle this feature.
- Enhanced catalog federation with SigV4 authentication support, additional authentication types for credential vending, and location-based access restrictions to block credential vending for remote tables outside allowed location lists.

#### Changes
- `client.region` is no longer considered a "credential" property (related to Iceberg REST Catalog API).
- Relaxed the requirements for S3 storage's ARN to allow Polaris to connect to more non-AWS S3 storage appliances.
- Added checksum to helm deployment so that it will restart when the configmap has changed.
- Generic Table is no longer in beta and is generally-available.
- Added Windows support for Python client

[Iceberg Metrics Reporting]: https://iceberg.apache.org/docs/latest/metrics-reporting/
[Open Policy Agent (OPA)]: https://www.openpolicyagent.org/

