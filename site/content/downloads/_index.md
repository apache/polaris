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
weight: 200
toc_hide: true
hide_summary: true
exclude_search: true
cascade:
  type: docs
params:
  show_page_toc: true 
---

## Helm Chart
Repo: https://downloads.apache.org/incubator/polaris/helm-chart

## 1.0.0 release
| Artifact                                                                                                                                                                                         | PGP Sig                                                                                                                                                  | SHA-512 |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---|
| [source tar.gz](https://downloads.apache.org/incubator/polaris/1.0.0-incubating/apache-polaris-1.0.0-incubating.tar.gz)                                                              | [.asc](https://downloads.apache.org/incubator/polaris/1.0.0-incubating/apache-polaris-1.0.0-incubating.tar.gz.asc)                                       | [.sha512](https://downloads.apache.org/incubator/polaris/1.0.0-incubating/apache-polaris-1.0.0-incubating.tar.gz.sha512) |
| [binary tgz](https://downloads.apache.org/incubator/polaris/1.0.0-incubating/polaris-bin-1.0.0-incubating.tgz)                                                                       | [.asc](https://downloads.apache.org/incubator/polaris/1.0.0-incubating/polaris-bin-1.0.0-incubating.tgz.asc)                                             | [.sha512](https://downloads.apache.org/incubator/polaris/1.0.0-incubating/polaris-bin-1.0.0-incubating.tgz.sha512) |
| [binary zip](https://downloads.apache.org/incubator/polaris/1.0.0-incubating/polaris-bin-1.0.0-incubating.zip)                                                                       | [.asc](https://downloads.apache.org/incubator/polaris/1.0.0-incubating/polaris-bin-1.0.0-incubating.zip.asc)                                             | [.sha512](https://downloads.apache.org/incubator/polaris/1.0.0-incubating/polaris-bin-1.0.0-incubating.zip.sha512) |
| [Spark 3.5 with Scala 2.12 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.0.0-incubating/polaris-spark-3.5_2.12-1.0.0-incubating-bundle.jar) | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.0.0-incubating/polaris-spark-3.5_2.12-1.0.0-incubating-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.0.0-incubating/polaris-spark-3.5_2.12-1.0.0-incubating-bundle.jar.sha512) |
| [Spark 3.5 with Scala 2.13 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.0.0-incubating/polaris-spark-3.5_2.13-1.0.0-incubating-bundle.jar) | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.0.0-incubating/polaris-spark-3.5_2.13-1.0.0-incubating-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.0.0-incubating/polaris-spark-3.5_2.13-1.0.0-incubating-bundle.jar.sha512) |

Apache Polaris 1.0.0-incubating was released on July 9th, 2025.
- **Highlights**
    - First release ready for real-world workloads after the public beta 0.9.0
    - **Binary distribution** -- first release with single downloadable .tgz or .zip artifact.
    - **Helm Chart** – debut of an official Helm chart for seamless Kubernetes deployment
- **New features & enhancements**
    - **Policy Store** — persistence with schema evolution, built‑in TMS policies (Data compaction, Snapshot expiry, etc) and REST CRUD endpoints
    - **Postgres JDBC persistence** — native JDBC backend with robust support for concurrent changes.
    - **Rollback Compaction on Conflicts** - makes Polaris smarter, to revert the compaction commits in case of crunch to let the writers who are actually adding or removing the data to the table succeed. In a sense treating compaction as always a lower priority process.
    - **Enhanced runtime** — new runtime powered by Quarkus delivers out‑of‑the‑box Kubernetes readiness, quick startup, OIDC integration, and many other benefits. Polaris server and admin tool are now using Quarkus as a runtime framework.
    - **HTTP caching via ETag** — the loadTable endpoint supports ETag, reducing bandwidth and improving perceived latency
    - **Support for external identity providers (IdP)** — Polaris can now be its own IdP, delegate to an external IdP, or both
    - **Snapshot filtering** – clients can choose to load only referenced snapshots
    - **Catalog Federation (experimental)** – federate requests to an external Iceberg REST or Hadoop Catalog
    - **Generic Tables (experimental)** — serve multiple table formats besides Iceberg tables; initial Spark 3.5 plugin supports Delta Lake
    - **Event Listener framework (experimental)** — subscribe to catalog events (AfterTableCommitedEvent, BeforeViewCommitedEvent, etc)
- **Notable bug fixes**
    - **CLI and Python Client improvements** – Support for new features, CLI repair, changes to the update subcommand, and various fixes
    - **Safe configurations** – Catalog-level Polaris configurations follow a strict naming convention to avoid name clashes with user-provided configuration entries. Legacy Polaris configuration names are still supported in 1.0 to allow existing deployments to migrate without rush.
    - **TableOperations optimizations** – Changes to BasePolarisTableOperations result in less traffic to object storage during commits
    - **Bounded entity cache** – The entity cache is now more memory-aware and less likely to lead to OOMs
    - **Bootstrapping fixes** – Users can more easily bootstrap a new realm. Root credentials can be provided by the user or generated by Polaris (and returned to the user).
- **Breaking changes**
    - **Server Configuration** – The format used to configure the Polaris service in 0.9 has changed with the migration to Quarkus and changes to configurations
    - **Bootstrap Flow** – The bootstrap flow used in 0.9 has changed with the migration to Quarkus and the new admin tool

## 0.9.0 release

| Artifact                                                                                                                                 | PGP Sig | SHA-512 |
|------------------------------------------------------------------------------------------------------------------------------------------|---|---|
| [0.9.0-incubating source tar.gz](https://downloads.apache.org/incubator/polaris/0.9.0-incubating/apache-polaris-0.9.0-incubating.tar.gz) | [.asc](https://downloads.apache.org/incubator/polaris/0.9.0-incubating/apache-polaris-0.9.0-incubating.tar.gz.asc) | [.sha512](https://downloads.apache.org/incubator/polaris/0.9.0-incubating/apache-polaris-0.9.0-incubating.tar.gz.sha512) |

Apache Polaris 0.9.0 was released on March 11, 2025 as the first Polaris release. Only the source distribution is available for this release. 
