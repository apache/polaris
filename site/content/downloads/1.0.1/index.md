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
title: "Apache Polaris 1.0.1-incubating"
linkTitle: "1.0.1"
release_version: "1.0.1"
weight: 980
hide_summary: true
exclude_search: true
type: downloads
menus:
  main:
    parent: downloads
    weight: 980
    identifier: downloads-1.0.1
---

Released on August 16th, 2025.

### Downloads

| Artifact                                                                                                                                                                                         | PGP Sig                                                                                                                                                  | SHA-512 |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---|
| [source tar.gz](https://archive.apache.org/dist/incubator/polaris/1.0.1-incubating/apache-polaris-1.0.1-incubating.tar.gz)                                                              | [.asc](https://archive.apache.org/dist/incubator/polaris/1.0.1-incubating/apache-polaris-1.0.1-incubating.tar.gz.asc)                                       | [.sha512](https://archive.apache.org/dist/incubator/polaris/1.0.1-incubating/apache-polaris-1.0.1-incubating.tar.gz.sha512) |
| [binary tgz](https://archive.apache.org/dist/incubator/polaris/1.0.1-incubating/polaris-bin-1.0.1-incubating.tgz)                                                                       | [.asc](https://archive.apache.org/dist/incubator/polaris/1.0.1-incubating/polaris-bin-1.0.1-incubating.tgz.asc)                                             | [.sha512](https://archive.apache.org/dist/incubator/polaris/1.0.1-incubating/polaris-bin-1.0.1-incubating.tgz.sha512) |
| [binary zip](https://archive.apache.org/dist/incubator/polaris/1.0.1-incubating/polaris-bin-1.0.1-incubating.zip)                                                                       | [.asc](https://downloads.apache.org/incubator/polaris/1.0.1-incubating/polaris-bin-1.0.1-incubating.zip.asc)                                             | [.sha512](https://downloads.apache.org/incubator/polaris/1.0.1-incubating/polaris-bin-1.0.1-incubating.zip.sha512) |
| [Spark 3.5 with Scala 2.12 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.0.1-incubating/polaris-spark-3.5_2.12-1.0.1-incubating-bundle.jar) | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.0.1-incubating/polaris-spark-3.5_2.12-1.0.1-incubating-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.0.1-incubating/polaris-spark-3.5_2.12-1.0.1-incubating-bundle.jar.sha512) |
| [Spark 3.5 with Scala 2.13 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.0.1-incubating/polaris-spark-3.5_2.13-1.0.1-incubating-bundle.jar) | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.0.1-incubating/polaris-spark-3.5_2.13-1.0.1-incubating-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.0.1-incubating/polaris-spark-3.5_2.13-1.0.1-incubating-bundle.jar.sha512) |

### Release Notes

This is a maintenance release on the 1.0.0 release fixing a couple of issues on the Helm Chart:
- remove db-kind in Helm Chart
- add relational-jdbc to helm

