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
title: "1.1.0"
linkTitle: "1.1.0"
weight: 970
hide_summary: true
exclude_search: true
type: docs
menus:
  main:
    parent: downloads
    weight: 970
    identifier: downloads-1.1.0
---

## Apache Polaris 1.1.0-incubating

Released on September 19th, 2025.

### Downloads

| Artifact                                                                                                                                                                                         | PGP Sig                                                                                                                                                  | SHA-512 |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---|
| [source tar.gz](https://archive.apache.org/dist/incubator/polaris/1.1.0-incubating/apache-polaris-1.1.0-incubating.tar.gz)                                                              | [.asc](https://archive.apache.org/dist/incubator/polaris/1.1.0-incubating/apache-polaris-1.1.0-incubating.tar.gz.asc)                                       | [.sha512](https://archive.apache.org/dist/incubator/polaris/1.1.0-incubating/apache-polaris-1.1.0-incubating.tar.gz.sha512) |
| [binary tgz](https://archive.apache.org/dist/incubator/polaris/1.1.0-incubating/polaris-bin-1.1.0-incubating.tgz)                                                                       | [.asc](https://archive.apache.org/dist/incubator/polaris/1.1.0-incubating/polaris-bin-1.1.0-incubating.tgz.asc)                                             | [.sha512](https://archive.apache.org/dist/incubator/polaris/1.1.0-incubating/polaris-bin-1.1.0-incubating.tgz.sha512) |
| [binary zip](https://archive.apache.org/dist/incubator/polaris/1.1.0-incubating/polaris-bin-1.1.0-incubating.zip)                                                                       | [.asc](https://archive.apache.org/dist/incubator/polaris/1.1.0-incubating/polaris-bin-1.1.0-incubating.zip.asc)                                             | [.sha512](https://archive.apache.org/dist/incubator/polaris/1.1.0-incubating/polaris-bin-1.1.0-incubating.zip.sha512) |
| [Spark 3.5 with Scala 2.12 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.1.0-incubating/polaris-spark-3.5_2.12-1.1.0-incubating-bundle.jar) | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.1.0-incubating/polaris-spark-3.5_2.12-1.1.0-incubating-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.1.0-incubating/polaris-spark-3.5_2.12-1.1.0-incubating-bundle.jar.sha512) |
| [Spark 3.5 with Scala 2.13 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.1.0-incubating/polaris-spark-3.5_2.13-1.1.0-incubating-bundle.jar) | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.1.0-incubating/polaris-spark-3.5_2.13-1.1.0-incubating-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.1.0-incubating/polaris-spark-3.5_2.13-1.1.0-incubating-bundle.jar.sha512) |

### Release Notes

#### New features & enhancements
- HMS support
- IMPLICIT authentication type
- Support for non-AWS S3 compatible storage with STS: MinIO, s3a scheme support
- Use of Realm instead of RealmId
- Modularized Federation Architecture
- Federated Catalog Support in Polaris CLI
- Expanded External Identity Provider support
- Python package (official)
- Documentation improvements (release process, multi-realms configuration)

#### Bug fixes
- Fix drop view with default server configuration
- Fix MinIO support
- Remove ThreadLocal

#### Breaking changes
- Helm chart: the default value of the `authentication.tokenBroker.secret.symmetricKey.secretKey` property has changed 
  from `symmetric.pem` to `symmetric.key`.
- For migrations from 1.0.x to 1.1.x, users using JDBC persistence and wanting to continue using v1 schema, must ensure
  that they, run following SQL statement under `POLARIS_SCHEMA` to make sure version table exists:
  ```sql 
  CREATE TABLE IF NOT EXISTS version (
     version_key TEXT PRIMARY KEY,
     version_value INTEGER NOT NULL
  );
  INSERT INTO version (version_key, version_value)
    VALUES ('version', 1)
  ON CONFLICT (version_key) DO UPDATE
                          SET version_value = EXCLUDED.version_value;
  COMMENT ON TABLE version IS 'the version of the JDBC schema in use';
  
  ALTER TABLE polaris_schema.entities ADD COLUMN IF NOT EXISTS location_without_scheme TEXT;
  ```
  - Please don't enable [OPTIMIZED_SIBLING_CHECK](https://github.com/apache/polaris/blob/740993963cb41c2c1b4638be5e04dd00f1263c98/polaris-core/src/main/java/org/apache/polaris/core/config/FeatureConfiguration.java#L346) feature configuration, once the above SQL statements are run. As it may lead to incorrect behavior, due to missing data for location_without_scheme column.

