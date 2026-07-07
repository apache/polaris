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
title: smallrye-polaris_persistence_relational_jdbc
build:
  list: never
  render: never
---

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.persistence.relational.jdbc.jdbc-url` |  | `string` | Optional JDBC URL for a Polaris-managed datasource. When set, Polaris creates the JDBC pool  directly instead of using the Quarkus datasource extension.  |
| `polaris.persistence.relational.jdbc.driver` |  | `string` | Optional JDBC driver class name for the Polaris-managed datasource.  |
| `polaris.persistence.relational.jdbc.driver-directory` |  | `string` | Optional directory of JDBC driver jars loaded for the Polaris-managed datasource. When unset,  Polaris looks for `jdbc-drivers` beside `quarkus-run.jar` in binary distributions.  |
| `polaris.persistence.relational.jdbc.username` |  | `string` | Optional username for the Polaris-managed datasource.  |
| `polaris.persistence.relational.jdbc.password` |  | `string` | Optional password for the Polaris-managed datasource.  |
| `polaris.persistence.relational.jdbc.maximum-pool-size` |  | `int` | Optional maximum connection pool size for the Polaris-managed datasource.  |
| `polaris.persistence.relational.jdbc.minimum-idle` |  | `int` | Optional minimum idle connection count for the Polaris-managed datasource.  |
| `polaris.persistence.relational.jdbc.connection-timeout-in-ms` |  | `long` | Optional connection timeout in milliseconds for the Polaris-managed datasource.  |
| `polaris.persistence.relational.jdbc.max-retries` |  | `int` |  |
| `polaris.persistence.relational.jdbc.max-duration-in-ms` |  | `long` |  |
| `polaris.persistence.relational.jdbc.initial-delay-in-ms` |  | `long` |  |
| `polaris.persistence.relational.jdbc.database-type` |  | `string` |  |
