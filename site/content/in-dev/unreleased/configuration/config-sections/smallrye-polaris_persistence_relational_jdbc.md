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
| `polaris.persistence.relational.jdbc.max-retries` |  | `int` | The maximum number of retries before giving up the operation.  |
| `polaris.persistence.relational.jdbc.max-duration-in-ms` |  | `long` | The maximum retry duration in milliseconds.  |
| `polaris.persistence.relational.jdbc.initial-delay-in-ms` |  | `long` | The initial retry delay.  |
| `polaris.persistence.relational.jdbc.database-type` |  | `string` | Explicitly configured database type. If not specified, the database type will be inferred from  the JDBC connection metadata. Supported values: "postgresql", "cockroachdb", "h2"  |
| `polaris.persistence.relational.jdbc.datasource` |  | `string` | The datasource name to use. Required. |
