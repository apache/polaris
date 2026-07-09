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
title: smallrye-polaris_persistence_nosql_mongodb
build:
  list: never
  render: never
---

Polaris persistence, MongoDB backend specific configuration.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.persistence.nosql.mongodb.connection-string` |  | `string` |  |
| `polaris.persistence.nosql.mongodb.database-name` |  | `string` |  |
| `polaris.persistence.nosql.mongodb.allow-prefix-deletion` |  | `boolean` | Optionally enable realm-deletion using a prefix-delete. <br><br>Prefix-deletion is disabled by default. |
