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
title: smallrye-polaris_persistence_nosql_maintenance_catalog
build:
  list: never
  render: never
---

No SQL persistence implementation of Polaris stores a history of changes per kind of object  (principals, principal roles, grants, immediate tasks, catalog roles and catalog state).  

Each `*-retain` configuration property controls how many of the latest commits are  retained for one kind of history. All properties default to retaining only the latest commit.  (`#paginationTokenRetention()`) additionally keeps superseded snapshots available for  snapshot-based pagination.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.persistence.nosql.maintenance.catalog.pagination-token-retention` | `P30D` | `duration` | Minimum time to retain a container snapshot after it is superseded, so pagination tokens that  reference that snapshot can continue to be used.  <br><br>A zero duration disables time-based retention. |
| `polaris.persistence.nosql.maintenance.catalog.principals-retain` | `1` | `int` | Number of latest principal commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.principal-roles-retain` | `1` | `int` | Number of latest principal-role commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.grants-retain` | `1` | `int` | Number of latest realm-grant commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.immediate-tasks-retain` | `1` | `int` | Number of latest immediate-task commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.catalogs-history-retain` | `1` | `int` | Number of latest catalog-list commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.catalog-roles-retain` | `1` | `int` | Number of latest catalog-role commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.catalog-policies-retain` | `1` | `int` | Number of latest catalog-policy commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.catalog-state-retain` | `1` | `int` | Number of latest catalog-state commits to retain.  |
