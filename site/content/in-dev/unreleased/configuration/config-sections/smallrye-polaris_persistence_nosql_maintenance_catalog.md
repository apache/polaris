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

Each kind of history has independent controls for the minimum number of commits to retain, the  minimum retention duration, and whether to retain all commits. Count and duration controls are  combined, retaining commits required by either one. The per-history controls default to one  commit, zero duration, and retain-all disabled.   

(`#paginationTokenRetention()`) is a separate global minimum that additionally keeps  superseded snapshots available for snapshot-based pagination.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.persistence.nosql.maintenance.catalog.pagination-token-retention` | `P30D` | `duration` | Minimum time to retain a container snapshot after it is superseded, so pagination tokens that  reference that snapshot can continue to be used.  This minimum applies only to histories backed  by container snapshots that can be referenced by pagination tokens.   <br><br>A zero duration disables this pagination-token retention minimum. |
| `polaris.persistence.nosql.maintenance.catalog.principals-retain` | `1` | `int` | Number of latest principal commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.principals-retain-duration` | `PT0S` | `duration` | Minimum duration to retain principal commits after they are superseded.  |
| `polaris.persistence.nosql.maintenance.catalog.principals-retain-all` | `false` | `boolean` | Whether to retain all principal commits.  |
| `polaris.persistence.nosql.maintenance.catalog.principal-roles-retain` | `1` | `int` | Number of latest principal-role commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.principal-roles-retain-duration` | `PT0S` | `duration` | Minimum duration to retain principal-role commits after they are superseded.  |
| `polaris.persistence.nosql.maintenance.catalog.principal-roles-retain-all` | `false` | `boolean` | Whether to retain all principal-role commits.  |
| `polaris.persistence.nosql.maintenance.catalog.grants-retain` | `1` | `int` | Number of latest realm-grant commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.grants-retain-duration` | `PT0S` | `duration` | Minimum duration to retain realm-grant commits after they are superseded.  |
| `polaris.persistence.nosql.maintenance.catalog.grants-retain-all` | `false` | `boolean` | Whether to retain all realm-grant commits.  |
| `polaris.persistence.nosql.maintenance.catalog.immediate-tasks-retain` | `1` | `int` | Number of latest immediate-task commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.immediate-tasks-retain-duration` | `PT0S` | `duration` | Minimum duration to retain immediate-task commits after they are superseded.  |
| `polaris.persistence.nosql.maintenance.catalog.immediate-tasks-retain-all` | `false` | `boolean` | Whether to retain all immediate-task commits.  |
| `polaris.persistence.nosql.maintenance.catalog.catalogs-history-retain` | `1` | `int` | Number of latest catalog-list commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.catalogs-history-retain-duration` | `PT0S` | `duration` | Minimum duration to retain catalog-list commits after they are superseded.  |
| `polaris.persistence.nosql.maintenance.catalog.catalogs-history-retain-all` | `false` | `boolean` | Whether to retain all catalog-list commits.  |
| `polaris.persistence.nosql.maintenance.catalog.catalog-roles-retain` | `1` | `int` | Number of latest catalog-role commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.catalog-roles-retain-duration` | `PT0S` | `duration` | Minimum duration to retain catalog-role commits after they are superseded.  |
| `polaris.persistence.nosql.maintenance.catalog.catalog-roles-retain-all` | `false` | `boolean` | Whether to retain all catalog-role commits.  |
| `polaris.persistence.nosql.maintenance.catalog.catalog-policies-retain` | `1` | `int` | Number of latest catalog-policy commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.catalog-policies-retain-duration` | `PT0S` | `duration` | Minimum duration to retain catalog-policy commits after they are superseded.  |
| `polaris.persistence.nosql.maintenance.catalog.catalog-policies-retain-all` | `false` | `boolean` | Whether to retain all catalog-policy commits.  |
| `polaris.persistence.nosql.maintenance.catalog.catalog-state-retain` | `1` | `int` | Number of latest catalog-state commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.catalog-state-retain-duration` | `PT0S` | `duration` | Minimum duration to retain catalog-state commits after they are superseded.  |
| `polaris.persistence.nosql.maintenance.catalog.catalog-state-retain-all` | `false` | `boolean` | Whether to retain all catalog-state commits.  |
