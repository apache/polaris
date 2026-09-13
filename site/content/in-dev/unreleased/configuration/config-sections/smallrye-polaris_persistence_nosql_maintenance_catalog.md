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

(`#retention()`) provides independent controls for each kind of history. Count and  duration controls are combined, retaining commits required by either one. The per-history  controls default to one commit, zero duration, and retain-all disabled.   

(`#minRetentionDuration()`) provides a global minimum retention duration for all kinds of  history.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.persistence.nosql.maintenance.catalog.min-retention-duration` | `PT0S` | `duration` | Minimum duration to retain commits for all kinds of history. This is combined with each  per-history duration, retaining commits required by either setting.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.principals.num-commits` | `1` | `int` | Minimum number of latest commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.principals.duration` | `PT0S` | `duration` | Minimum duration to retain commits after they are superseded.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.principals.all` | `false` | `boolean` | Whether to retain all commits.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.principal-roles.num-commits` | `1` | `int` | Minimum number of latest commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.principal-roles.duration` | `PT0S` | `duration` | Minimum duration to retain commits after they are superseded.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.principal-roles.all` | `false` | `boolean` | Whether to retain all commits.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.grants.num-commits` | `1` | `int` | Minimum number of latest commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.grants.duration` | `PT0S` | `duration` | Minimum duration to retain commits after they are superseded.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.grants.all` | `false` | `boolean` | Whether to retain all commits.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.immediate-tasks.num-commits` | `1` | `int` | Minimum number of latest commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.immediate-tasks.duration` | `PT0S` | `duration` | Minimum duration to retain commits after they are superseded.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.immediate-tasks.all` | `false` | `boolean` | Whether to retain all commits.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.catalogs-history.num-commits` | `1` | `int` | Minimum number of latest commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.catalogs-history.duration` | `PT0S` | `duration` | Minimum duration to retain commits after they are superseded.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.catalogs-history.all` | `false` | `boolean` | Whether to retain all commits.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.catalog-roles.num-commits` | `1` | `int` | Minimum number of latest commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.catalog-roles.duration` | `PT0S` | `duration` | Minimum duration to retain commits after they are superseded.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.catalog-roles.all` | `false` | `boolean` | Whether to retain all commits.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.catalog-policies.num-commits` | `1` | `int` | Minimum number of latest commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.catalog-policies.duration` | `PT0S` | `duration` | Minimum duration to retain commits after they are superseded.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.catalog-policies.all` | `false` | `boolean` | Whether to retain all commits.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.catalog-state.num-commits` | `1` | `int` | Minimum number of latest commits to retain.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.catalog-state.duration` | `PT0S` | `duration` | Minimum duration to retain commits after they are superseded.  |
| `polaris.persistence.nosql.maintenance.catalog.retention.catalog-state.all` | `false` | `boolean` | Whether to retain all commits.  |
