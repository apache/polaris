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

The rules are defined using a [CEL  script ](https://github.com/projectnessie/cel-java/). The default rules for all kinds of objects are to retain the history for 3 days, for  the catalog state for 30 days.   

The scripts have access to the following declared values:   

 * `ref` (string) name of the reference    
 * `commits` (64-bit int) number of the currently processed commit, starting at `1` 
 * `ageDays` (64-bit int) age of currently processed commit in days    
 * `ageHours` (64-bit int) age of currently processed commit in hours    
 * `ageMinutes` (64-bit int) age of currently processed commit in minutes  

Scripts _must_ return a `boolean` yielding whether the commit shall be retained.  Note that maintenance-service implementations can keep the first not-to-be-retained commit.   

Example scripts   

 * `ageDays < 30 || commits <= 10` retains the reference history with at least 10        commits and commits that are younger than 30 days    
 * `true` retains the whole reference history    
 * `false` retains the most recent commit

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.persistence.nosql.maintenance.catalog.principals-retain` | `false` | `string` |  |
| `polaris.persistence.nosql.maintenance.catalog.principal-roles-retain` | `false` | `string` |  |
| `polaris.persistence.nosql.maintenance.catalog.grants-retain` | `false` | `string` |  |
| `polaris.persistence.nosql.maintenance.catalog.immediate-tasks-retain` | `false` | `string` |  |
| `polaris.persistence.nosql.maintenance.catalog.catalogs-history-retain` | `false` | `string` |  |
| `polaris.persistence.nosql.maintenance.catalog.catalog-roles-retain` | `false` | `string` |  |
| `polaris.persistence.nosql.maintenance.catalog.catalog-policies-retain` | `false` | `string` |  |
| `polaris.persistence.nosql.maintenance.catalog.catalog-state-retain` | `false` | `string` |  |
