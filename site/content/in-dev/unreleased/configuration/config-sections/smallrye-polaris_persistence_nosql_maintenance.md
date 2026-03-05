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
title: smallrye-polaris_persistence_nosql_maintenance
build:
  list: never
  render: never
---

Maintenance service configuration.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.persistence.nosql.maintenance.expected-reference-count` | `100` | `long` | Provides the expected number of references in all realms to retain, defaults to 100, must be at least `100`. This value is used as the  default if no information of a previous maintenance run is present, it is also the minimum  number of expected references.  |
| `polaris.persistence.nosql.maintenance.expected-obj-count` | `100000` | `long` | Provides the expected number of objects in all realms to retain, defaults to 100000, must be at least `100000`. This value is used as the  default if no information of a previous maintenance run is present, it is also the minimum  number of expected objects.  |
| `polaris.persistence.nosql.maintenance.count-from-last-run-multiplier` | `1.1` | `double` | Maintenance service sizes the bloom-filters used to hold the identified references and objects  according to the expression `lastRun.numberOfIdentified * countFromLastRunMultiplier`. The default is to add 10% to the number of identified items. |
| `polaris.persistence.nosql.maintenance.filter-initialized-fpp` | `1.0E-5` | `double` | False-positive-probability (FPP) used to initialize the bloom-filters for identified references  and objects.   |
| `polaris.persistence.nosql.maintenance.max-acceptable-filter-fpp` | `5.0E-5` | `double` | Expected maximum false-positive-probability (FPP) used to check the bloom-filters for  identified references and objects.  <br><br>If the FPP of a bloom filter exceeds this value, no individual references or objects will be  purged.  |
| `polaris.persistence.nosql.maintenance.retained-runs` | `50` | `int` | Number of retained maintenance run objects (MaintenanceRunInformation), must be at  least `2`.  |
| `polaris.persistence.nosql.maintenance.created-at-grace-time` | `PT3H` | `duration` | Objects and references that have been created _after_ a maintenance run has started are  never purged.  This option defines an additional grace time to when the maintenance run has  started.   <br><br>This value is a safety net for two reasons:   <br><br> * Respect the wall-clock drift between Polaris nodes.    <br> * Respect the order of writes in Polaris persistence. Objects are written _before_ those become reachable via a commit. Commits may take a little time (milliseconds, up to        a few seconds, depending on the system load) to complete. Therefore, implementations        enforce a minimum of 5 minutes.  <br><br> |
| `polaris.persistence.nosql.maintenance.object-scan-rate-limit-per-second` |  | `int` | Optionally limit the number of objects scanned per second. Default is to not throttle object  scanning.  |
| `polaris.persistence.nosql.maintenance.reference-scan-rate-limit-per-second` |  | `int` | Optionally limit the number of references scanned per second. <br><br>Default is to not throttle reference scanning. |
| `polaris.persistence.nosql.maintenance.delete-batch-size` | `10` | `int` | Size of the delete-batches when purging objects.  |
