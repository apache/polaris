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
title: smallrye-polaris_persistence_metrics
build:
  list: never
  render: never
---

Configuration for selecting the (`org.apache.polaris.core.persistence.metrics.MetricsPersistence`) implementation.  

This configuration allows selecting the metrics persistence backend independently from the  entity metastore. Available types include:   

 * `noop` (default) - No persistence, metrics are discarded    
 * `relational-jdbc` - Persists metrics to the JDBC database (requires metrics schema)

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.persistence.metrics.type` | `noop` | `string` | The type of the metrics persistence to use. Must be a registered (`org.apache.polaris.core.persistence.metrics.MetricsPersistence`) identifier.   <br><br>Defaults to `noop` which discards all metrics.  |
