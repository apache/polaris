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
title: smallrye-polaris_persistence
build:
  list: never
  render: never
---

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.persistence.type` |  | `string` | The type of the persistence to use. Must be a registered (`org.apache.polaris.core.persistence.MetaStoreManagerFactory`) identifier.  |
| `polaris.persistence.auto-bootstrap-types` |  | `list of string` |  |
| `polaris.persistence.reference-previous-head-count` | `20` | `int` |  |
| `polaris.persistence.max-index-stripes` | `20` | `int` |  |
| `polaris.persistence.max-embedded-index-size` | `32k` | `MemorySize` |  |
| `polaris.persistence.max-index-stripe-size` | `128k` | `MemorySize` |  |
| `polaris.persistence.bucketized-bulk-fetch-size` | `16` | `int` | The number of objects to fetch at once via (`Persistence#bucketizedBulkFetches(Stream,<br> Class)`).  |
| `polaris.persistence.max-serialized-value-size` | `350k` | `MemorySize` | The maximum size of a serialized value in a persisted database row.  |
