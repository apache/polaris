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
| `polaris.persistence.retry.timeout` | `PT15S` | `duration` | Maximum time a retry loop allows `Committer#commit(CommitRetryable)` to keep retrying before failing with `RetryTimeoutException`. |
| `polaris.persistence.retry.retries` | `10000` | `int` | Maximum number of retries allowed. |
| `polaris.persistence.retry.initial-sleep-lower` | `PT0.010S` | `duration` | Initial lower bound for the retry-loop sleep duration. This value is doubled after each retry until `RetryConfig#maxSleep()` is reached. The actual sleep duration is chosen randomly between the current lower and upper bounds. |
| `polaris.persistence.retry.initial-sleep-upper` | `PT0.020S` | `duration` | Initial upper bound for the retry-loop sleep duration. This value is doubled after each retry until `RetryConfig#maxSleep()` is reached. The actual sleep duration is chosen randomly between the current lower and upper bounds. |
| `polaris.persistence.retry.max-sleep` | `PT0.250S` | `duration` | Maximum retry-loop sleep duration. |
| `polaris.persistence.retry.fair-retries` | `SLEEPING` | `enum (UNFAIR, SLEEPING)` | Without mitigation, frequently started retry loops running against highly contended resources can cause some retry-loop invocations to make no progress and eventually time out. <br><br>The default fair-retries type helps in these scenarios while sacrificing overall throughput as little as possible. |
| `polaris.persistence.bucketized-bulk-fetch-size` | `16` | `int` | The number of objects to fetch at once via `Persistence#bucketizedBulkFetches(Stream, Class)`. |
| `polaris.persistence.max-serialized-value-size` | `350k` | `MemorySize` | The maximum size of a serialized value in a persisted database row.  |
