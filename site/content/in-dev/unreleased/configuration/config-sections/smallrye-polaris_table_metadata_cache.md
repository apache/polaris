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
title: smallrye-polaris_table_metadata_cache
build:
  list: never
  render: never
---

Configuration for the in-memory cache of table metadata JSON documents, which serves table loads  without reading the metadata file from object storage.  Documents not accessed for an hour expire.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.table-metadata-cache.max-bytes` |  | `long` | Approximate upper bound on the heap used by cached table metadata JSON documents, in bytes,  counting each document's UTF-8 length plus an estimate of per-entry overhead.  Overrides the  fraction of the maximum heap size when set. Eviction may briefly lag writes, so budget this  cache with headroom alongside the other in-memory caches (such as the entity cache and `polaris.persistence.cache` ). Zero disables caching. |
| `polaris.table-metadata-cache.fraction-of-max-heap-size` | `0.05` | `double` | Fraction of the maximum heap size used as the cache budget when `max-bytes` is not set.  Unlike `polaris.persistence.cache`, the budget has no minimum size and no keep-free  adjustment, so it stays a small share of the heap on any heap size. Zero disables caching.  |
| `polaris.table-metadata-cache.max-bytes-per-entry` | `8388608` | `long` | Approximate upper bound on the heap used by a single cached document, in bytes, estimated the  same way as `max-bytes`. Defaults to 8 MiB. The effective bound is at most a tenth of the  cache budget. Larger documents are always read from storage so that one oversized document does  not evict the rest of the cache.  |
