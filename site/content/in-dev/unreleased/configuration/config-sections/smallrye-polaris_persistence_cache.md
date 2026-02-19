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
title: smallrye-polaris_persistence_cache
build:
  list: never
  render: never
---

Persistence cache configuration.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.persistence.cache.enable` | `true` | `boolean` | Optionally disable the cache, the default value is `true`, meaning that the cache is  _enabled_ by default.   |
| `polaris.persistence.cache.reference-ttl` | `PT15M` | `duration` | Duration to cache the state of references.  |
| `polaris.persistence.cache.reference-negative-ttl` |  | `duration` | Duration to cache whether a reference does _not_ exist (negative caching).   |
| `polaris.persistence.cache.sizing.fraction-of-max-heap-size` |  | `double` | Fraction of Java’s max heap size to use for cache objects, set to 0 to disable. Must not be  used with fixed cache sizing. If neither this value nor a fixed size is configured, a default  of `.4` (40%) is assumed, if `enable-soft-references` is enabled, else `.6` (60%) is assumed.  |
| `polaris.persistence.cache.sizing.fraction-min-size` | `64M` | `MemorySize` | When using fractional cache sizing, this amount in MB is the minimum cache size.  |
| `polaris.persistence.cache.sizing.fraction-adjustment` | `256M` | `MemorySize` | When using fractional cache sizing, this amount in MB of the heap will always be "kept free"  when calculating the cache size.   |
| `polaris.persistence.cache.sizing.fixed-size` |  | `MemorySize` | Capacity of the persistence cache in MiB.  |
| `polaris.persistence.cache.sizing.cache-capacity-overshoot` | `0.1` | `double` | Admitted cache-capacity-overshoot fraction, defaults to `0.1` (10 %).  <br><br>New elements are admitted to be added to the cache, if the cache's size is less than `cache-capacity * (1 + cache-capacity-overshoot` .   <br><br>Cache eviction happens asynchronously. Situations when eviction cannot keep up with the  amount of data added could lead to out-of-memory situations.   <br><br>The value, if present, must be greater than 0. |
