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
title: smallrye-polaris_idempotency
build:
  list: never
  render: never
---

Deploy-time configuration for entity-property (single-transaction) idempotency. 

This model embeds the `Idempotency-Key` directly into the `internalProperties` of  the entity created by the originating operation, so the key and the operation commit atomically.  There is no separate idempotency store, so the configuration surface is intentionally minimal:  just a feature flag and the key TTL.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.idempotency.enabled` | `false` | `boolean` | Whether handler-level idempotency is enabled. When `false` the handlers ignore the `Idempotency-Key` header entirely and never stamp or read idempotency keys.  |
| `polaris.idempotency.ttl` | `PT5M` | `duration` | TTL for newly recorded idempotency keys. A retry that arrives within this window of a  successful create replays the existing table; after it elapses the key is treated as absent and  dropped inline on the entity's next write.  |
