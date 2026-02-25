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
title: Idempotency
linkTitle: Idempotency
type: docs
weight: 650
---

Polaris can enforce HTTP idempotency for a scoped set of REST endpoints via an `Idempotency-Key` request header.
When enabled, Polaris will reserve keys, wait for in-progress duplicates, and replay finalized responses for duplicates.

## Recommended configuration

### Single-node (one replica)

This mode is simplest and is typically suitable for development and test.

```properties
polaris.idempotency.enabled=true
polaris.idempotency.heartbeat-enabled=false
polaris.idempotency.purge-enabled=false
```

### Multi-node / Kubernetes (multiple replicas)

In multi-node deployments, idempotency only works correctly if:

- A **shared durable idempotency store** is used (for example JDBC + Postgres), so all replicas see the same keys.
- Each replica has a **unique executor id**, so reservations and heartbeats can be attributed to the correct owner.
- **Heartbeats are enabled**, so long-running requests do not appear stale to other replicas during routing changes or restarts.

```properties
polaris.idempotency.enabled=true
polaris.idempotency.heartbeat-enabled=true

# Ensure executor IDs are unique per replica.
# If unset, Polaris derives a best-effort value from $POD_NAME/$HOSTNAME plus the process id.
# polaris.idempotency.executor-id=${POD_NAME}
```

## Tuning notes

- Duration settings (for example `polaris.idempotency.ttl-seconds`) accept ISO-8601 values like `PT5M`, `PT30S`, or shorthand like `60S`.
- **`polaris.idempotency.heartbeat-interval-seconds`**: should be shorter than **`polaris.idempotency.lease-ttl-seconds`**.
  A common pattern is a heartbeat interval of a few seconds and a lease TTL of several heartbeat intervals.
- **Pod restarts**: if a replica crashes mid-request, other replicas may observe the key as in-progress.
  With heartbeats enabled, staleness is detected based on the last heartbeat. Without heartbeats, in-progress keys may
  appear stale sooner and lead to retryable errors.
- **Purge**: the periodic purge is best-effort cleanup of expired keys. In multi-node deployments, enabling purge on every
  replica can create unnecessary contention. Consider enabling purge on a single replica or running purge via an external
  scheduled job.

## Configuration keys

All idempotency settings use the `polaris.idempotency.*` prefix. See `IdempotencyConfiguration` in the runtime service for the full list of keys and defaults.

