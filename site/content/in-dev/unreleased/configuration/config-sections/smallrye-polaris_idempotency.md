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

Deploy-time configuration for handler-level idempotency. 

Reservations are persisted via (`org.apache.polaris.core.persistence.IdempotencyPersistence`), which sits on the realm's (`org.apache.polaris.core.persistence.BasePersistence`).   

All settings here are deployment-wide constants read from the Quarkus configuration tree.  They do not vary per-realm or per-catalog. Per-realm or per-catalog overrides can be introduced  in a follow-up if/when there is a concrete operator request for it.   Single-node vs multi-node deployments 

In a multi-node deployment idempotency records must be shared across nodes via a durable store  (for example JDBC + Postgres). Each node must use a different executor id so  ownership/cancel/heartbeats can be attributed correctly.   

Purge is a best-effort background cleanup. In multi-node deployments, enabling purge on every  replica can create unnecessary contention; consider running purge in only one replica (via (`#purgeExecutorId()`)) or via an external scheduled job.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.idempotency.enabled` | `false` | `boolean` | Whether handler-level idempotency is enabled. When `false` the handlers ignore the  `Idempotency-Key` header entirely and never read or write the idempotency store.  |
| `polaris.idempotency.key-header` | `Idempotency-Key` | `string` | Request header name containing the client-provided idempotency key.  |
| `polaris.idempotency.executor-id` |  | `string` | Executor identifier to store alongside reservations (e.g. pod / instance id).   <br><br>If unset or blank, the service derives a best-effort identifier from environment / host info  (for example `$POD_NAME` / `$HOSTNAME` plus the process id).   <br><br>In multi-node deployments, executor ids must be unique per replica. |
| `polaris.idempotency.purge-executor-id` |  | `string` | Optional executor id that is allowed to run purge. <br><br>When set, only the node whose resolved (`#executorId()`) matches this value will run the  purge timer.  |
| `polaris.idempotency.ttl` | `PT5M` | `duration` | Default TTL for newly reserved idempotency keys. After this duration the reservation may be  purged by the background maintenance task.  |
| `polaris.idempotency.ttl-grace` | `PT0S` | `duration` | Additional grace added to (`#ttl()`) when reserving keys.  Extends retention slightly to  tolerate clock skew and queued retries while keeping the advertised lifetime unchanged.  |
| `polaris.idempotency.in-progress-wait` | `PT2S` | `duration` | Maximum time the handler waits while polling an in-progress reservation owned by another  executor before returning a retryable response.  The poll loop runs synchronously on the calling  worker thread, so this value is the upper bound on how long a single duplicate request can pin  a worker. Set conservatively: a high value combined with concurrent duplicate traffic can  exhaust the worker pool. Operators are expected to deploy route-level rate limiting on  idempotent endpoints in addition to this cap.  |
| `polaris.idempotency.lease-ttl` | `PT25S` | `duration` | Lease TTL for considering an in-progress idempotency owner "active" based on heartbeatAt. If a  duplicate observes `(now - heartbeatAt > leaseTtl)`, the owner is treated as stale and  the server will not wait indefinitely.  |
| `polaris.idempotency.in-progress-poll-interval` | `PT0.1S` | `duration` | Polling interval used while waiting for an in-progress duplicate.  |
| `polaris.idempotency.purge-enabled` | `false` | `boolean` | Whether the background purge timer is enabled. In multi-node deployments, enabling purge on  all replicas may cause unnecessary contention; set (`#purgeExecutorId()`) to pin purge to a  single replica.  |
| `polaris.idempotency.purge-interval` | `P1D` | `duration` | Purge interval. Defaults to `P1D` since records expire on their own `ttl` and the  timer only controls how often dead rows are reclaimed; operators with very high reservation  churn can lower it. Examples: `P1D`, `PT1H`, `PT15M`. |
| `polaris.idempotency.purge-grace` | `PT0S` | `duration` | Purge records expired strictly before `(now - purgeGrace)`.  |
