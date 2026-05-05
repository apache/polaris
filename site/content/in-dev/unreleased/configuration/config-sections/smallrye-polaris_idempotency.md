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

Deploy-time / platform configuration for handler-level idempotency. 

Reservations are persisted via (`org.apache.polaris.core.persistence.IdempotencyPersistence`), which sits on the realm's (`org.apache.polaris.core.persistence.BasePersistence`).   What lives here vs. (`org.apache.polaris.core.config.FeatureConfiguration`) 

The settings on this interface are deployment-wide constants that an operator typically sets  once per service installation (HTTP header name, executor identity, infrastructure timing knobs).  They do not vary per-realm or per-catalog and are read directly from the Quarkus configuration  tree.   

Tenant-visible behaviour knobs (whether the feature is on, TTLs, the in-progress wait budget,  lease TTL, purge enable) live in (`org.apache.polaris.core.config.FeatureConfiguration`) as  `IDEMPOTENCY_*` entries so they can be overridden per-realm or per-catalog at runtime  through the standard configuration resolution path.   Single-node vs multi-node deployments 

In a multi-node deployment idempotency records must be shared across nodes via a durable store  (for example JDBC + Postgres). Each node must use a different executor id so  ownership/cancel/heartbeats can be attributed correctly.   

Purge is a best-effort background cleanup. In multi-node deployments, enabling purge on every  replica can create unnecessary contention; consider running purge in only one replica (via (`#purgeExecutorId()`)) or via an external scheduled job.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.idempotency.key-header` | `Idempotency-Key` | `string` | Request header name containing the client-provided idempotency key.  |
| `polaris.idempotency.executor-id` |  | `string` | Executor identifier to store alongside reservations (e.g. pod / instance id).   <br><br>If unset or blank, the service derives a best-effort identifier from environment / host info  (for example `$POD_NAME` / `$HOSTNAME` plus the process id).   <br><br>In multi-node deployments, executor ids must be unique per replica. |
| `polaris.idempotency.purge-executor-id` |  | `string` | Optional executor id that is allowed to run purge. <br><br>When set, only the node whose resolved (`#executorId()`) matches this value will run the  purge timer.  |
| `polaris.idempotency.in-progress-poll-interval` | `PT0.1S` | `duration` | Polling interval used while waiting for an in-progress duplicate.  |
| `polaris.idempotency.purge-interval` | `PT1H` | `duration` | Purge interval. Defaults to `PT1H` so per-pod wake-ups stay cheap; operators with very  high reservation churn can lower it. Examples: `PT1H`, `PT15M`. |
| `polaris.idempotency.purge-grace` | `PT0S` | `duration` | Purge records expired strictly before `(now - purgeGrace)`.  |
