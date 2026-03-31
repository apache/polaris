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

Idempotency configuration. 

Response replay persists and replays the full HTTP response entity for completed requests.  Responses are not truncated, because truncation would break clients. Any practical size limits  are therefore imposed by the chosen `IdempotencyStore` implementation / backing database.  If persistence fails (for example due to store limits), replay may be unavailable for that key.   Single-node vs multi-node deployments 

In a multi-node deployment (multiple Polaris replicas), idempotency records must be shared  across nodes via a durable store (for example JDBC + Postgres). Each node must use a different  executor id so ownership/heartbeats can be attributed correctly.   

Heartbeats should be enabled in multi-node deployments to prevent long-running requests from  looking stale after routing changes, pod restarts, or transient pauses.   

Purge is a best-effort background cleanup. In multi-node deployments, enabling purge on every  replica can create unnecessary contention; consider running purge in only one replica or via an  external scheduled job.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.idempotency.scopes` |  | `list of ` | Allowlist of endpoint scopes where idempotency is enforced. <br><br>This is an optional safety/rollout control: it limits idempotency to specific endpoint  prefixes and provides a stable (`Scope#operationType()`) for request binding.   <br><br>If empty, the filter falls back to applying idempotency for Iceberg REST catalog mutating  endpoints only.  |
| `polaris.idempotency.scopes.method` |  | `string` | HTTP method (e.g. POST). |
| `polaris.idempotency.scopes.path-prefix` |  | `string` | Request path prefix (no scheme/host; no leading slash required).  |
| `polaris.idempotency.scopes.operation-type` |  | `string` | Stable operation identifier used for the idempotency binding. <br><br>This should be stable across refactors and should not be derived from the HTTP method. |
| `polaris.idempotency.enabled` | `false` | `boolean` | Enable HTTP idempotency at the request/response filter layer.  |
| `polaris.idempotency.key-header` | `Idempotency-Key` | `string` | Request header name containing the client-provided idempotency key.  |
| `polaris.idempotency.ttl-seconds` | `PT5M` | `duration` | Default TTL for newly reserved keys. <br><br>Examples: `PT5M`, `PT300S`. |
| `polaris.idempotency.ttl-grace-seconds` | `PT0S` | `duration` | Additional grace added to (`#ttl()`) when reserving keys.  <br><br>This optionally extends retention slightly (for example to tolerate clock skew and queued  retries) while keeping the advertised lifetime unchanged. The default is `PT0S` (no extra  grace).   <br><br>Examples: `PT0S`, `PT10S`. |
| `polaris.idempotency.executor-id` |  | `string` | Executor identifier to store alongside reservations (e.g. pod/instance id).   <br><br>If unset or blank, the service derives a best-effort identifier from environment/host info  (for example `$POD_NAME` / `$HOSTNAME` plus the process id).   <br><br>In multi-node deployments, executor ids must be unique per replica. |
| `polaris.idempotency.in-progress-wait-seconds` | `PT30S` | `duration` | Maximum time to wait for an in-progress idempotency key to finalize before returning a  retryable response.   |
| `polaris.idempotency.poll-initial-delay` | `PT0.2S` | `duration` | Initial poll delay when waiting for an in-progress key to finalize. <br><br>Subsequent polls use exponential backoff up to (`#pollMaxDelay()`). |
| `polaris.idempotency.poll-max-delay` | `PT2S` | `duration` | Maximum poll delay when waiting for an in-progress key to finalize. <br><br>Higher values reduce DB load from duplicate requests at the cost of slightly higher replay  latency.  |
| `polaris.idempotency.lease-ttl-seconds` | `PT25S` | `duration` | Lease TTL for considering an in-progress owner "active" based on `heartbeatAt`. <br><br>If a duplicate observes `now - heartbeatAt > leaseTtl()`, the owner is treated as  stale and the server should not wait indefinitely.  |
| `polaris.idempotency.response-summary-max-bytes` | `262144` | `int` | Maximum byte size of the serialized response body stored for replay. <br><br>If the serialized body exceeds this limit, it is discarded and replay returns only the  status code and headers. This prevents unbounded memory and storage usage from large responses  (e.g. table metadata that can reach hundreds of MB).  |
| `polaris.idempotency.response-header-allowlist` |  | `list of string` | Response headers that are persisted and replayed (exact names). <br><br>Only the first header value is stored and replayed. |
| `polaris.idempotency.heartbeat-enabled` | `false` | `boolean` | Enable periodic heartbeats while a request is in progress. <br><br>Recommended for multi-node deployments. |
| `polaris.idempotency.heartbeat-interval-seconds` | `PT5S` | `duration` | Heartbeat interval while a request is in progress. <br><br>Examples: `PT5S`, `PT1S`.   <br><br>In multi-node deployments, this should be shorter than (`#leaseTtl()`). |
| `polaris.idempotency.purge-enabled` | `false` | `boolean` | Enable periodic purge of expired idempotency records. <br><br>In multi-node deployments, enabling purge on all replicas may cause unnecessary contention. |
| `polaris.idempotency.purge-executor-id` |  | `string` | Optional executor id that is allowed to run purge. <br><br>When set, only the node whose resolved (`#executorId()`) matches this value will run the  purge timer. This can be used to avoid "thundering herd" behavior in multi-node deployments.  |
| `polaris.idempotency.purge-interval-seconds` | `PT1M` | `duration` | Purge interval. <br><br>Examples: `PT1M`, `PT60S`. |
| `polaris.idempotency.purge-grace-seconds` | `PT0S` | `duration` | Purge records expired strictly before (now - grace). <br><br>Optionally keeps just-expired keys around a bit longer (for example to tolerate clock skew).  The default is `PT0S` (no extra grace).  |
