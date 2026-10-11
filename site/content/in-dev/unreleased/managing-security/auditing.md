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
title: Auditing with Events
type: docs
weight: 400
---

Polaris emits structured events for catalog and management operations. Operators
can forward these events to an external system to build an audit trail. This page
describes what Polaris records today, how events are delivered, and the known
limits of that delivery. It does not prescribe a compliance architecture.

{{< alert warning >}}
Polaris is not a security information and event management (SIEM) system. Retention,
access control, tamper evidence, and querying of audit records are properties of
the system that receives the events, not of Polaris.
{{< /alert >}}

## What is recorded

Each event has a `PolarisEventType`, for example `BEFORE_LOAD_TABLE` and
`AFTER_LOAD_TABLE`. Most operations emit a `BEFORE_*` event when the request
starts and an `AFTER_*` event when it completes successfully. Event types are
grouped into categories such as `CATALOG`, `TABLE`, `PRINCIPAL`, and `CREDENTIAL`.
See the
[event listener configuration]({{% ref "../configuration/configuration-reference" %}})
for the full list.

Credential vending is covered by `BEFORE_LOAD_CREDENTIALS` and
`AFTER_LOAD_CREDENTIALS`.

Events pass through `DefaultEventSanitizer` before reaching listeners. By default,
it removes full catalog and principal objects, principal update requests, and
notification requests. Additional attributes can be removed with
`polaris.event-listener.denylisted-attributes`. Each listener then writes only
selected fields; custom listeners are responsible for not forwarding sensitive
attributes.

### What is not recorded

* **Authorization denials.** A denied request raises a `ForbiddenException`.
  There is no event type for a denial. A denied operation emits its `BEFORE_*`
  event and no matching `AFTER_*` event.
* **Authentication failures.** Requests rejected during authentication are not
  dispatched as events.
* **Decisions made by external authorizers.** If an external policy decision
  point is configured, its decision logs are produced by that system, not by
  Polaris.

HTTP access logs are not a substitute: they do not reliably include the
authenticated principal.

## Choosing listeners

Enable one or more listeners with `polaris.event-listener.types`. Each listener
can be limited to specific event types or categories with
`polaris.event-listener.<name>.enabled-event-types` and
`polaris.event-listener.<name>.enabled-event-categories`.

| Listener | Destination |
|----------|-------------|
| Persistence (in-memory buffer) | `events` table in the Polaris metastore |
| AWS CloudWatch | CloudWatch Logs |
| Kafka | A Kafka topic |
| OpenTelemetry | OpenTelemetry log records |

The `events` table shares the metastore and has no retention, purge, or query API
in Polaris. Users with direct database access can modify it. For an audit trail,
forward events to an external system instead.

## Delivery guarantees

Event delivery is best-effort. Operators should plan for the following:

* **Events are delivered asynchronously.** They are not written in the same
  transaction as the operation they describe.
* **A bounded backlog drops events.** If
  `polaris.event-listener.listener-backlog.queue-size` is set, events are dropped
  and logged when a listener's backlog is full. The default is unbounded.
* **The persistence listener buffers in memory.** Events are flushed after
  `polaris.event-listener.persistence-in-memory-buffer.buffer-time` (default
  `5000ms`) or after `max-buffer-size` events (default `5`). A failed flush is
  retried, then the batch is logged as failed and discarded. On shutdown,
  Polaris attempts to flush buffered events and discards events that arrive
  after shutdown begins. Buffered events are lost if the process terminates
  abruptly.
* **There is no ordering guarantee across listeners.**

## Correlating with storage access logs

Catalog events do not record reads and writes to table data. When Polaris vends
storage credentials, the cloud provider's access logs record the data access.
On AWS, Polaris can add session tags to the STS `AssumeRole` request used for
credential vending. The tags appear in CloudTrail and can be joined with Polaris
events. Session tags are disabled by default. Enable them with the
`SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL` feature flag, which also requires the IAM
role trust policy to allow `sts:TagSession`. See the
[configuration reference]({{% ref "../configuration/configuration-reference" %}}).
