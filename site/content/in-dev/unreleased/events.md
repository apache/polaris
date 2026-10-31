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
title: Events
type: docs
weight: 455
---

Polaris emits structured events for catalog and management operations and can
forward them to external systems through listeners. This page describes what
events record, how to configure listeners, how events are delivered, and how to
correlate events with other logs.

{{< alert warning >}}
Polaris events are not a complete audit trail. They do not record authorization
denials or authentication failures, and delivery is asynchronous and
best-effort. See [What is not recorded](#what-is-not-recorded) and
[Delivery guarantees](#delivery-guarantees) before using events for auditing.
Retention, access control, tamper evidence, and querying of records are
properties of the system that receives the events, not of Polaris.
{{< /alert >}}

## What is recorded

Each event has a `PolarisEventType`, for example `BEFORE_LOAD_TABLE` and
`AFTER_LOAD_TABLE`. Most operations emit a `BEFORE_*` event when the request
starts and an `AFTER_*` event when it completes successfully. Event types are
grouped into categories such as `CATALOG`, `NAMESPACE`, `TABLE`, `VIEW`,
`PRINCIPAL`, `PRINCIPAL_ROLE`, `CATALOG_ROLE`, and `CREDENTIAL`.

Events cover both catalog operations and management operations. For example:

* Loading a table: `BEFORE_LOAD_TABLE` and `AFTER_LOAD_TABLE`.
* Vending storage credentials: `BEFORE_LOAD_CREDENTIALS` and `AFTER_LOAD_CREDENTIALS`.
* Creating a catalog or principal: `BEFORE_CREATE_CATALOG`, `BEFORE_CREATE_PRINCIPAL`,
  and their `AFTER_*` counterparts.
* Rotating principal credentials: `BEFORE_ROTATE_CREDENTIALS` and `AFTER_ROTATE_CREDENTIALS`.
* Granting and revoking privileges and roles, for example
  `BEFORE_ADD_GRANT_TO_CATALOG_ROLE` and `BEFORE_ASSIGN_PRINCIPAL_ROLE`.

Every event carries metadata: an event ID, a timestamp, and the realm. When
available, it also carries the authenticated principal, the request ID, and the
OpenTelemetry trace context. The principal is absent for events dispatched
before authentication completes.

Events pass through an `EventSanitizer` before reaching listeners, unless the
listener implements `RawEventAccess`, which opts it out of sanitization. No
built-in listener does so. The default
implementation, `DefaultEventSanitizer`, removes full catalog and principal objects, principal update requests, and
notification requests. When it removes a catalog object, it keeps the catalog
name. Additional attributes can be removed with
`polaris.event-listener.denylisted-attributes`. Each listener then writes only
selected fields. Custom listeners are responsible for not forwarding sensitive
attributes. A custom build of Polaris can replace the default sanitizer by
providing its own `EventSanitizer` bean, so check which sanitizer your
deployment uses.

### What is not recorded

* **Authorization denials.** There is no event type for a denial. A denied
  operation emits its `BEFORE_*` event and no matching `AFTER_*` event. With the
  built-in authorizer, the server logs the denial at `INFO`, including the
  principal and the missing privileges.
* **Authentication failures.** Requests rejected during authentication are not
  dispatched as events.
* **Decisions made by external authorizers.** If an
  [external policy decision point]({{< relref "managing-security/external-pdp" >}}) is configured,
  its decision logs are produced by that system, not by Polaris.

HTTP access logs are not a substitute. They do not reliably include the
authenticated principal.

## Choosing listeners

Enable one or more listeners with `polaris.event-listener.types`. Each listener
can be limited to specific event types or categories with
`polaris.event-listener.<name>.enabled-event-types` and
`polaris.event-listener.<name>.enabled-event-categories`. If neither is set, the
listener receives all events.

| Listener | Identifier | Destination |
|----------|------------|-------------|
| Persistence (in-memory buffer) | `persistence-in-memory-buffer` | `events` table in the Polaris metastore |
| AWS CloudWatch | `aws-cloudwatch` | CloudWatch Logs |
| Kafka | `kafka` | A Kafka topic |
| OpenTelemetry | `opentelemetry` | OpenTelemetry log records |

Event types excluded by these filters, such as credential vending or grant
changes, are not delivered to that listener. See the [configuration reference]({{% ref "configuration/configuration-reference#events" %}})
for all listener settings.

### The `events` table

The persistence listener writes to the `events` table in the same database, and
through the same datasource, as the catalog metastore. The table has a primary
key on `event_id` and no other indexes. Polaris provides no retention, purge,
or query API for it, and users with direct database access can modify it. The
persistence listener is not available with NoSQL persistence. For an audit trail,
forward events to an external system instead.

## Delivery guarantees

Event delivery is best-effort. Operators should plan for the following:

* **Events are delivered asynchronously.** They are not written in the same
  transaction as the operation they describe, and a delivery failure does not
  fail the request.
* **A bounded backlog drops events.** If
  `polaris.event-listener.listener-backlog.queue-size` is set, events are dropped
  and logged at `ERROR` when a listener's backlog is full. The default is unbounded.
* **A failing listener loses the event.** If a listener throws while handling an
  event, the error is logged and the event is not retried. Other listeners are
  not affected.
* **The persistence listener buffers in memory.** Events are flushed after
  `polaris.event-listener.persistence-in-memory-buffer.buffer-time` (default
  `5000ms`) or after `max-buffer-size` events (default `5`). A failed flush is
  retried, then the batch is logged as failed and discarded. On shutdown,
  Polaris attempts to flush buffered events and discards events that arrive
  after shutdown begins. Buffered events are lost if the process terminates
  abruptly.
* **Kafka and CloudWatch send asynchronously by default.** Send failures are
  logged and not retried by Polaris. Setting `synchronous-mode=true` makes the
  listener wait for each send. It does not make the originating request wait or
  fail.
* **Ordering is limited.** There is no ordering guarantee across listeners. The
  Kafka listener uses the event ID as the record key, so the Kafka listener
  configuration recommends a single-partition topic to preserve FIFO order.

## Correlating with storage access logs

Catalog events do not record reads and writes to table data. When Polaris vends
storage credentials, the cloud provider's access logs record the data access.

* **AWS:** Polaris can add session tags to the STS `AssumeRole` request used for
  credential vending. The tags appear in CloudTrail and can be joined with
  Polaris events. Session tags are disabled by default. Enable them with the
  `SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL` feature flag, which requires the IAM
  role trust policy to allow `sts:TagSession`. The
  `SESSION_NAME_FIELDS_IN_SUBSCOPED_CREDENTIAL` flag adds context to the role
  session name instead.
* **Google Cloud Storage:** with `GCS_PRINCIPAL_ATTRIBUTION_ENABLED`, the Polaris
  principal appears in Cloud Storage Data Access logs. It requires additional
  Workload Identity Federation settings.

See the [feature flags reference]({{% ref "configuration/configuration-reference#polarisfeatures" %}})
for the options and requirements of each flag.

## Correlating with request IDs

Polaris returns the request ID in a response header, `X-Request-ID` by default.
The header name is configured with `polaris.log.request-id-header-name`. A client
can also set the header on the request. The request ID appears in the server log
and in event metadata, so it links a client call to both.

For example, to investigate a denied request:

1. Get the request ID from the client's response or logs.
2. Search the Polaris server log for that request ID. With the built-in
   authorizer, the log line for the denial names the principal, the operation,
   and the missing privileges. With an external authorizer, check that system's
   decision logs.
3. In the event sink, find events with the same request ID. A denied operation
   shows a `BEFORE_*` event without a matching `AFTER_*` event. Any failed
   request shows the same pattern, for example a missing table or a commit
   conflict, so the server log line from step 2 is what confirms a denial.

See [Telemetry]({{% ref "telemetry#mdc-logging" %}})
for the log format and MDC keys.
