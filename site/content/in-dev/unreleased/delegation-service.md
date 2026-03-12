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
title: Delegation Service
type: docs
weight: 430
---

The Polaris Delegation Service enables external services — compute engines, orchestrators, policy enforcement points, and
maintenance tools — to interact with Polaris as a centralized governance and metadata authority.

External services can integrate with Polaris using three complementary patterns:

- **Pull**: The external service queries Polaris on demand to fetch policies, configuration, or metadata.
- **Push**: The external service sends notifications to Polaris to inform it of changes happening externally.
- **Event**: Polaris emits internal events at every operation boundary, enabling custom listeners to bridge Polaris with external systems reactively.

## Architecture Overview

```
 ┌──────────────────────┐         ┌─────────────────────┐
 │   External Service   │         │   External Catalog   │
 │  (Spark, Trino,      │         │  (Hive, Glue,       │
 │   Flink, custom)     │         │   custom REST)       │
 └──────┬───────────────┘         └──────┬──────────────┘
        │                                │
        │  PULL: query policies,         │  PUSH: send table
        │  metadata, configuration       │  change notifications
        │                                │
        ▼                                ▼
 ┌──────────────────────────────────────────────────────┐
 │                  Apache Polaris                       │
 │                                                      │
 │  ┌──────────┐  ┌─────────────┐  ┌────────────────┐  │
 │  │ Policy   │  │ Notification│  │ Event Listener │  │
 │  │ API      │  │ API         │  │ Framework      │  │
 │  └──────────┘  └─────────────┘  └───────┬────────┘  │
 │                                         │           │
 └─────────────────────────────────────────┼───────────┘
                                           │
                                    EVENT: BEFORE/AFTER
                                    hooks on all operations
                                           │
                                           ▼
                                 ┌──────────────────┐
                                 │ Custom Listeners  │
                                 │ (Kafka, webhooks, │
                                 │  CloudWatch, ...) │
                                 └──────────────────┘
```

## Pull vs Push — Choosing the Right Integration Pattern

The pull and push patterns serve different purposes and have different trade-offs. Understanding when to use
each — or combine them — is key to building a robust integration with Polaris.

### Pull: Service-Driven Queries

In the pull model, the external service decides **when** and **what** to query. This gives the service full
control over timing and request volume.

**Best suited for:**
- Compute engines that need policies or metadata at query planning time (e.g., Spark, Trino, Flink)
- Maintenance services that run on a fixed schedule (e.g., a compaction job that runs every hour)
- Services that need to discover resources dynamically (listing namespaces and tables)
- Scenarios where the external service is the source of truth for when work should happen

**Advantages:**
- Simple to implement — standard REST API calls
- No infrastructure needed beyond an HTTP client
- Service controls its own polling frequency and backoff strategy
- Naturally resilient — a missed poll just delays the next check, nothing is lost

**Considerations:**
- Introduces polling latency — changes are not detected until the next poll cycle
- Generates load on Polaris proportional to the number of services and their polling frequency
- Use the `version` field on policies to avoid redundant reprocessing (see [Polling and Versioning](#polling-and-versioning))

### Push: External-Catalog-Driven Notifications

In the push model, the external system sends notifications to Polaris the moment a change occurs. Polaris
does not poll the external system — it reacts to incoming notifications.

**Best suited for:**
- External catalogs (Hive Metastore, AWS Glue, custom REST catalogs) that manage table lifecycle outside Polaris
- Scenarios where Polaris must mirror metadata from another system with minimal delay
- Multi-catalog environments where Polaris is the unified governance layer but does not own all table metadata

**Advantages:**
- Near-real-time synchronization — Polaris learns about changes as soon as they happen
- No polling overhead on either side
- Timestamp-based ordering ensures consistency even under concurrent updates
- `VALIDATE` pre-checks prevent wasted work when permissions or locations are misconfigured

**Considerations:**
- Requires the external system to implement notification sending logic (e.g., a Hive hook or a CDC pipeline)
- The caller must manage monotonically increasing timestamps and handle `409 Conflict` rejections
- Only applies to table metadata synchronization — not for querying policies or configuration

### Combining Pull and Push with Event Listeners

For the most responsive integrations, combine pull and push with the event listener pattern:

```
 ┌──────────────┐   1. PUSH notification    ┌────────────┐
 │ External     │ ─────────────────────────→ │  Polaris   │
 │ Catalog      │                            │            │
 └──────────────┘                            │  2. Fires  │
                                             │  AFTER_    │
                                             │  SEND_     │
                                             │  NOTIF.    │
                                             └─────┬──────┘
                                                   │
                              3. Event listener     │
                              forwards to Kafka     │
                                                   ▼
 ┌──────────────┐   4. Consumes event        ┌────────────┐
 │ Maintenance  │ ←──────────────────────────│   Kafka    │
 │ Service      │                            └────────────┘
 │              │   5. PULL: fetch policies
 │              │ ─────────────────────────→  Polaris
 └──────────────┘
```

In this pattern:
1. An external catalog **pushes** metadata changes to Polaris via notifications.
2. Polaris fires `AFTER_SEND_NOTIFICATION` events, which a custom listener forwards to a message queue.
3. A maintenance service consumes the event and **pulls** the applicable policies to decide what action to take.

This eliminates polling latency while keeping the maintenance service decoupled from the notification flow.

## Access Delegation Modes

When external services interact with Polaris to access table data, Polaris supports two credential
delegation mechanisms via the `X-Iceberg-Access-Delegation` HTTP header, as defined by the Iceberg
REST API specification:

| Mode | Header Value | Description |
|------|-------------|-------------|
| **Vended Credentials** | `vended-credentials` | Polaris generates short-lived, scoped credentials (e.g., AWS STS tokens) and sends them to the client. The client uses these credentials directly to access storage. |
| **Remote Signing** | `remote-signing` | The client sends data access requests back to Polaris, which signs them on behalf of the client. The client never receives raw storage credentials. |

The client specifies the desired mode(s) as a comma-separated list in the `X-Iceberg-Access-Delegation`
header. For backward compatibility, the legacy value `true` is treated as `vended-credentials`.

## Pull Pattern — Querying Polaris from an External Service

In the pull model, an external service authenticates with Polaris and calls its REST APIs on demand to retrieve
policies, table metadata, or configuration. This is the most common integration pattern for compute engines and
maintenance services.

### Authentication

External services authenticate using OAuth2 tokens:

```bash
curl -X POST https://polaris.example.com/v1/oauth/tokens \
  -d "grant_type=client_credentials" \
  -d "client_id=<service-client-id>" \
  -d "client_secret=<service-client-secret>" \
  -d "scope=PRINCIPAL_ROLE:ALL"
```

Polaris also supports delegating authentication to an external Identity Provider (IdP) via OIDC. See
[External IdP](managing-security/external-idp/) for details.

### Fetching Applicable Policies

The primary pull endpoint for external services is the **applicable policies** API. It resolves the full policy
hierarchy (including inheritance from catalog and namespace levels) for a given resource:

```
GET /polaris/v1/{catalog}/applicable-policies?namespace={ns}&target-name={table}&policyType={type}
```

Parameters:
- `namespace` — the target namespace (use `%1F` as separator for multi-level namespaces)
- `target-name` — the table or view name (omit for namespace-level or catalog-level queries)
- `policyType` — optional filter (e.g., `system.data-compaction`)
- `page-token`, `page-size` — for paginating large result sets

**Policy inheritance** works as follows: when you query applicable policies for a table, Polaris resolves
policies attached at the catalog level, the namespace level (including parent namespaces), and the table
level itself. Policies marked as `inheritable: true` propagate down the hierarchy. The `inherited` field in
the response indicates whether a policy was directly attached to the target or inherited from a parent scope.

**Example response:**
```json
{
  "applicable-policies": [
    {
      "name": "compaction-policy",
      "policy-type": "system.data-compaction",
      "inherited": true,
      "namespace": ["analytics"],
      "version": 3,
      "inheritable": true,
      "content": "{\"version\": \"2025-02-03\", \"enable\": true, \"config\": {\"target_file_size_bytes\": 134217728}}"
    }
  ]
}
```

### Managing Policies

Beyond reading policies, external services with appropriate permissions can manage the full policy lifecycle:

| Operation | Endpoint | Method |
|-----------|----------|--------|
| Create a policy | `/polaris/v1/{catalog}/namespaces/{ns}/policies` | `POST` |
| List policies in a namespace | `/polaris/v1/{catalog}/namespaces/{ns}/policies` | `GET` |
| Load a specific policy | `/polaris/v1/{catalog}/namespaces/{ns}/policies/{name}` | `GET` |
| Update a policy | `/polaris/v1/{catalog}/namespaces/{ns}/policies/{name}` | `PATCH` |
| Drop a policy | `/polaris/v1/{catalog}/namespaces/{ns}/policies/{name}` | `DELETE` |
| Attach a policy to a resource | `/polaris/v1/{catalog}/namespaces/{ns}/policies/{name}/mappings` | `PUT` |
| Detach a policy from a resource | `/polaris/v1/{catalog}/namespaces/{ns}/policies/{name}/mappings` | `POST` |

### Example: External Compaction Service

A typical pull-based integration for a data compaction service:

```
1. Authenticate
   POST /v1/oauth/tokens → obtain bearer token

2. Discover tables
   GET /v1/{catalog}/namespaces/{ns}/tables → list of tables

3. Fetch compaction policies for each table
   GET /polaris/v1/{catalog}/applicable-policies
       ?namespace={ns}&target-name={table}&policyType=system.data-compaction

4. Execute compaction
   If the policy has "enable": true, run compaction using the parameters
   from the policy content (target_file_size_bytes, compaction_strategy, etc.)

5. Repeat on schedule
   Poll periodically or use an event-driven trigger to detect policy changes.
```

### Polling and Versioning

Policies include a `version` field that increments on every update. External services can use this to detect
changes efficiently:

1. Cache the last-seen version for each policy.
2. Periodically call the applicable-policies endpoint.
3. Compare versions — only re-process resources whose policy version has changed.

For real-time change detection, combine the pull pattern with the [event listener pattern](#event-listeners--reacting-to-polaris-operations):
register a listener for `AFTER_ATTACH_POLICY` or `AFTER_UPDATE_POLICY` events and trigger a policy refresh in
the external service when relevant events fire.

## Push Pattern — Sending Notifications to Polaris

In the push model, an external catalog or service notifies Polaris when table metadata changes occur outside of
Polaris. This keeps Polaris synchronized with externally managed tables.

### Notification Endpoint

```
POST /v1/{catalog}/namespaces/{namespace}/tables/{table}/notifications
```

The target catalog must be configured as `EXTERNAL` for notifications to be accepted.

### Notification Types

| Type | Description |
|------|-------------|
| `CREATE` | A new table was created in the external catalog |
| `UPDATE` | Table metadata was updated (e.g., new snapshot, schema change) |
| `DROP` | The table was dropped from the external catalog |
| `VALIDATE` | Dry-run to pre-validate permissions and location settings without creating state |

### Request Payload

```json
{
  "notification-type": "UPDATE",
  "payload": {
    "table-name": "transactions",
    "timestamp": 1710288000000,
    "table-uuid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "metadata-location": "s3://warehouse/ns/transactions/metadata/v3.metadata.json"
  }
}
```

Payload fields:
- `table-name` — the name of the table being reported on
- `timestamp` — a monotonically increasing 64-bit integer (milliseconds since epoch) used for ordering
- `table-uuid` — the UUID of the table in the external catalog
- `metadata-location` — the storage path to the Iceberg `metadata.json` file that Polaris should read to synchronize its state

Optionally, the payload can include a `metadata` field with inline table metadata, but the standard approach is to
provide the `metadata-location` and let Polaris fetch it.

### Timestamp Ordering

Polaris enforces strict timestamp ordering per table. If a notification arrives with a timestamp older than or
equal to the most recently processed timestamp for that table, Polaris rejects it with a `409 Conflict` response:

```json
{
  "error": {
    "message": "A notification with a newer timestamp has been admitted for table",
    "type": "AlreadyExistsException",
    "code": 409
  }
}
```

The caller is responsible for ensuring correct timestamp ordering across notifications, including handling
clock skew when notifications originate from multiple sources.

### Using VALIDATE for Pre-Checks

Before creating a table in an external catalog, a service can send a `VALIDATE` notification to verify that:
- The caller has the required permissions on the target catalog
- The intended metadata location falls within `ALLOWED_LOCATIONS`
- The target catalog is configured as `EXTERNAL`

A successful validation returns `204 No Content`. This avoids the scenario where a table is created externally
but Polaris rejects subsequent notifications due to configuration issues.

### Interaction with Event Listeners

Every incoming notification also triggers event listener hooks. Polaris emits `BEFORE_SEND_NOTIFICATION` and
`AFTER_SEND_NOTIFICATION` events (codes 1000–1001), which include the full notification request as an attribute
(`EventAttributes.NOTIFICATION_REQUEST`). This allows custom listeners to react to external catalog changes —
for example, to propagate notifications to a message queue or trigger downstream pipelines.

### Example: Syncing an External Hive Metastore

```
1. Hive hook detects a table creation in the Hive Metastore.

2. Hook sends a VALIDATE notification to Polaris to pre-check the location:
   POST /v1/hive-catalog/namespaces/default/tables/orders/notifications
   { "notification-type": "VALIDATE", "payload": { ... } }

3. If validation succeeds (204), proceed. If it fails (400/403), alert the operator.

4. After the table is committed in Hive, send a CREATE notification:
   POST /v1/hive-catalog/namespaces/default/tables/orders/notifications
   { "notification-type": "CREATE", "payload": { ... } }

5. On subsequent metadata updates, send UPDATE notifications with increasing timestamps.
```

## Event Listeners — Reacting to Polaris Operations

Polaris fires events at the boundary of every operation, enabling custom listeners to react to changes and
bridge Polaris with external systems.

### Event Model

Every operation emits a pair of events:
- **BEFORE**: fired before the operation executes (can be used for validation or pre-processing)
- **AFTER**: fired after the operation completes successfully

Each event is a `PolarisEvent` record containing three components:

| Component | Type | Description |
|-----------|------|-------------|
| `type` | `PolarisEventType` | The specific event type (e.g., `AFTER_CREATE_TABLE`) |
| `metadata` | `PolarisEventMetadata` | Contextual metadata about the event |
| `attributes` | `EventAttributeMap` | Typed key-value map of event-specific data |

#### Event Metadata

Every event carries rich metadata for auditing and tracing:

| Field | Type | Description |
|-------|------|-------------|
| `eventId` | `UUID` | Unique identifier for this event |
| `timestamp` | `Instant` | When the event was emitted |
| `realmId` | `String` | The Polaris realm where the event occurred |
| `user` | `Optional<PolarisPrincipal>` | The principal who triggered the operation (includes name and activated roles) |
| `requestId` | `Optional<String>` | The HTTP request ID for correlation |
| `openTelemetryContext` | `Map<String, String>` | OpenTelemetry trace/span context for distributed tracing |

#### Event Attributes

Events carry typed attributes specific to each operation. Attributes are accessed via type-safe keys defined
in `EventAttributes`. For example:

| Attribute Key | Type | Available On |
|---------------|------|-------------|
| `CATALOG_NAME` | `String` | Most events |
| `NAMESPACE` | `Namespace` | Namespace, table, view, policy events |
| `TABLE_IDENTIFIER` | `TableIdentifier` | Table events |
| `TABLE_METADATA` | `TableMetadata` | Table load/update/create events |
| `NOTIFICATION_REQUEST` | `NotificationRequest` | Notification events |
| `POLICY_NAME` | `String` | Policy events |
| `POLICY_TYPE` | `String` | Policy events |
| `CREATE_POLICY_REQUEST` | `CreatePolicyRequest` | `BEFORE_CREATE_POLICY` |
| `ATTACH_POLICY_REQUEST` | `AttachPolicyRequest` | `BEFORE_ATTACH_POLICY` |
| `GET_APPLICABLE_POLICIES_RESPONSE` | `GetApplicablePoliciesResponse` | `AFTER_GET_APPLICABLE_POLICIES` |
| `TASK_ENTITY_ID` | `Long` | Task events |
| `TASK_SUCCESS` | `Boolean` | `AFTER_ATTEMPT_TASK` |
| `HTTP_METHOD` | `String` | Rate limiting events |
| `REQUEST_URI` | `String` | Rate limiting events |

### Event Types

Events cover all Polaris domains:

| Domain | Event Code Range | Examples |
|--------|-----------------|----------|
| Catalog | 100–109 | `BEFORE_CREATE_CATALOG`, `AFTER_DELETE_CATALOG` |
| Catalog Role | 200–217 | `BEFORE_CREATE_CATALOG_ROLE`, `AFTER_ADD_GRANT_TO_CATALOG_ROLE` |
| Principal | 300–319 | `BEFORE_CREATE_PRINCIPAL`, `AFTER_ROTATE_CREDENTIALS` |
| Principal Role | 400–417 | `BEFORE_CREATE_PRINCIPAL_ROLE`, `AFTER_ASSIGN_CATALOG_ROLE_TO_PRINCIPAL_ROLE` |
| Namespace | 500–511 | `BEFORE_CREATE_NAMESPACE`, `AFTER_DROP_NAMESPACE` |
| Table | 600–617 | `BEFORE_CREATE_TABLE`, `AFTER_UPDATE_TABLE` |
| View | 700–715 | `BEFORE_CREATE_VIEW`, `AFTER_REPLACE_VIEW` |
| Credential | 800–801 | `BEFORE_LOAD_CREDENTIALS`, `AFTER_LOAD_CREDENTIALS` |
| Transaction | 900–901 | `BEFORE_COMMIT_TRANSACTION`, `AFTER_COMMIT_TRANSACTION` |
| Notification | 1000–1001 | `BEFORE_SEND_NOTIFICATION`, `AFTER_SEND_NOTIFICATION` |
| Configuration | 1100–1101 | `BEFORE_GET_CONFIG`, `AFTER_GET_CONFIG` |
| Policy | 1200–1215 | `BEFORE_CREATE_POLICY`, `AFTER_ATTACH_POLICY`, `AFTER_GET_APPLICABLE_POLICIES` |
| Generic Table | 1300–1307 | `BEFORE_CREATE_GENERIC_TABLE`, `AFTER_LOAD_GENERIC_TABLE` |
| Task Execution | 1400–1401 | `BEFORE_ATTEMPT_TASK`, `AFTER_ATTEMPT_TASK` |
| Rate Limiting | 1500 | `BEFORE_LIMIT_REQUEST_RATE` |

### Implementing a Custom Event Listener

Implement the `PolarisEventListener` interface and register it as a CDI bean:

```java
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.listeners.PolarisEventListener;

@ApplicationScoped
public class ExternalCompactionTriggerListener implements PolarisEventListener {

    @Override
    public void onEvent(PolarisEvent event) {
        if (event.type() == PolarisEventType.AFTER_ATTACH_POLICY) {
            // A policy was just attached to a resource.
            // Access typed attributes safely via EventAttributes keys.
            String catalog = event.attributes()
                .get(EventAttributes.CATALOG_NAME)
                .orElse("unknown");
            String policyName = event.attributes()
                .get(EventAttributes.POLICY_NAME)
                .orElse("unknown");
            String realmId = event.metadata().realmId();

            // Notify the external compaction service to re-evaluate its schedule.
            // ... send message to external service
        }
    }
}
```

Listeners are invoked synchronously within the request thread. Long-running work should be offloaded to a
background executor or message queue to avoid blocking the request path.

### Built-in Listeners

Polaris ships with several listener implementations:

| Listener | Identifier | Description |
|----------|-----------|-------------|
| `NoOpPolarisEventListener` | `no-op` | Default implementation; does nothing. Use this in production when no event forwarding is needed. |
| `InMemoryBufferEventListener` | `persistence-in-memory-buffer` | Buffers events in memory using a Caffeine cache, then flushes them in batches to the meta store. Includes automatic retry (up to 5 retries with 1s delay) on flush failures. Useful for testing and event persistence. |
| `AwsCloudWatchEventListener` | `aws-cloudwatch` | Forwards `AFTER_REFRESH_TABLE` events to AWS CloudWatch Logs asynchronously. Automatically creates the configured log group and stream on startup. |

Custom listeners can forward events to Kafka, HTTP webhooks, message queues, or any external system.

### Configuring Event Listeners

The event listener is configured via `application.properties` using the `polaris.event-listener` prefix:

```properties
# Default: no-op listener (does nothing)
polaris.event-listener.type=no-op
```

**In-memory buffer listener:**

```properties
polaris.event-listener.type=persistence-in-memory-buffer
polaris.event-listener.persistence-in-memory-buffer.buffer-time=5000ms
polaris.event-listener.persistence-in-memory-buffer.max-buffer-size=5
```

| Property | Default | Description |
|----------|---------|-------------|
| `buffer-time` | `5000ms` | Maximum time to buffer events before flushing |
| `max-buffer-size` | `5` | Maximum number of events to buffer before flushing |

**AWS CloudWatch listener:**

```properties
polaris.event-listener.type=aws-cloudwatch
polaris.event-listener.aws-cloudwatch.log-group=polaris-cloudwatch-default-group
polaris.event-listener.aws-cloudwatch.log-stream=polaris-cloudwatch-default-stream
polaris.event-listener.aws-cloudwatch.region=us-east-1
polaris.event-listener.aws-cloudwatch.synchronous-mode=false
```

| Property | Default | Description |
|----------|---------|-------------|
| `log-group` | — | CloudWatch Logs log group name (created automatically if it does not exist) |
| `log-stream` | — | CloudWatch Logs log stream name (created automatically if it does not exist) |
| `region` | — | AWS region for the CloudWatch Logs client |
| `synchronous-mode` | `false` | When `true`, blocks until the log event is written; when `false`, writes asynchronously |

## Reporting Service Job Status Back to Polaris

After an external service executes a job (compaction, snapshot expiry, orphan file removal, etc.), it
typically needs to report the outcome back to Polaris so that other services, operators, and the
governance layer can observe the result. Polaris provides several mechanisms for this.

### Option 1: Updating Policy Content

The most natural mechanism for services that are **policy-driven** is to update the policy content with
execution results. Since policies already define the job parameters, they are a logical place to record
the outcome.

```
1. Service fetches the compaction policy for a table:
   GET /polaris/v1/{catalog}/applicable-policies
       ?namespace=analytics&target-name=orders&policyType=system.data-compaction

   Response includes version: 3 and content with job parameters.

2. Service executes the compaction job.

3. Service updates the policy with execution results:
   PATCH /polaris/v1/{catalog}/namespaces/analytics/policies/compaction-policy
   {
     "current-policy-version": 3,
     "content": "{\"version\": \"2025-02-03\", \"enable\": true, \"config\": {\"target_file_size_bytes\": 134217728}, \"last-execution\": {\"status\": \"SUCCEEDED\", \"timestamp\": \"2026-03-12T10:30:00Z\", \"files-compacted\": 42, \"bytes-written\": 5637144576}}"
   }
```

The `current-policy-version` field provides optimistic concurrency control: if another client updated
the policy between the read and the write, the update fails with a `409 Conflict`, and the service
should re-read and retry.

This approach works well because:
- Policy updates fire `AFTER_UPDATE_POLICY` events, so other listeners are automatically notified
- The policy version increments, so other services polling with version-based change detection pick up the change
- The execution result is co-located with the configuration that triggered it

### Option 2: Updating Table Properties

For services that operate on Iceberg tables, updating the table's properties via the standard Iceberg REST
API is another option. Table properties are key-value pairs that can store arbitrary metadata:

```
POST /v1/{catalog}/namespaces/{namespace}/tables/{table}
{
  "requirements": [...],
  "updates": [
    {
      "action": "set-properties",
      "updates": {
        "compaction.last-run": "2026-03-12T10:30:00Z",
        "compaction.status": "SUCCEEDED",
        "compaction.files-rewritten": "42"
      }
    }
  ]
}
```

This uses the standard Iceberg `UpdateTable` API with a `set-properties` update action. Table property
updates fire `AFTER_UPDATE_TABLE` events and update the table version, making them observable by other
services and event listeners.

### Option 3: Using Generic Tables for Job Tracking

For services that need a dedicated **job execution log** rather than updating inline metadata, Polaris
provides the Generic Table API. Generic tables are non-Iceberg tables that can store arbitrary structured
data via key-value properties.

```
POST /polaris/v1/{catalog}/namespaces/analytics/generic-tables
{
  "name": "compaction-jobs-orders",
  "format": "json",
  "doc": "Compaction job execution log for orders table",
  "properties": {
    "target-table": "orders",
    "last-job-id": "job-20260312-103000",
    "last-job-status": "SUCCEEDED",
    "last-job-timestamp": "2026-03-12T10:30:00Z",
    "last-job-files-compacted": "42",
    "last-job-bytes-written": "5637144576",
    "last-job-duration-ms": "185000"
  }
}
```

Generic Table API endpoints:

| Operation | Endpoint | Method |
|-----------|----------|--------|
| Create a job tracking table | `/polaris/v1/{catalog}/namespaces/{ns}/generic-tables` | `POST` |
| List job tracking tables | `/polaris/v1/{catalog}/namespaces/{ns}/generic-tables` | `GET` |
| Load job status | `/polaris/v1/{catalog}/namespaces/{ns}/generic-tables/{table}` | `GET` |
| Remove a job tracking table | `/polaris/v1/{catalog}/namespaces/{ns}/generic-tables/{table}` | `DELETE` |

Generic tables support Polaris RBAC, so access to job status can be controlled via catalog roles
and grants, just like Iceberg tables.

### Option 4: Sending Notifications for Externally Managed Changes

When a service performs work that changes the physical state of a table (e.g., rewriting data files during
compaction), and the table is managed externally, the service should send an `UPDATE` notification to
Polaris so that the metadata stays synchronized:

```
POST /v1/{catalog}/namespaces/analytics/tables/orders/notifications
{
  "notification-type": "UPDATE",
  "payload": {
    "table-name": "orders",
    "timestamp": 1741776600000,
    "table-uuid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "metadata-location": "s3://warehouse/analytics/orders/metadata/v42.metadata.json"
  }
}
```

This is specifically for keeping Polaris's view of the table metadata current after external modifications.

### Choosing a Status Reporting Mechanism

| Mechanism | Best For | Observable By |
|-----------|----------|---------------|
| **Policy content update** | Policy-driven jobs where the result is part of the policy lifecycle | `AFTER_UPDATE_POLICY` events, version-based polling |
| **Table property update** | Per-table execution metadata tied to the Iceberg table | `AFTER_UPDATE_TABLE` events, table version tracking |
| **Generic table** | Dedicated job execution logs, multi-table job summaries, historical tracking | `AFTER_CREATE_GENERIC_TABLE` events, direct load |
| **Notification** | Syncing physical metadata changes back to Polaris for externally managed tables | `AFTER_SEND_NOTIFICATION` events |

### Example: End-to-End Compaction Service Workflow

The following example combines pulling configuration, executing work, and reporting status:

```
1. Authenticate
   POST /v1/oauth/tokens → bearer token

2. Fetch compaction policy (PULL)
   GET /polaris/v1/{catalog}/applicable-policies
       ?namespace=analytics&target-name=orders&policyType=system.data-compaction
   → policy version=3, target_file_size_bytes=134217728

3. Execute compaction job
   Read table metadata, select files, rewrite data files.

4. Commit new table snapshot (via Iceberg REST API)
   POST /v1/{catalog}/namespaces/analytics/tables/orders
   → new table metadata written to storage

5. Report job status (update policy content)
   PATCH /polaris/v1/{catalog}/namespaces/analytics/policies/compaction-policy
   {
     "current-policy-version": 3,
     "content": "{...original config..., \"last-execution\": {\"status\": \"SUCCEEDED\", ...}}"
   }

6. Polaris fires AFTER_UPDATE_POLICY event
   → Custom listeners detect the status change
   → Other services polling see version increment (now version=4)
```

## Federation — Polaris as a Client of External Catalogs

In addition to being queried by external services, Polaris can itself delegate catalog operations to remote
Iceberg REST catalogs. This is the **federation** feature, where Polaris acts as a unified entry point while
the actual table metadata lives in an external system.

Federation enables several key capabilities:
- **Unified access control**: Polaris applies its RBAC model across both local and federated catalogs, providing
  a single security boundary.
- **Credential vending for federated catalogs**: Polaris can vend credentials for data stored in federated
  catalogs, so clients do not need direct access to the remote catalog's storage credentials.
- **Sub-catalog RBAC**: Fine-grained role-based access control can be applied to individual namespaces and
  tables within a federated catalog.

For full details, see [Iceberg REST Federation](federation/iceberg-rest-federation/).

## API Reference

| Use Case | Pattern | Endpoint | Method |
|----------|---------|----------|--------|
| Get applicable policies | Pull | `/polaris/v1/{catalog}/applicable-policies` | `GET` |
| Load a specific policy | Pull | `/polaris/v1/{catalog}/namespaces/{ns}/policies/{name}` | `GET` |
| List policies | Pull | `/polaris/v1/{catalog}/namespaces/{ns}/policies` | `GET` |
| Create a policy | Pull | `/polaris/v1/{catalog}/namespaces/{ns}/policies` | `POST` |
| Update a policy | Pull | `/polaris/v1/{catalog}/namespaces/{ns}/policies/{name}` | `PATCH` |
| Drop a policy | Pull | `/polaris/v1/{catalog}/namespaces/{ns}/policies/{name}` | `DELETE` |
| Attach a policy | Pull | `/polaris/v1/{catalog}/namespaces/{ns}/policies/{name}/mappings` | `PUT` |
| Detach a policy | Pull | `/polaris/v1/{catalog}/namespaces/{ns}/policies/{name}/mappings` | `POST` |
| Push table change notification | Push | `/v1/{catalog}/namespaces/{ns}/tables/{table}/notifications` | `POST` |
| Validate table location | Push | `/v1/{catalog}/namespaces/{ns}/tables/{table}/notifications` (type=VALIDATE) | `POST` |
| Create a generic table | Status | `/polaris/v1/{catalog}/namespaces/{ns}/generic-tables` | `POST` |
| Load a generic table | Status | `/polaris/v1/{catalog}/namespaces/{ns}/generic-tables/{table}` | `GET` |
| List generic tables | Status | `/polaris/v1/{catalog}/namespaces/{ns}/generic-tables` | `GET` |
| Drop a generic table | Status | `/polaris/v1/{catalog}/namespaces/{ns}/generic-tables/{table}` | `DELETE` |
| React to Polaris events | Event | `PolarisEventListener.onEvent()` | Java SPI |

For the complete API specifications, see:
- [Policy API](https://github.com/apache/polaris/blob/main/spec/polaris-catalog-apis/policy-apis.yaml)
- [Notification API](https://github.com/apache/polaris/blob/main/spec/polaris-catalog-apis/notifications-api.yaml)
- [Generic Table API](https://github.com/apache/polaris/blob/main/spec/polaris-catalog-apis/generic-tables-api.yaml)
