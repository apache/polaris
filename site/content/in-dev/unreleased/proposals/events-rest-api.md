---
title: Events REST API
linkTitle: Events REST API
weight: 100
---
<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Proposal: REST API for Querying Catalog Events

**Author:** Anand Sankaran
**Date:** 2026-03-02
**Status:** Draft Proposal
**Target:** Apache Polaris

---

## Abstract

This proposal defines a REST API endpoint for querying catalog events from Apache Polaris. The endpoint exposes data already being persisted via the existing JDBC persistence model (`events` table) and follows established Polaris API patterns.

**Note:** The Events API in this proposal is designed to align with the emerging [Iceberg Events API specification](https://github.com/apache/iceberg/pull/12584), which is nearing consensus in the Apache Iceberg community. This ensures forward compatibility and consistency with the broader Iceberg ecosystem.

---

## Table of Contents

1. [Motivation](#1-motivation)
2. [Use Cases](#2-use-cases)
3. [Design Principles](#3-design-principles)
4. [API Specification](#4-api-specification)
5. [Authorization](#5-authorization)
6. [OpenAPI Schema](#6-openapi-schema)
7. [Implementation Notes](#7-implementation-notes)
8. [Iceberg Events API Alignment](#8-iceberg-events-api-alignment)

---

## 1. Motivation

Apache Polaris currently persists catalog events to the database, but provides no REST API to query this data. Users must access the database directly to retrieve audit information.

Adding a read-only REST endpoint enables:
- Programmatic access to events without database credentials
- Integration with monitoring dashboards and alerting systems
- Consistent authorization via Polaris RBAC
- Pagination and filtering without writing SQL

---

## 2. Use Cases

### 2.1 Audit & Compliance
- Track who created/dropped/modified catalog objects
- Monitor administrative actions (credential rotation, grant changes)
- Generate compliance reports for access patterns

### 2.2 Catalog Federation
- Synchronize catalog state across distributed systems
- Enable CDC (Change Data Capture) for downstream consumers
- Support workflow triggering based on catalog changes

### 2.3 Debugging & Troubleshooting
- Investigate catalog state changes over time
- Correlate events with distributed traces (`otel_trace_id`, `otel_span_id`)
- Trace operations via `request_id`

---

## 3. Design Principles

| Principle | Rationale |
|-----------|-----------|
| **Iceberg Events API alignment** | Events API follows the [Iceberg Events API spec](https://github.com/apache/iceberg/pull/12584) for ecosystem compatibility |
| **POST for complex filtering** | Events API uses POST with request body (per Iceberg spec) to support complex filters (arrays, nested objects) |
| **Read-only semantics** | All endpoints are read-only; events are written via existing flows |
| **Consistent pagination** | Follow `continuation-token` pattern (Iceberg) |
| **Flexible filtering** | Time ranges, operation types, catalog objects - common query patterns |
| **RBAC integration** | Leverage existing Polaris authorization model |
| **Realm handling** | Process Polaris realms consistently with existing APIs; realm context is derived from the authenticated principal |
| **Stable envelope** | Polaris-owned stable envelope fields for resilient client integrations; type-specific payloads are versioned independently |

### 3.1 Stable Envelope

To reduce coupling to any single upstream schema (e.g., Iceberg) and keep client integrations resilient, the Events API SHOULD return records using a **stable envelope**: a small, Polaris-owned set of top-level fields that remain consistent across all event types, plus a versioned payload for the type-specific body.

The envelope enables clients to reliably paginate, deduplicate, and correlate records (request IDs / trace IDs) without needing to understand the full payload schema. Type-specific details (Iceberg-aligned or Polaris-specific) live under `payload`, identified by `payload.type` and `payload.version`.

#### Envelope Fields (Conceptual)

| Field | Description |
|-------|-------------|
| `id` | Unique identifier for the event |
| `timestampMs` | Event timestamp (epoch milliseconds) |
| `catalog` | Catalog identifier |
| `realm` | Realm identifier (if applicable) |
| `actor` | Principal/service + optional client metadata |
| `request` | Request context (requestId, trace/span IDs) |
| `object` | Resource identity: namespace/table/view identifiers and optional UUID/snapshotId |
| `payload` | `{ type, version, data, extensions? }` |

#### Compatibility / Evolution Rules

1. **Envelope is additive-only**: New envelope fields may be added as optional; existing envelope fields MUST NOT change meaning or type, and MUST NOT be removed.

2. **Breaking changes require versioning at the API boundary**: Any breaking envelope change requires a new major API version (e.g., `/v2/...`) rather than changing `/v1`.

3. **Payload is independently versioned**: `payload.type` selects the schema family (e.g., `iceberg.events`, `polaris.events`), and `payload.version` increments on breaking payload changes.

4. **Unknown payloads are safe to ignore**: Clients MUST treat unknown `payload.type` or higher `payload.version` as opaque and continue to operate using envelope fields (e.g., still paginate / display metadata).

5. **Payload changes within a version are additive-only**: Within a given `payload.type` + `payload.version`, new fields may be added as optional; removals/renames/type changes require a new `payload.version`.

6. **Flattening is a presentation choice**: The default representation SHOULD keep domain-specific structures nested under `payload.data`; alternative "flattened" views (if needed) should be offered as an explicit, separately-versioned representation rather than redefining the canonical schema.

---

## 4. API Specification

### 4.1 Endpoint Summary

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/events/v1/{prefix}` | Query events for a catalog |

> **Note:** The Events API uses a dedicated `/api/events/v1/` namespace to avoid URI path clashes with the [Iceberg Events API specification](https://github.com/apache/iceberg/pull/12584) until that spec is approved. The API design follows Iceberg Events API patterns for future compatibility.

### 4.2 Path Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `prefix` | string | Catalog prefix (typically the catalog name) |

### 4.3 Events API (Iceberg-Compatible)

The Events API follows the [Iceberg Events API specification](https://github.com/apache/iceberg/pull/12584) for ecosystem compatibility.

**Scope:** This endpoint is **catalog-scoped** (via `{prefix}`) and returns events describing changes to **catalog objects** (namespaces, tables, views) within that catalog. Realm-level administrative events (for example, principal lifecycle and credential rotation) are **out of scope** for `/api/events/v1/{prefix}` and would require a separate realm-scoped API surface.

Key design decisions from the Iceberg spec:

- **POST method**: Allows complex filtering with arrays and nested objects in the request body
- **Continuation token**: Opaque cursor for resumable pagination
- **Operation-centric model**: Events are structured around operations (create-table, update-table, etc.)
- **Custom extensions**: Support for `polaris-` prefixed custom operation types for Polaris catalog-scoped extensions

#### Request Body (`QueryEventsRequest`)

> **Note:** This request schema matches the [Iceberg Events API specification](https://github.com/apache/iceberg/pull/12584) for ecosystem compatibility.

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `continuation-token` | string | No | Opaque cursor to resume fetching from previous request |
| `page-size` | integer | No | Maximum events per page (server may return fewer) |
| `after-timestamp-ms` | long | No | Filter: events after this timestamp (inclusive) |
| `operation-types` | array[string] | No | Filter by operation types (see below) |
| `catalog-objects-by-name` | array[array[string]] | No | Filter by namespace/table/view names |
| `catalog-objects-by-id` | array[object] | No | Filter by table/view UUIDs |
| `object-types` | array[string] | No | Filter by object type: `namespace`, `table`, `view` |
| `custom-filters` | object | No | Implementation-specific filter extensions |

#### Standard Operation Types

| Operation Type | Description |
|----------------|-------------|
| `create-table` | Table created and committed |
| `register-table` | Existing table registered in catalog |
| `drop-table` | Table dropped |
| `update-table` | Table metadata updated |
| `rename-table` | Table renamed |
| `create-view` | View created |
| `drop-view` | View dropped |
| `update-view` | View updated |
| `rename-view` | View renamed |
| `create-namespace` | Namespace created |
| `update-namespace-properties` | Namespace properties updated |
| `drop-namespace` | Namespace dropped |

#### Polaris Custom Operation Types

For Polaris-specific events not covered by the Iceberg spec, use the `polaris-` prefix. To keep `/api/events/v1/{prefix}` catalog-scoped, these custom operations must be **catalog-scoped** as well.

| Custom Operation Type | Description |
|----------------------|-------------|
| `polaris-create-catalog-role` | Catalog role created |
| `polaris-grant-privilege` | Privilege granted |
| `polaris-revoke-privilege` | Privilege revoked |

### 4.4 Example Requests and Responses

#### Query Events (Iceberg-Compatible)

**Request:**
```http
POST /api/events/v1/my-catalog
Authorization: Bearer <token>
Content-Type: application/json

{
  "page-size": 2,
  "operation-types": ["create-table", "update-table"],
  "after-timestamp-ms": 1709251200000,
  "catalog-objects-by-name": [
    ["analytics", "events"]
  ],
  "object-types": ["table"]
}
```

**Response:**
```json
{
  "next-page-token": "eyJ0cyI6MTcwOTMzNzYxMjM0NSwiaWQiOiI1NTBlODQwMCJ9",
  "highest-processed-timestamp-ms": 1709337612345,
  "events": [
    {
      "event-id": "550e8400-e29b-41d4-a716-446655440000",
      "request-id": "req-12345",
      "request-event-count": 1,
      "timestamp-ms": 1709337612345,
      "actor": {
        "principal": "admin@example.com",
        "client-ip": "192.168.1.100"
      },
      "operation": {
        "operation-type": "create-table",
        "identifier": {
          "namespace": ["analytics", "events"],
          "name": "page_views"
        },
        "table-uuid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "updates": [
          {"action": "assign-uuid", "uuid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"},
          {"action": "set-current-schema", "schema-id": 0},
          {"action": "set-default-spec", "spec-id": 0}
        ]
      }
    },
    {
      "event-id": "661f9511-f30c-52e5-b827-557766551111",
      "request-id": "req-12346",
      "request-event-count": 1,
      "timestamp-ms": 1709337500000,
      "actor": {
        "principal": "etl-service@example.com"
      },
      "operation": {
        "operation-type": "update-table",
        "identifier": {
          "namespace": ["analytics", "events"],
          "name": "user_actions"
        },
        "table-uuid": "b2c3d4e5-f6a7-8901-bcde-f23456789012",
        "updates": [
          {"action": "add-snapshot", "snapshot-id": 123456789}
        ],
        "requirements": [
          {"type": "assert-table-uuid", "uuid": "b2c3d4e5-f6a7-8901-bcde-f23456789012"}
        ]
      }
    }
  ]
}
```

#### Query Events with Custom (Catalog-Scoped) Polaris Operations

**Request:**
```http
POST /api/events/v1/my-catalog
Authorization: Bearer <token>
Content-Type: application/json

{
  "page-size": 10,
  "operation-types": ["polaris-grant-privilege", "polaris-create-catalog-role"]
}
```

**Response:**
```json
{
  "next-page-token": "eyJ0cyI6MTcwOTMzODAwMDAwMH0=",
  "highest-processed-timestamp-ms": 1709338000000,
  "events": [
    {
      "event-id": "772f0622-g41d-63f6-c938-668877662222",
      "request-id": "req-admin-001",
      "request-event-count": 1,
      "timestamp-ms": 1709338000000,
      "actor": {
        "principal": "security-admin@example.com"
      },
      "operation": {
        "operation-type": "custom",
        "custom-type": "polaris-grant-privilege",
        "identifier": {
          "namespace": ["analytics", "events"],
          "name": "page_views"
        },
        "table-uuid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "privilege": "TABLE_READ_DATA",
        "grantee": "data-analyst-role"
      }
    }
  ]
}
```

---

## 5. Authorization

### 5.1 Required Privileges

This proposal introduces a **new dedicated privilege** for reading events, following the principle of **separation of duties**. This ensures that:

- Read-only audit/monitoring access does not require management permissions
- Fine-grained access control is possible for different operational roles

| Endpoint | Required Privilege | Scope | New Privilege? |
|----------|-------------------|-------|----------------|
| Query Events | `CATALOG_READ_EVENTS` | Catalog | **Yes** |

### 5.2 New Privilege Definition

| Privilege | Scope | Description |
|-----------|-------|-------------|
| `CATALOG_READ_EVENTS` | Catalog | Read-only access to catalog events (audit log). Does not grant any management capabilities. |

### 5.3 Rationale: Separation of Duties

Introducing a dedicated read-only privilege enables proper **separation of duties**:

| Use Case | Required Privilege | Why Not Reuse Existing? |
|----------|-------------------|------------------------|
| Security auditor reviewing catalog changes | `CATALOG_READ_EVENTS` | Should not require `CATALOG_MANAGE_METADATA` (management access) |
| Catalog admin | `CATALOG_MANAGE_METADATA` implies `CATALOG_READ_EVENTS` | Admins can see all events |

### 5.4 Privilege Hierarchy

The new privilege fits into the existing hierarchy as follows:

```
CATALOG_MANAGE_METADATA
  └── CATALOG_READ_EVENTS (implied)
```

This means:
- Users with `CATALOG_MANAGE_METADATA` automatically have `CATALOG_READ_EVENTS`
- But the reverse is **not** true: `CATALOG_READ_EVENTS` does not grant management access

### 5.5 Implementation Notes

New privileges require:
1. Adding entries to `PolarisPrivilege` enum
2. Updating the privilege hierarchy in the authorizer
3. Adding privilege checks in the new API endpoint

---

## 6. OpenAPI Schema

> **Note:** The OpenAPI specifications below are embedded in this proposal for review context. Upon approval, these should be extracted into separate files for ease of processing and proper integration:
> - **Events API** → `spec/events-service.yml` (new dedicated service spec with base path `/api/events/v1/`)

### 6.1 Events API

Add the following to a new `spec/events-service.yml`:

> **Note:** The Events API uses a dedicated `/api/events/v1/` namespace to avoid URI path clashes with the Iceberg REST Catalog events API until that spec is approved. The request/response schemas follow Iceberg Events API patterns for future compatibility.

```yaml
paths:
  /v1/{prefix}:
    parameters:
      - $ref: '#/components/parameters/prefix'
    post:
      tags:
        - Catalog API
      summary: Get events for changes to catalog objects
      description: >
        Returns a sequence of changes to catalog objects (tables, namespaces, views)
        that allows clients to efficiently track metadata modifications without polling
        individual resources. Consumers track their progress through a continuation-token,
        enabling resumable synchronization after downtime or errors.

        This endpoint primarily supports use cases like catalog federation, workflow
        triggering, and basic audit capabilities.

        Consumers should be prepared to handle 410 Gone responses when requested sequences
        are outside the server's retention window. Consumers should also de-duplicate
        received events based on the event's `event-id`.
      operationId: getEvents
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/QueryEventsRequest'
      responses:
        '200':
          description: A sequence of change events to catalog objects
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/QueryEventsResponse'
        '400':
          $ref: '#/components/responses/BadRequestErrorResponse'
        '401':
          $ref: '#/components/responses/UnauthorizedResponse'
        '403':
          $ref: '#/components/responses/ForbiddenResponse'
        '410':
          description: Gone - The requested offset is no longer available
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorModel'
        '503':
          $ref: '#/components/responses/ServiceUnavailableResponse'
        '5XX':
          $ref: '#/components/responses/ServerErrorResponse'
```

### 6.2 Events API Schemas (Iceberg-Compatible)

Add these schemas to `spec/events-service.yml`:

```yaml
components:
  schemas:
    QueryEventsRequest:
      type: object
      properties:
        continuation-token:
          type: string
          description: >
            A continuation token to resume fetching events from a previous request.
            If not provided, events are fetched from the beginning of the event log
            subject to other filters.
        page-size:
          type: integer
          format: int32
          description: >
            The maximum number of events to return in a single response.
            Servers may return less results than requested.
        after-timestamp-ms:
          type: integer
          format: int64
          description: >
            The timestamp in milliseconds to start consuming events from (inclusive).
        operation-types:
          type: array
          items:
            $ref: "#/components/schemas/OperationType"
          description: Filter events by operation type.
        catalog-objects-by-name:
          type: array
          items:
            $ref: "#/components/schemas/CatalogObjectIdentifier"
          description: >
            Filter events by catalog objects referenced by name (namespaces, tables, views).
            For namespaces, events for all containing objects are returned recursively.
        catalog-objects-by-id:
          type: array
          items:
            $ref: "#/components/schemas/CatalogObjectUuid"
          description: Filter events by table/view UUIDs.
        object-types:
          type: array
          items:
            type: string
            enum: [namespace, table, view]
          description: Filter events by catalog object type.
        custom-filters:
          type: object
          additionalProperties: true
          description: Implementation-specific filter extensions.

    QueryEventsResponse:
      type: object
      required:
        - next-page-token
        - highest-processed-timestamp-ms
        - events
      properties:
        next-page-token:
          type: string
          description: >
            An opaque continuation token to fetch the next page of events.
        highest-processed-timestamp-ms:
          type: integer
          format: int64
          description: >
            The highest event timestamp processed when generating this response.
        events:
          type: array
          items:
            $ref: "#/components/schemas/Event"

    Event:
      type: object
      required:
        - event-id
        - request-id
        - request-event-count
        - timestamp-ms
        - operation
      properties:
        event-id:
          type: string
          description: Unique ID of this event. Clients should deduplicate based on this ID.
        request-id:
          type: string
          description: >
            Opaque ID of the request this event belongs to. Events from the same
            request share this ID.
        request-event-count:
          type: integer
          description: >
            Total number of events generated by this request.
        timestamp-ms:
          type: integer
          format: int64
          description: Timestamp when this event occurred (epoch milliseconds).
        actor:
          type: object
          additionalProperties: true
          description: >
            The actor who performed the operation (e.g., user, service account).
            Content is implementation-specific.
        operation:
          type: object
          description: The operation that was performed.
          discriminator:
            propertyName: operation-type
            mapping:
              create-table: "#/components/schemas/CreateTableOperation"
              register-table: "#/components/schemas/RegisterTableOperation"
              drop-table: "#/components/schemas/DropTableOperation"
              update-table: "#/components/schemas/UpdateTableOperation"
              rename-table: "#/components/schemas/RenameTableOperation"
              create-view: "#/components/schemas/CreateViewOperation"
              drop-view: "#/components/schemas/DropViewOperation"
              update-view: "#/components/schemas/UpdateViewOperation"
              rename-view: "#/components/schemas/RenameViewOperation"
              create-namespace: "#/components/schemas/CreateNamespaceOperation"
              update-namespace-properties: "#/components/schemas/UpdateNamespacePropertiesOperation"
              drop-namespace: "#/components/schemas/DropNamespaceOperation"
              custom: "#/components/schemas/CustomOperation"

    OperationType:
      type: string
      description: >
        Defines the type of operation. Clients should ignore unknown operation types.
      anyOf:
        - type: string
          enum:
            - create-table
            - register-table
            - drop-table
            - update-table
            - rename-table
            - create-view
            - drop-view
            - update-view
            - rename-view
            - create-namespace
            - update-namespace-properties
            - drop-namespace
        - $ref: '#/components/schemas/CustomOperationType'

    CustomOperationType:
      type: string
      description: >
        Custom operation type for Polaris catalog-specific extensions.
        Must start with 'polaris-' followed by an implementation-specific identifier.
      pattern: '^polaris-[a-zA-Z0-9-_.]+$'

    CustomOperation:
      type: object
      description: Extension point for catalog-specific operations (e.g., Polaris privileges).
      required:
        - operation-type
        - custom-type
      properties:
        operation-type:
          type: string
          const: "custom"
        custom-type:
          $ref: '#/components/schemas/CustomOperationType'
        identifier:
          $ref: "#/components/schemas/TableIdentifier"
          description: Table or view identifier this operation applies to, if applicable.
        namespace:
          $ref: "#/components/schemas/Namespace"
          description: Namespace this operation applies to, if applicable.
        table-uuid:
          type: string
          format: uuid
        view-uuid:
          type: string
          format: uuid
      additionalProperties: true

    CatalogObjectIdentifier:
      type: array
      items:
        type: string
      description: Reference to a named object in the catalog (namespace, table, or view).
      example: ["accounting", "tax"]

    CatalogObjectUuid:
      type: object
      required:
        - uuid
        - type
      properties:
        uuid:
          type: string
          description: The UUID of the catalog object.
        type:
          type: string
          enum: [table, view]
```

---

## 7. Implementation Notes

### 7.1 Prerequisite: Extend Event Persistence Layer

> **Important:** The current `PolarisPersistenceEventListener` in Apache Polaris only persists **two event types**: `AFTER_CREATE_TABLE` and `AFTER_CREATE_CATALOG`. All other events are ignored. For the Events API to be useful, the persistence layer must be extended to capture all relevant mutation events.

#### Current State

The existing event listener (`PolarisPersistenceEventListener.java`) has a limited switch statement:

```java
public void onEvent(PolarisEvent event) {
  switch (event.type()) {
    case AFTER_CREATE_TABLE -> handleAfterCreateTable(event);
    case AFTER_CREATE_CATALOG -> handleAfterCreateCatalog(event);
    default -> {
      // Other events not handled by this listener
    }
  }
}
```

#### Required Changes

The persistence layer needs to be extended to capture all `AFTER_*` mutation events that should be exposed via the Events API. For v1, this is limited to **catalog-object mutations** (tables, views, namespaces) within a catalog.

| Category | Events to Add |
|----------|---------------|
| **Table Operations** | `AFTER_UPDATE_TABLE`, `AFTER_DROP_TABLE`, `AFTER_RENAME_TABLE`, `AFTER_REGISTER_TABLE` |
| **View Operations** | `AFTER_CREATE_VIEW`, `AFTER_DROP_VIEW`, `AFTER_REPLACE_VIEW`, `AFTER_RENAME_VIEW` |
| **Namespace Operations** | `AFTER_CREATE_NAMESPACE`, `AFTER_UPDATE_NAMESPACE_PROPERTIES`, `AFTER_DROP_NAMESPACE` |

**Note:** Realm-level administrative events (for example, principal lifecycle and credential rotation) are intentionally out of scope for `/api/events/v1/{prefix}`.

**Note:** Read-only operations (`AFTER_LOAD_*`, `AFTER_LIST_*`, `AFTER_GET_*`, `AFTER_CHECK_EXISTS_*`) should **not** be persisted for the Events API as they do not represent catalog mutations.

#### Implementation Approach

1. **Phase 1**: Extend `PolarisPersistenceEventListener` to handle all Iceberg-standard operations (tables, views, namespaces)
2. **Phase 2**: Optionally add support for **catalog-scoped** Polaris extensions (for example, catalog-role and grant changes) using `polaris-*` custom operation types
3. **Phase 3**: Implement the Events REST API on top of the persisted data

### 7.2 Database Queries

The endpoints will query existing tables with appropriate filtering and pagination:

```sql
-- List events with cursor-based pagination
SELECT * FROM events
WHERE realm_id = ?
  AND catalog_id = ?
  AND (timestamp_ms, event_id) < (?, ?)  -- cursor
  AND event_type = ?                      -- optional filter
  AND timestamp_ms >= ?                   -- optional filter
  AND timestamp_ms < ?                    -- optional filter
ORDER BY timestamp_ms DESC, event_id DESC
LIMIT ?;
```

### 7.3 Recommended Indexes

```sql
-- Events indexes
CREATE INDEX IF NOT EXISTS idx_events_catalog_ts
    ON events(realm_id, catalog_id, timestamp_ms DESC, event_id DESC);
CREATE INDEX IF NOT EXISTS idx_events_type
    ON events(realm_id, catalog_id, event_type, timestamp_ms DESC);
```

### 7.4 Files to Modify

| File | Changes |
|------|---------|
| `spec/events-service.yml` | **New file** - Events API paths and schemas |
| `api/events-service/` | **New** - Generated Events API interfaces |
| `runtime/service/.../events/` | **New** - Events service implementation |
| `polaris-core/.../persistence/BasePersistence.java` | Add read methods |
| `persistence/relational-jdbc/.../JdbcBasePersistenceImpl.java` | Query implementations |

### 7.5 Pagination Token Format

Internal format (base64-encoded JSON, opaque to clients):

```json
{
  "ts": 1709337612345,
  "id": "550e8400-e29b-41d4-a716-446655440000"
}
```

### 7.6 Mapping PolarisEventType to Iceberg Events API

Polaris internally uses a `PolarisEventType` enum that distinguishes between `BEFORE_*` and `AFTER_*` events for each operation. This section explains how these internal events map to the Iceberg Events API.

#### 7.6.1 Design Decision: Only AFTER Events are Exposed

The Iceberg Events API represents **completed operations** that have been committed to the catalog. Therefore:

| Internal Event Pattern | Exposed via API? | Rationale |
|------------------------|------------------|-----------|
| `AFTER_*` events | **Yes** | Represent successful, committed operations |
| `BEFORE_*` events | **No** | Represent intent, not outcome; may fail after firing |

**Why not expose BEFORE events?**

1. **Semantic mismatch**: The Iceberg Events API is designed for change data capture (CDC) and audit logs of *completed* changes.
2. **Consistency**: Exposing `BEFORE_*` events could lead to consumers seeing "phantom" operations that never actually occurred.
3. **Use case alignment**: The primary use cases (audit, federation, workflow triggers) all require knowing what *actually happened*.
4. **Internal vs external**: `BEFORE_*` events serve internal purposes (request filtering, rate limiting, pre-validation hooks).

#### 7.6.2 Mapping AFTER Events to Operation Types

The mapping follows a straightforward pattern - the `AFTER_` prefix is stripped and the remaining name maps to the Iceberg operation type:

| Polaris `PolarisEventType` | Iceberg `operation-type` | Notes |
|----------------------------|--------------------------|-------|
| **Standard Iceberg Operations** | | |
| `AFTER_CREATE_TABLE` | `create-table` | Direct mapping |
| `AFTER_REGISTER_TABLE` | `register-table` | Direct mapping |
| `AFTER_DROP_TABLE` | `drop-table` | Direct mapping |
| `AFTER_UPDATE_TABLE` | `update-table` | Includes schema evolution, property changes |
| `AFTER_RENAME_TABLE` | `rename-table` | Direct mapping |
| `AFTER_CREATE_VIEW` | `create-view` | Direct mapping |
| `AFTER_DROP_VIEW` | `drop-view` | Direct mapping |
| `AFTER_REPLACE_VIEW` | `update-view` | View replacement maps to update |
| `AFTER_RENAME_VIEW` | `rename-view` | Direct mapping |
| `AFTER_CREATE_NAMESPACE` | `create-namespace` | Direct mapping |
| `AFTER_UPDATE_NAMESPACE_PROPERTIES` | `update-namespace-properties` | Direct mapping |
| `AFTER_DROP_NAMESPACE` | `drop-namespace` | Direct mapping |
| **Polaris Custom Operations (catalog-scoped)** | | Use `custom` type with `polaris-*` |
| `AFTER_CREATE_CATALOG_ROLE` | `custom` (`polaris-create-catalog-role`) | RBAC |
| `AFTER_ADD_GRANT_TO_CATALOG_ROLE` | `custom` (`polaris-grant-privilege`) | RBAC |
| `AFTER_REVOKE_GRANT_FROM_CATALOG_ROLE` | `custom` (`polaris-revoke-privilege`) | RBAC |

---

## 8. Iceberg Events API Alignment

This section documents the alignment with the [Iceberg Events API specification](https://github.com/apache/iceberg/pull/12584) and explains the rationale for design decisions.

### 8.1 Why Align with Iceberg Events API?

The Iceberg Events API is an emerging specification that is nearing consensus in the Apache Iceberg community. Aligning Polaris with this specification provides:

1. **Ecosystem Compatibility**: Clients built for the Iceberg Events API will work with Polaris without modification
2. **Future-Proofing**: Avoids breaking changes when the Iceberg spec is finalized
3. **Tooling Interoperability**: Monitoring tools, federation services, and workflow triggers can work across Iceberg-compatible catalogs
4. **Reduced Cognitive Load**: Developers familiar with Iceberg don't need to learn a new API

### 8.2 Key Design Decisions from Iceberg Spec

| Decision | Iceberg Spec Approach | Rationale |
|----------|----------------------|-----------|
| **HTTP Method** | `POST` (not `GET`) | Allows complex filtering with arrays and nested objects in request body |
| **API Path** | `/v1/{prefix}/events` | Part of Iceberg REST Catalog, not a separate management API |
| **Pagination** | `continuation-token` | Opaque cursor that encodes server state; resumable after downtime |
| **Event Structure** | Operation-centric with discriminator | Each event contains a typed `operation` with operation-specific fields |
| **Operation Types** | Standardized enum + custom prefix extensions | Standard types for Iceberg operations; custom prefix for catalog-specific extensions |
| **Actor Field** | Generic object (implementation-specific) | Flexibility for different auth models (users, service accounts, etc.) |
| **Error Handling** | `410 Gone` for expired offsets | Explicit signal when continuation token is outside retention window |

### 8.3 Polaris-Specific Extensions

Polaris extends the Iceberg Events API using the `custom` operation type with `polaris-*` prefixed custom types:

| Custom Type | Polaris Event | Description |
|-------------|---------------|-------------|
| `polaris-create-catalog-role` | `AFTER_CREATE_CATALOG_ROLE` | Catalog role created |
| `polaris-grant-privilege` | `AFTER_ADD_GRANT_TO_CATALOG_ROLE` | Privilege granted to role |
| `polaris-revoke-privilege` | `AFTER_REVOKE_GRANT_FROM_CATALOG_ROLE` | Privilege revoked |

### 8.4 References

- **Iceberg Events API Proposal**: [Google Doc](https://docs.google.com/document/d/1WtIsNGVX75-_MsQIOJhXLAWg6IbplV4-DkLllQEiFT8/edit)
- **Iceberg Events API PR**: [apache/iceberg#12584](https://github.com/apache/iceberg/pull/12584)
- **Iceberg REST Catalog Spec**: [rest-catalog-open-api.yaml](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)

---

## Open Questions

1. **Event Retention**: What is the default retention period for events? Should it be configurable?
2. **Consistency Guarantees**: What ordering and delivery guarantees should Polaris provide for the Events API?

---

## References

- Database schema: `persistence/relational-jdbc/src/main/resources/postgres/schema-v4.sql`
- Event types: `runtime/service/src/main/java/org/apache/polaris/service/events/PolarisEventType.java`
- Iceberg Events API PR: https://github.com/apache/iceberg/pull/12584
- Iceberg Events API Design Doc: https://docs.google.com/document/d/1WtIsNGVX75-_MsQIOJhXLAWg6IbplV4-DkLllQEiFT8/edit

