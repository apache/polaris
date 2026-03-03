---
title: Observability REST API
linkTitle: Observability REST API
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

# Proposal: REST API for Querying Table Metrics and Events

**Author:** Anand Sankaran
**Date:** 2026-03-02
**Status:** Draft Proposal
**Target:** Apache Polaris

---

## Abstract

This proposal defines REST API endpoints for querying table metrics and catalog events from Apache Polaris. The endpoints expose data already being persisted via the existing JDBC persistence model (`events`, `scan_metrics_report`, `commit_metrics_report` tables) and follow established Polaris API patterns.

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

Apache Polaris currently persists table metrics (scan reports, commit reports) and catalog events to the database, but provides no REST API to query this data. Users must access the database directly to retrieve metrics or audit information.

Adding read-only REST endpoints enables:
- Programmatic access to metrics without database credentials
- Integration with monitoring dashboards and alerting systems
- Consistent authorization via Polaris RBAC
- Pagination and filtering without writing SQL

---

## 2. Use Cases

### 2.1 Table Health Monitoring
- Track write patterns: files added/removed per commit, record counts, duration trends
- Identify tables with high commit frequency or unusually large commits
- Detect issues indicating need for compaction (many small files) or optimization

### 2.2 Query Performance Analysis
- Understand read patterns: files scanned vs skipped, planning duration
- Identify inefficient queries with low manifest/file pruning ratios
- Correlate performance with filter expressions and projected columns

### 2.3 Capacity Planning & Chargeback
- Aggregate metrics by table, namespace, or principal over time
- Track storage growth trends (`total_file_size_bytes`)
- Attribute usage to teams/users via `principal_name`

### 2.4 Debugging & Troubleshooting
- Correlate metrics with distributed traces (`otel_trace_id`, `otel_span_id`)
- Investigate specific commits by `snapshot_id`
- Trace operations via `request_id`

### 2.5 Audit & Compliance
- Track who created/dropped/modified catalog objects
- Monitor administrative actions (credential rotation, grant changes)
- Generate compliance reports for access patterns

---

## 3. Design Principles

| Principle | Rationale |
|-----------|-----------|
| **Iceberg Events API alignment** | Events API follows the [Iceberg Events API spec](https://github.com/apache/iceberg/pull/12584) for ecosystem compatibility |
| **Dedicated metrics-reports namespace** | Metrics APIs use `/api/metrics-reports/v1/...` to separate from management and catalog APIs |
| **POST for complex filtering** | Events API uses POST with request body (per Iceberg spec) to support complex filters (arrays, nested objects) |
| **Read-only semantics** | All endpoints are read-only; metrics/events are written via existing flows |
| **Consistent pagination** | Follow `continuation-token` pattern (Iceberg) and `pageToken` pattern (Polaris APIs) |
| **Flexible filtering** | Time ranges, operation types, catalog objects - common query patterns |
| **RBAC integration** | Leverage existing Polaris authorization model |

---

## 4. API Specification

### 4.1 Endpoint Summary

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/catalog/v1/{prefix}/events` | Query events for a catalog (Iceberg-compatible) |
| GET | `/api/metrics-reports/v1/catalogs/{catalogName}/namespaces/{namespace}/tables/{table}/scan-metrics` | List scan metrics for a table |
| GET | `/api/metrics-reports/v1/catalogs/{catalogName}/namespaces/{namespace}/tables/{table}/commit-metrics` | List commit metrics for a table |

> **Note:** The Events API uses POST (not GET) and follows the Iceberg REST Catalog path structure (`/api/catalog/v1/{prefix}/events`) for compatibility with the [Iceberg Events API specification](https://github.com/apache/iceberg/pull/12584). The metrics APIs use a dedicated `/api/metrics-reports/v1/` namespace since they expose pre-populated records rather than managing catalog state - a server that doesn't support catalog management may still expose metrics reports.

### 4.2 Path Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `prefix` | string | Catalog prefix (typically the catalog name) |
| `catalogName` | string | Name of the catalog |
| `namespace` | string | Namespace (URL-encoded, multi-level separated by `%1F`) |
| `table` | string | Table name |

### 4.3 Events API (Iceberg-Compatible)

The Events API follows the [Iceberg Events API specification](https://github.com/apache/iceberg/pull/12584) for ecosystem compatibility. Key design decisions from the Iceberg spec:

- **POST method**: Allows complex filtering with arrays and nested objects in the request body
- **Continuation token**: Opaque cursor for resumable pagination
- **Operation-centric model**: Events are structured around operations (create-table, update-table, etc.)
- **Custom extensions**: Support for `x-` prefixed custom operation types for Polaris-specific events

#### Request Body (`QueryEventsRequest`)

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

For Polaris-specific events not covered by the Iceberg spec, use the `x-` prefix convention:

| Custom Operation Type | Description |
|----------------------|-------------|
| `x-polaris-create-catalog-role` | Catalog role created |
| `x-polaris-grant-privilege` | Privilege granted |
| `x-polaris-rotate-credentials` | Principal credentials rotated |
| `x-polaris-create-policy` | Policy created |
| `x-polaris-attach-policy` | Policy attached to resource |

### 4.4 Query Parameters (Metrics APIs)

#### List Scan Metrics (`/.../tables/{table}/scan-metrics`)

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `pageToken` | string | No | - | Cursor for pagination |
| `pageSize` | integer | No | 100 | Results per page (max: 1000) |
| `snapshotId` | long | No | - | Filter by snapshot ID |
| `principalName` | string | No | - | Filter by principal |
| `timestampFrom` | long | No | - | Start of time range (epoch ms) |
| `timestampTo` | long | No | - | End of time range (epoch ms) |

#### List Commit Metrics (`/.../tables/{table}/commit-metrics`)

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `pageToken` | string | No | - | Cursor for pagination |
| `pageSize` | integer | No | 100 | Results per page (max: 1000) |
| `snapshotId` | long | No | - | Filter by snapshot ID |
| `operation` | string | No | - | Filter by operation: `append`, `overwrite`, `delete`, `replace` |
| `principalName` | string | No | - | Filter by principal |
| `timestampFrom` | long | No | - | Start of time range (epoch ms) |
| `timestampTo` | long | No | - | End of time range (epoch ms) |

### 4.5 Example Requests and Responses

#### Query Events (Iceberg-Compatible)

**Request:**
```http
POST /api/catalog/v1/my-catalog/events
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

#### Query Events with Custom Polaris Operations

**Request:**
```http
POST /api/catalog/v1/my-catalog/events
Authorization: Bearer <token>
Content-Type: application/json

{
  "page-size": 10,
  "operation-types": ["x-polaris-grant-privilege", "x-polaris-rotate-credentials"]
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
        "custom-type": "x-polaris-grant-privilege",
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

#### List Scan Metrics

**Request:**
```http
GET /api/metrics-reports/v1/catalogs/my-catalog/namespaces/analytics%1Fevents/tables/page_views/scan-metrics?pageSize=2&timestampFrom=1709251200000
Authorization: Bearer <token>
```

**Response:**
```json
{
  "nextPageToken": null,
  "reports": [
    {
      "reportId": "scan-001-abc123",
      "catalogId": 12345,
      "tableId": 67890,
      "timestampMs": 1709337612345,
      "principalName": "analyst@example.com",
      "requestId": "req-scan-001",
      "otelTraceId": "abc123def456789012345678901234",
      "otelSpanId": "def456789012",
      "snapshotId": 1234567890123,
      "schemaId": 0,
      "filterExpression": "event_date >= '2024-03-01'",
      "projectedFieldIds": "1,2,3,5,8",
      "projectedFieldNames": "event_id,user_id,event_type,timestamp,page_url",
      "resultDataFiles": 150,
      "resultDeleteFiles": 5,
      "totalFileSizeBytes": 1073741824,
      "totalDataManifests": 12,
      "totalDeleteManifests": 2,
      "scannedDataManifests": 8,
      "scannedDeleteManifests": 2,
      "skippedDataManifests": 4,
      "skippedDeleteManifests": 0,
      "skippedDataFiles": 45,
      "skippedDeleteFiles": 0,
      "totalPlanningDurationMs": 250,
      "equalityDeleteFiles": 3,
      "positionalDeleteFiles": 2,
      "indexedDeleteFiles": 0,
      "totalDeleteFileSizeBytes": 52428800
    }
  ]
}
```

#### List Commit Metrics

**Request:**
```http
GET /api/metrics-reports/v1/catalogs/my-catalog/namespaces/analytics%1Fevents/tables/page_views/commit-metrics?operation=append&pageSize=2
Authorization: Bearer <token>
```

**Response:**
```json
{
  "nextPageToken": "eyJ0cyI6MTcwOTMzNzcwMDAwMCwiaWQiOiJjb21taXQtMDAyIn0=",
  "reports": [
    {
      "reportId": "commit-001-xyz789",
      "catalogId": 12345,
      "tableId": 67890,
      "timestampMs": 1709337800000,
      "principalName": "etl-service@example.com",
      "requestId": "req-commit-001",
      "otelTraceId": "xyz789abc123456789012345678901",
      "otelSpanId": "abc123456789",
      "snapshotId": 1234567890124,
      "sequenceNumber": 42,
      "operation": "append",
      "addedDataFiles": 10,
      "removedDataFiles": 0,
      "totalDataFiles": 160,
      "addedDeleteFiles": 0,
      "removedDeleteFiles": 0,
      "totalDeleteFiles": 5,
      "addedEqualityDeleteFiles": 0,
      "removedEqualityDeleteFiles": 0,
      "addedPositionalDeleteFiles": 0,
      "removedPositionalDeleteFiles": 0,
      "addedRecords": 100000,
      "removedRecords": 0,
      "totalRecords": 15000000,
      "addedFileSizeBytes": 104857600,
      "removedFileSizeBytes": 0,
      "totalFileSizeBytes": 1178599424,
      "totalDurationMs": 5000,
      "attempts": 1
    }
  ]
}
```

---

## 5. Authorization

### 5.1 Required Privileges

This proposal introduces **new dedicated privileges** for reading observability data, following the principle of **separation of duties**. This ensures that:

- Read-only audit/monitoring access does not require management permissions
- Monitoring tools can access metrics without requiring data read access
- Fine-grained access control is possible for different operational roles

| Endpoint | Required Privilege | Scope | New Privilege? |
|----------|-------------------|-------|----------------|
| Query Events | `CATALOG_READ_EVENTS` | Catalog | **Yes** |
| List Scan Metrics | `TABLE_READ_METRICS` | Table | **Yes** |
| List Commit Metrics | `TABLE_READ_METRICS` | Table | **Yes** |

### 5.2 New Privilege Definitions

| Privilege | Scope | Description |
|-----------|-------|-------------|
| `CATALOG_READ_EVENTS` | Catalog | Read-only access to catalog events (audit log). Does not grant any management capabilities. |
| `TABLE_READ_METRICS` | Table | Read-only access to table scan and commit metrics. Does not grant access to table data. |

### 5.3 Rationale: Separation of Duties

Introducing dedicated read-only privileges enables proper **separation of duties**:

| Use Case | Required Privilege | Why Not Reuse Existing? |
|----------|-------------------|------------------------|
| Security auditor reviewing catalog changes | `CATALOG_READ_EVENTS` | Should not require `CATALOG_MANAGE_METADATA` (management access) |
| Monitoring tool collecting table metrics | `TABLE_READ_METRICS` | Should not require `TABLE_READ_DATA` (data access) |
| Data analyst with table access | `TABLE_READ_DATA` implies `TABLE_READ_METRICS` | Users who can read data can also see metrics about their queries |
| Catalog admin | `CATALOG_MANAGE_METADATA` implies `CATALOG_READ_EVENTS` | Admins can see all events |

### 5.4 Privilege Hierarchy

The new privileges fit into the existing hierarchy as follows:

```
CATALOG_MANAGE_METADATA
  └── CATALOG_READ_EVENTS (implied)

TABLE_FULL_METADATA / TABLE_READ_DATA
  └── TABLE_READ_METRICS (implied)
```

This means:
- Users with `CATALOG_MANAGE_METADATA` automatically have `CATALOG_READ_EVENTS`
- Users with `TABLE_READ_DATA` automatically have `TABLE_READ_METRICS`
- But the reverse is **not** true: `CATALOG_READ_EVENTS` does not grant management access, and `TABLE_READ_METRICS` does not grant data access

### 5.5 Implementation Notes

New privileges require:
1. Adding entries to `PolarisPrivilege` enum
2. Updating the privilege hierarchy in the authorizer
3. Adding privilege checks in the new API endpoints

---

## 6. OpenAPI Schema

> **Note:** The OpenAPI specifications below are embedded in this proposal for review context. Upon approval, these should be extracted into separate files for ease of processing and proper integration:
> - **Events API** → `spec/rest-catalog-open-api.yaml` (extending Iceberg REST Catalog spec)
> - **Metrics Reports API** → `spec/metrics-reports-service.yml` (new dedicated service spec with base path `/api/metrics-reports/v1/`)

### 6.1 Events API (Iceberg REST Catalog Extension)

Add the following to `spec/rest-catalog-open-api.yaml` (aligned with Iceberg Events API spec):

```yaml
paths:
  /v1/{prefix}/events:
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

### 6.2 Metrics APIs (New Metrics Reports Service)

Add the following to a new `spec/metrics-reports-service.yml` (or extend existing management service):

> **Note:** The metrics APIs use `/api/metrics-reports/v1/` as the base path, separate from the management API. This reflects that metrics reports are read-only access to pre-populated data, not catalog management operations.

```yaml
paths:
  /catalogs/{catalogName}/namespaces/{namespace}/tables/{table}/scan-metrics:
    parameters:
      - $ref: '#/components/parameters/catalogName'
      - $ref: '#/components/parameters/namespace'
      - $ref: '#/components/parameters/table'
    get:
      operationId: listTableScanMetrics
      summary: List scan metrics for a table
      tags:
        - Observability
      parameters:
        - name: pageToken
          in: query
          schema:
            type: string
        - name: pageSize
          in: query
          schema:
            type: integer
            minimum: 1
            maximum: 1000
            default: 100
        - name: snapshotId
          in: query
          schema:
            type: integer
            format: int64
        - name: principalName
          in: query
          schema:
            type: string
        - name: timestampFrom
          in: query
          schema:
            type: integer
            format: int64
        - name: timestampTo
          in: query
          schema:
            type: integer
            format: int64
      responses:
        '200':
          description: Paginated list of scan metrics
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ListScanMetricsResponse'
        '403':
          description: Insufficient privileges
        '404':
          description: Table not found

  /catalogs/{catalogName}/namespaces/{namespace}/tables/{table}/commit-metrics:
    parameters:
      - $ref: '#/components/parameters/catalogName'
      - $ref: '#/components/parameters/namespace'
      - $ref: '#/components/parameters/table'
    get:
      operationId: listTableCommitMetrics
      summary: List commit metrics for a table
      tags:
        - Observability
      parameters:
        - name: pageToken
          in: query
          schema:
            type: string
        - name: pageSize
          in: query
          schema:
            type: integer
            minimum: 1
            maximum: 1000
            default: 100
        - name: snapshotId
          in: query
          schema:
            type: integer
            format: int64
        - name: operation
          in: query
          schema:
            type: string
        - name: principalName
          in: query
          schema:
            type: string
        - name: timestampFrom
          in: query
          schema:
            type: integer
            format: int64
        - name: timestampTo
          in: query
          schema:
            type: integer
            format: int64
      responses:
        '200':
          description: Paginated list of commit metrics
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ListCommitMetricsResponse'
        '403':
          description: Insufficient privileges
        '404':
          description: Table not found
```

### 6.3 Events API Schemas (Iceberg-Compatible)

Add these schemas to `spec/rest-catalog-open-api.yaml`:

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
        Custom operation type for catalog-specific extensions.
        Must start with 'x-' followed by an implementation-specific identifier.
      pattern: '^x-[a-zA-Z0-9-_.]+$'

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

### 6.4 Metrics API Schemas (Polaris Management Service)

Add these schemas to `spec/polaris-management-service.yml`:

```yaml
components:
  schemas:
    ScanMetricsReport:
      type: object
      required:
        - reportId
        - catalogId
        - tableId
        - timestampMs
      properties:
        reportId:
          type: string
        catalogId:
          type: integer
          format: int64
        tableId:
          type: integer
          format: int64
        timestampMs:
          type: integer
          format: int64
        principalName:
          type: string
        requestId:
          type: string
        otelTraceId:
          type: string
          description: OpenTelemetry trace ID
        otelSpanId:
          type: string
          description: OpenTelemetry span ID
        snapshotId:
          type: integer
          format: int64
        schemaId:
          type: integer
        filterExpression:
          type: string
        projectedFieldIds:
          type: string
        projectedFieldNames:
          type: string
        resultDataFiles:
          type: integer
          format: int64
        resultDeleteFiles:
          type: integer
          format: int64
        totalFileSizeBytes:
          type: integer
          format: int64
        totalDataManifests:
          type: integer
          format: int64
        totalDeleteManifests:
          type: integer
          format: int64
        scannedDataManifests:
          type: integer
          format: int64
        scannedDeleteManifests:
          type: integer
          format: int64
        skippedDataManifests:
          type: integer
          format: int64
        skippedDeleteManifests:
          type: integer
          format: int64
        skippedDataFiles:
          type: integer
          format: int64
        skippedDeleteFiles:
          type: integer
          format: int64
        totalPlanningDurationMs:
          type: integer
          format: int64
        equalityDeleteFiles:
          type: integer
          format: int64
        positionalDeleteFiles:
          type: integer
          format: int64
        indexedDeleteFiles:
          type: integer
          format: int64
        totalDeleteFileSizeBytes:
          type: integer
          format: int64

    ListScanMetricsResponse:
      type: object
      properties:
        nextPageToken:
          type: string
        reports:
          type: array
          items:
            $ref: '#/components/schemas/ScanMetricsReport'

    CommitMetricsReport:
      type: object
      required:
        - reportId
        - catalogId
        - tableId
        - timestampMs
        - snapshotId
        - operation
      properties:
        reportId:
          type: string
        catalogId:
          type: integer
          format: int64
        tableId:
          type: integer
          format: int64
        timestampMs:
          type: integer
          format: int64
        principalName:
          type: string
        requestId:
          type: string
        otelTraceId:
          type: string
        otelSpanId:
          type: string
        snapshotId:
          type: integer
          format: int64
        sequenceNumber:
          type: integer
          format: int64
        operation:
          type: string
          description: Commit operation (append, overwrite, delete, replace)
        addedDataFiles:
          type: integer
          format: int64
        removedDataFiles:
          type: integer
          format: int64
        totalDataFiles:
          type: integer
          format: int64
        addedDeleteFiles:
          type: integer
          format: int64
        removedDeleteFiles:
          type: integer
          format: int64
        totalDeleteFiles:
          type: integer
          format: int64
        addedEqualityDeleteFiles:
          type: integer
          format: int64
        removedEqualityDeleteFiles:
          type: integer
          format: int64
        addedPositionalDeleteFiles:
          type: integer
          format: int64
        removedPositionalDeleteFiles:
          type: integer
          format: int64
        addedRecords:
          type: integer
          format: int64
        removedRecords:
          type: integer
          format: int64
        totalRecords:
          type: integer
          format: int64
        addedFileSizeBytes:
          type: integer
          format: int64
        removedFileSizeBytes:
          type: integer
          format: int64
        totalFileSizeBytes:
          type: integer
          format: int64
        totalDurationMs:
          type: integer
          format: int64
        attempts:
          type: integer

    ListCommitMetricsResponse:
      type: object
      properties:
        nextPageToken:
          type: string
        reports:
          type: array
          items:
            $ref: '#/components/schemas/CommitMetricsReport'
```

---

## 7. Implementation Notes

### 7.1 Database Queries

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

-- List scan metrics
SELECT * FROM scan_metrics_report
WHERE realm_id = ?
  AND catalog_id = ?
  AND table_id = ?
  AND timestamp_ms >= ?
  AND timestamp_ms < ?
ORDER BY timestamp_ms DESC, report_id DESC
LIMIT ?;
```

### 7.2 Recommended Indexes

```sql
-- Events indexes
CREATE INDEX IF NOT EXISTS idx_events_catalog_ts
    ON events(realm_id, catalog_id, timestamp_ms DESC, event_id DESC);
CREATE INDEX IF NOT EXISTS idx_events_type
    ON events(realm_id, catalog_id, event_type, timestamp_ms DESC);

-- Metrics indexes (may already exist)
CREATE INDEX IF NOT EXISTS idx_scan_report_lookup
    ON scan_metrics_report(realm_id, catalog_id, table_id, timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_commit_report_lookup
    ON commit_metrics_report(realm_id, catalog_id, table_id, timestamp_ms DESC);
```

### 7.3 Files to Modify

| File | Changes |
|------|---------|
| `spec/rest-catalog-open-api.yaml` | Add Events API paths and schemas (Iceberg-compatible) |
| `spec/metrics-reports-service.yml` | **New file** - Metrics Reports API paths and schemas |
| `api/iceberg-service/` | Generated Events API interfaces |
| `api/metrics-reports-service/` | **New** - Generated Metrics Reports API interfaces |
| `runtime/service/.../catalog/` | Events service implementation |
| `runtime/service/.../metrics/` | **New** - Metrics reports service implementation |
| `polaris-core/.../persistence/BasePersistence.java` | Add read methods |
| `persistence/relational-jdbc/.../JdbcBasePersistenceImpl.java` | Query implementations |

### 7.4 Pagination Token Format

Internal format (base64-encoded JSON, opaque to clients):

```json
{
  "ts": 1709337612345,
  "id": "550e8400-e29b-41d4-a716-446655440000"
}
```

### 7.5 Mapping PolarisEventType to Iceberg Events API

Polaris internally uses a `PolarisEventType` enum that distinguishes between `BEFORE_*` and `AFTER_*` events for each operation (e.g., `BEFORE_CREATE_TABLE` and `AFTER_CREATE_TABLE`). This section explains how these internal events map to the Iceberg Events API.

#### 7.5.1 Design Decision: Only AFTER Events are Exposed

The Iceberg Events API represents **completed operations** that have been committed to the catalog. Therefore:

| Internal Event Pattern | Exposed via API? | Rationale |
|------------------------|------------------|-----------|
| `AFTER_*` events | **Yes** | Represent successful, committed operations |
| `BEFORE_*` events | **No** | Represent intent, not outcome; may fail after firing |

**Why not expose BEFORE events?**

1. **Semantic mismatch**: The Iceberg Events API is designed for change data capture (CDC) and audit logs of *completed* changes. `BEFORE_*` events fire before validation and persistence, so they may represent operations that ultimately fail.

2. **Consistency**: Exposing `BEFORE_*` events could lead to consumers seeing "phantom" operations that never actually occurred.

3. **Use case alignment**: The primary use cases (audit, federation, workflow triggers) all require knowing what *actually happened*, not what was *attempted*.

4. **Internal vs external**: `BEFORE_*` events serve internal purposes (request filtering, rate limiting, pre-validation hooks) and are not meaningful to external consumers.

#### 7.5.2 Mapping AFTER Events to Operation Types

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
| **Polaris Custom Operations** | | Use `custom` type with `x-polaris-*` |
| `AFTER_CREATE_CATALOG` | `custom` (`x-polaris-create-catalog`) | Catalog-level, not in Iceberg spec |
| `AFTER_DELETE_CATALOG` | `custom` (`x-polaris-delete-catalog`) | Catalog-level |
| `AFTER_CREATE_PRINCIPAL` | `custom` (`x-polaris-create-principal`) | Access management |
| `AFTER_DELETE_PRINCIPAL` | `custom` (`x-polaris-delete-principal`) | Access management |
| `AFTER_ROTATE_CREDENTIALS` | `custom` (`x-polaris-rotate-credentials`) | Security operation |
| `AFTER_CREATE_PRINCIPAL_ROLE` | `custom` (`x-polaris-create-principal-role`) | RBAC |
| `AFTER_CREATE_CATALOG_ROLE` | `custom` (`x-polaris-create-catalog-role`) | RBAC |
| `AFTER_ADD_GRANT_TO_CATALOG_ROLE` | `custom` (`x-polaris-grant-privilege`) | RBAC |
| `AFTER_REVOKE_GRANT_FROM_CATALOG_ROLE` | `custom` (`x-polaris-revoke-privilege`) | RBAC |
| `AFTER_CREATE_POLICY` | `custom` (`x-polaris-create-policy`) | Policy management |
| `AFTER_ATTACH_POLICY` | `custom` (`x-polaris-attach-policy`) | Policy management |
| `AFTER_CREATE_GENERIC_TABLE` | `custom` (`x-polaris-create-generic-table`) | Generic table support |

#### 7.5.3 Read-Only Operations: Not Exposed

Events for read-only operations are **not exposed** via the Events API because they do not represent catalog mutations:

| Excluded Event Types | Reason |
|----------------------|--------|
| `AFTER_GET_CATALOG`, `AFTER_LIST_CATALOGS` | Read-only, no state change |
| `AFTER_LOAD_TABLE`, `AFTER_LIST_TABLES`, `AFTER_CHECK_EXISTS_TABLE` | Read-only |
| `AFTER_LOAD_NAMESPACE_METADATA`, `AFTER_LIST_NAMESPACES` | Read-only |
| `AFTER_LOAD_VIEW`, `AFTER_LIST_VIEWS` | Read-only |
| `AFTER_GET_CONFIG`, `AFTER_LOAD_CREDENTIALS` | Configuration/credential reads |
| `AFTER_LIST_*` (all list operations) | Read-only enumeration |

#### 7.5.4 Implementation: Event Filtering

The event persistence layer should filter events before storing them for the Events API:

```java
// Events eligible for the REST API (completed mutations only)
private static final Set<PolarisEventType> EXPOSED_EVENT_TYPES = Set.of(
    // Standard Iceberg operations
    AFTER_CREATE_TABLE, AFTER_UPDATE_TABLE, AFTER_DROP_TABLE,
    AFTER_RENAME_TABLE, AFTER_REGISTER_TABLE,
    AFTER_CREATE_VIEW, AFTER_DROP_VIEW, AFTER_REPLACE_VIEW, AFTER_RENAME_VIEW,
    AFTER_CREATE_NAMESPACE, AFTER_UPDATE_NAMESPACE_PROPERTIES, AFTER_DROP_NAMESPACE,
    // Polaris custom operations
    AFTER_CREATE_CATALOG, AFTER_DELETE_CATALOG, AFTER_UPDATE_CATALOG,
    AFTER_CREATE_PRINCIPAL, AFTER_DELETE_PRINCIPAL, AFTER_UPDATE_PRINCIPAL,
    AFTER_ROTATE_CREDENTIALS, AFTER_RESET_CREDENTIALS,
    AFTER_CREATE_PRINCIPAL_ROLE, AFTER_DELETE_PRINCIPAL_ROLE,
    AFTER_CREATE_CATALOG_ROLE, AFTER_DELETE_CATALOG_ROLE,
    AFTER_ADD_GRANT_TO_CATALOG_ROLE, AFTER_REVOKE_GRANT_FROM_CATALOG_ROLE,
    AFTER_ASSIGN_PRINCIPAL_ROLE, AFTER_REVOKE_PRINCIPAL_ROLE,
    AFTER_ASSIGN_CATALOG_ROLE_TO_PRINCIPAL_ROLE, AFTER_REVOKE_CATALOG_ROLE_FROM_PRINCIPAL_ROLE,
    AFTER_CREATE_POLICY, AFTER_UPDATE_POLICY, AFTER_DROP_POLICY,
    AFTER_ATTACH_POLICY, AFTER_DETACH_POLICY,
    AFTER_CREATE_GENERIC_TABLE, AFTER_DROP_GENERIC_TABLE
);

public boolean shouldPersistForEventsApi(PolarisEventType eventType) {
    return EXPOSED_EVENT_TYPES.contains(eventType);
}
```

---

## Appendix A: Polaris Internal Event Types Reference

Polaris internal event types are categorized by code ranges. These are mapped to Iceberg-compatible operation types when exposed via the Events API:

| Range | Category | Internal Event Type | Iceberg Operation Type |
|-------|----------|---------------------|------------------------|
| 100-109 | Catalog | `AFTER_CREATE_CATALOG` | `custom` (`x-polaris-create-catalog`) |
| | | `AFTER_DELETE_CATALOG` | `custom` (`x-polaris-delete-catalog`) |
| 200-217 | Catalog Role | `AFTER_CREATE_CATALOG_ROLE` | `custom` (`x-polaris-create-catalog-role`) |
| | | `AFTER_ADD_GRANT_TO_CATALOG_ROLE` | `custom` (`x-polaris-grant-privilege`) |
| 300-319 | Principal | `AFTER_CREATE_PRINCIPAL` | `custom` (`x-polaris-create-principal`) |
| | | `AFTER_ROTATE_CREDENTIALS` | `custom` (`x-polaris-rotate-credentials`) |
| 400-417 | Principal Role | `AFTER_CREATE_PRINCIPAL_ROLE` | `custom` (`x-polaris-create-principal-role`) |
| 500-511 | Namespace | `AFTER_CREATE_NAMESPACE` | `create-namespace` |
| | | `AFTER_UPDATE_NAMESPACE_PROPERTIES` | `update-namespace-properties` |
| | | `AFTER_DROP_NAMESPACE` | `drop-namespace` |
| 600-617 | Table | `AFTER_CREATE_TABLE` | `create-table` |
| | | `AFTER_UPDATE_TABLE` | `update-table` |
| | | `AFTER_DROP_TABLE` | `drop-table` |
| | | `AFTER_RENAME_TABLE` | `rename-table` |
| | | `AFTER_REGISTER_TABLE` | `register-table` |
| 700-715 | View | `AFTER_CREATE_VIEW` | `create-view` |
| | | `AFTER_UPDATE_VIEW` | `update-view` |
| | | `AFTER_DROP_VIEW` | `drop-view` |
| | | `AFTER_RENAME_VIEW` | `rename-view` |
| 1200-1215 | Policy | `AFTER_CREATE_POLICY` | `custom` (`x-polaris-create-policy`) |
| | | `AFTER_ATTACH_POLICY` | `custom` (`x-polaris-attach-policy`) |
| 1300-1307 | Generic Table | `AFTER_CREATE_GENERIC_TABLE` | `custom` (`x-polaris-create-generic-table`) |

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
| **Operation Types** | Standardized enum + `x-` prefix extensions | Standard types for Iceberg operations; custom prefix for catalog-specific extensions |
| **Actor Field** | Generic object (implementation-specific) | Flexibility for different auth models (users, service accounts, etc.) |
| **Error Handling** | `410 Gone` for expired offsets | Explicit signal when continuation token is outside retention window |

### 8.3 Polaris-Specific Extensions

Polaris extends the Iceberg Events API using the `custom` operation type with `x-polaris-*` prefixed custom types:

| Custom Type | Polaris Event | Description |
|-------------|---------------|-------------|
| `x-polaris-create-catalog` | `AFTER_CREATE_CATALOG` | Catalog created |
| `x-polaris-create-catalog-role` | `AFTER_CREATE_CATALOG_ROLE` | Catalog role created |
| `x-polaris-grant-privilege` | `AFTER_ADD_GRANT_TO_CATALOG_ROLE` | Privilege granted to role |
| `x-polaris-revoke-privilege` | `AFTER_REMOVE_GRANT_FROM_CATALOG_ROLE` | Privilege revoked |
| `x-polaris-create-principal` | `AFTER_CREATE_PRINCIPAL` | Principal created |
| `x-polaris-rotate-credentials` | `AFTER_ROTATE_CREDENTIALS` | Credentials rotated |
| `x-polaris-create-policy` | `AFTER_CREATE_POLICY` | Policy created |
| `x-polaris-attach-policy` | `AFTER_ATTACH_POLICY` | Policy attached to resource |

### 8.4 Mapping Polaris Internal Events to Iceberg Operations

| Polaris Event Type | Iceberg Operation Type |
|-------------------|------------------------|
| `AFTER_CREATE_TABLE` | `create-table` |
| `AFTER_UPDATE_TABLE` | `update-table` |
| `AFTER_DROP_TABLE` | `drop-table` |
| `AFTER_RENAME_TABLE` | `rename-table` |
| `AFTER_REGISTER_TABLE` | `register-table` |
| `AFTER_CREATE_VIEW` | `create-view` |
| `AFTER_UPDATE_VIEW` / `AFTER_REPLACE_VIEW` | `update-view` |
| `AFTER_DROP_VIEW` | `drop-view` |
| `AFTER_RENAME_VIEW` | `rename-view` |
| `AFTER_CREATE_NAMESPACE` | `create-namespace` |
| `AFTER_UPDATE_NAMESPACE_PROPERTIES` | `update-namespace-properties` |
| `AFTER_DROP_NAMESPACE` | `drop-namespace` |
| Other Polaris events | `custom` with `x-polaris-*` type |

### 8.5 References

- **Iceberg Events API Proposal**: [Google Doc](https://docs.google.com/document/d/1WtIsNGVX75-_MsQIOJhXLAWg6IbplV4-DkLllQEiFT8/edit)
- **Iceberg Events API PR**: [apache/iceberg#12584](https://github.com/apache/iceberg/pull/12584)
- **Iceberg REST Catalog Spec**: [rest-catalog-open-api.yaml](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)

---

## Open Questions

1. **Aggregations**: Are aggregated metrics views needed (e.g., daily summaries)?
2. **Event Retention**: What is the default retention period for events? Should it be configurable?
3. **Consistency Guarantees**: What ordering and delivery guarantees should Polaris provide for the Events API?

## Resolved Questions

1. ~~**Privileges**: Use existing privileges or introduce new `READ_EVENTS`/`READ_METRICS`?~~
   - **Resolution**: Introduce new dedicated privileges (`CATALOG_READ_EVENTS`, `TABLE_READ_METRICS`) to support separation of duties. See [Section 5](#5-authorization) for details.

---

## References

- Database schema: `persistence/relational-jdbc/src/main/resources/postgres/schema-v4.sql`
- Event types: `runtime/service/src/main/java/org/apache/polaris/service/events/PolarisEventType.java`
- Metrics persistence: `runtime/service/src/main/java/org/apache/polaris/service/reporting/PersistingMetricsReporter.java`
- Iceberg Events API PR: https://github.com/apache/iceberg/pull/12584
- Iceberg Events API Design Doc: https://docs.google.com/document/d/1WtIsNGVX75-_MsQIOJhXLAWg6IbplV4-DkLllQEiFT8/edit

