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

---

## Table of Contents

1. [Motivation](#1-motivation)
2. [Use Cases](#2-use-cases)
3. [Design Principles](#3-design-principles)
4. [API Specification](#4-api-specification)
5. [Authorization](#5-authorization)
6. [OpenAPI Schema](#6-openapi-schema)
7. [Implementation Notes](#7-implementation-notes)

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
| **Management API namespace** | Use `/api/management/v1/...` to separate from Iceberg REST Catalog paths |
| **Read-only endpoints** | Only GET methods; metrics/events are written via existing flows |
| **Consistent pagination** | Follow existing `pageToken`/`nextPageToken` patterns |
| **Flexible filtering** | Time ranges, principal, snapshot - common query patterns |
| **RBAC integration** | Leverage existing Polaris authorization model |

---

## 4. API Specification

### 4.1 Endpoint Summary

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/management/v1/catalogs/{catalogName}/events` | List events for a catalog |
| GET | `/api/management/v1/catalogs/{catalogName}/events/{eventId}` | Get a specific event |
| GET | `/api/management/v1/catalogs/{catalogName}/namespaces/{namespace}/tables/{table}/scan-metrics` | List scan metrics for a table |
| GET | `/api/management/v1/catalogs/{catalogName}/namespaces/{namespace}/tables/{table}/commit-metrics` | List commit metrics for a table |

### 4.2 Path Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `catalogName` | string | Name of the catalog |
| `namespace` | string | Namespace (URL-encoded, multi-level separated by `%1F`) |
| `table` | string | Table name |
| `eventId` | string | Unique event identifier |

### 4.3 Query Parameters

#### List Events (`/catalogs/{catalogName}/events`)

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `pageToken` | string | No | - | Cursor for pagination (from previous response) |
| `pageSize` | integer | No | 100 | Results per page (max: 1000) |
| `eventType` | string | No | - | Filter by event type (e.g., `AFTER_CREATE_TABLE`) |
| `resourceType` | string | No | - | Filter by resource: `CATALOG`, `NAMESPACE`, `TABLE`, `VIEW` |
| `resourceIdentifier` | string | No | - | Filter by resource identifier (exact match) |
| `principalName` | string | No | - | Filter by principal who triggered the event |
| `timestampFrom` | long | No | - | Start of time range (epoch milliseconds, inclusive) |
| `timestampTo` | long | No | - | End of time range (epoch milliseconds, exclusive) |

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

### 4.4 Example Requests and Responses

#### List Events

**Request:**
```http
GET /api/management/v1/catalogs/my-catalog/events?pageSize=2&eventType=AFTER_CREATE_TABLE&timestampFrom=1709251200000
Authorization: Bearer <token>
```

**Response:**
```json
{
  "nextPageToken": "eyJ0cyI6MTcwOTMzNzYxMjM0NSwiaWQiOiI1NTBlODQwMCJ9",
  "events": [
    {
      "eventId": "550e8400-e29b-41d4-a716-446655440000",
      "catalogId": "my-catalog",
      "requestId": "req-12345",
      "eventType": "AFTER_CREATE_TABLE",
      "timestampMs": 1709337612345,
      "principalName": "admin@example.com",
      "resourceType": "TABLE",
      "resourceIdentifier": "analytics.events.page_views",
      "additionalProperties": {
        "table-uuid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
      }
    },
    {
      "eventId": "661f9511-f30c-52e5-b827-557766551111",
      "catalogId": "my-catalog",
      "requestId": "req-12346",
      "eventType": "AFTER_CREATE_TABLE",
      "timestampMs": 1709337500000,
      "principalName": "etl-service@example.com",
      "resourceType": "TABLE",
      "resourceIdentifier": "analytics.events.user_actions",
      "additionalProperties": {
        "table-uuid": "b2c3d4e5-f6a7-8901-bcde-f23456789012"
      }
    }
  ]
}
```

#### Get Single Event

**Request:**
```http
GET /api/management/v1/catalogs/my-catalog/events/550e8400-e29b-41d4-a716-446655440000
Authorization: Bearer <token>
```

**Response:**
```json
{
  "eventId": "550e8400-e29b-41d4-a716-446655440000",
  "catalogId": "my-catalog",
  "requestId": "req-12345",
  "eventType": "AFTER_CREATE_TABLE",
  "timestampMs": 1709337612345,
  "principalName": "admin@example.com",
  "resourceType": "TABLE",
  "resourceIdentifier": "analytics.events.page_views",
  "additionalProperties": {
    "table-uuid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "traceparent": "00-abc123def456789012345678901234-def456789012-01"
  }
}
```

#### List Scan Metrics

**Request:**
```http
GET /api/management/v1/catalogs/my-catalog/namespaces/analytics%1Fevents/tables/page_views/scan-metrics?pageSize=2&timestampFrom=1709251200000
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
GET /api/management/v1/catalogs/my-catalog/namespaces/analytics%1Fevents/tables/page_views/commit-metrics?operation=append&pageSize=2
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

| Endpoint | Required Privilege | Scope |
|----------|-------------------|-------|
| List/Get Events | `CATALOG_MANAGE_METADATA` | Catalog |
| List Scan Metrics | `TABLE_READ_DATA` | Table |
| List Commit Metrics | `TABLE_READ_DATA` | Table |

### 5.2 Rationale

- **Events** contain catalog-wide audit information and should require catalog-level administrative access
- **Metrics** are table-specific and align with read access since they describe query patterns and commit history
- This follows the principle of least privilege while enabling common use cases

### 5.3 Alternative: New Privileges

If finer-grained control is desired, new privileges could be introduced:

| New Privilege | Description |
|---------------|-------------|
| `CATALOG_READ_EVENTS` | Read-only access to catalog events |
| `TABLE_READ_METRICS` | Read-only access to table metrics |

---

## 6. OpenAPI Schema

Add the following to `spec/polaris-management-service.yml`:

### 6.1 Paths

```yaml
paths:
  /catalogs/{catalogName}/events:
    parameters:
      - $ref: '#/components/parameters/catalogName'
    get:
      operationId: listCatalogEvents
      summary: List events for a catalog
      description: Returns a paginated list of events with optional filtering
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
        - name: eventType
          in: query
          schema:
            type: string
        - name: resourceType
          in: query
          schema:
            type: string
            enum: [CATALOG, NAMESPACE, TABLE, VIEW]
        - name: resourceIdentifier
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
          description: Paginated list of events
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ListEventsResponse'
        '403':
          description: Insufficient privileges
        '404':
          description: Catalog not found

  /catalogs/{catalogName}/events/{eventId}:
    parameters:
      - $ref: '#/components/parameters/catalogName'
      - name: eventId
        in: path
        required: true
        schema:
          type: string
    get:
      operationId: getEvent
      summary: Get a specific event
      tags:
        - Observability
      responses:
        '200':
          description: The requested event
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/CatalogEvent'
        '403':
          description: Insufficient privileges
        '404':
          description: Event or catalog not found

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

### 6.2 Schemas

```yaml
components:
  schemas:
    CatalogEvent:
      type: object
      required:
        - eventId
        - catalogId
        - eventType
        - timestampMs
        - resourceType
        - resourceIdentifier
      properties:
        eventId:
          type: string
          description: Unique event identifier
        catalogId:
          type: string
          description: Catalog where the event occurred
        requestId:
          type: string
          description: Request ID that triggered this event
        eventType:
          type: string
          description: Event type (e.g., AFTER_CREATE_TABLE)
        timestampMs:
          type: integer
          format: int64
          description: Event timestamp (epoch milliseconds)
        principalName:
          type: string
          description: Principal who triggered the event
        resourceType:
          type: string
          enum: [CATALOG, NAMESPACE, TABLE, VIEW]
        resourceIdentifier:
          type: string
          description: Fully qualified resource identifier
        additionalProperties:
          type: object
          additionalProperties:
            type: string
          description: Event-specific metadata

    ListEventsResponse:
      type: object
      properties:
        nextPageToken:
          type: string
          description: Token for next page (null if no more results)
        events:
          type: array
          items:
            $ref: '#/components/schemas/CatalogEvent'

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
| `spec/polaris-management-service.yml` | Add paths and schemas |
| `api/management-service/` | Generated API interfaces |
| `runtime/service/.../admin/` | Service implementation |
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

---

## Appendix: Event Types Reference

Events are categorized by code ranges:

| Range | Category | Examples |
|-------|----------|----------|
| 100-109 | Catalog | `AFTER_CREATE_CATALOG`, `AFTER_DELETE_CATALOG` |
| 200-217 | Catalog Role | `AFTER_CREATE_CATALOG_ROLE`, `AFTER_ADD_GRANT_TO_CATALOG_ROLE` |
| 300-319 | Principal | `AFTER_CREATE_PRINCIPAL`, `AFTER_ROTATE_CREDENTIALS` |
| 400-417 | Principal Role | `AFTER_CREATE_PRINCIPAL_ROLE`, `AFTER_ASSIGN_CATALOG_ROLE_TO_PRINCIPAL_ROLE` |
| 500-511 | Namespace | `AFTER_CREATE_NAMESPACE`, `AFTER_DROP_NAMESPACE` |
| 600-617 | Table | `AFTER_CREATE_TABLE`, `AFTER_UPDATE_TABLE`, `AFTER_DROP_TABLE` |
| 700-715 | View | `AFTER_CREATE_VIEW`, `AFTER_REPLACE_VIEW` |
| 1200-1215 | Policy | `AFTER_CREATE_POLICY`, `AFTER_ATTACH_POLICY` |
| 1300-1307 | Generic Table | `AFTER_CREATE_GENERIC_TABLE` |

---

## Open Questions

1. **Aggregations**: Are aggregated metrics views needed (e.g., daily summaries)?
2. **Privileges**: Use existing privileges or introduce new `READ_EVENTS`/`READ_METRICS`?

---

## References

- Database schema: `persistence/relational-jdbc/src/main/resources/postgres/schema-v4.sql`
- Event types: `runtime/service/src/main/java/org/apache/polaris/service/events/PolarisEventType.java`
- Metrics persistence: `runtime/service/src/main/java/org/apache/polaris/service/reporting/PersistingMetricsReporter.java`

