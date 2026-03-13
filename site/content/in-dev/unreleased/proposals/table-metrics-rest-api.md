---
title: Table Metrics REST API
linkTitle: Table Metrics REST API
weight: 110
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

# Proposal: REST API for Querying Table Metrics

**Author:** Anand Sankaran
**Date:** 2026-03-02
**Status:** Draft Proposal
**Target:** Apache Polaris

---

## Abstract

This proposal defines REST API endpoints for querying table metrics (scan reports, commit reports) from Apache Polaris. The endpoints expose data already being persisted via the existing JDBC persistence model (`scan_metrics_report`, `commit_metrics_report` tables) and follow established Polaris API patterns.

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

Apache Polaris currently persists table metrics (scan reports, commit reports) to the database, but provides no REST API to query this data. Users must access the database directly to retrieve metrics information.

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

---

## 3. Design Principles

| Principle | Rationale |
|-----------|-----------|
| **Dedicated metrics-reports namespace** | Metrics APIs use `/api/metrics-reports/v1/...` to separate from management and catalog APIs |
| **Read-only semantics** | All endpoints are read-only; metrics are written via existing flows |
| **Consistent pagination** | Follow `pageToken` pattern (Polaris APIs) |
| **Flexible filtering** | Time ranges, snapshot IDs, principals - common query patterns |
| **RBAC integration** | Leverage existing Polaris authorization model |
| **Realm handling** | Process Polaris realms consistently with existing APIs; realm context is derived from the authenticated principal |
| **Stable envelope** | Polaris-owned stable envelope fields for resilient client integrations; type-specific payloads are versioned independently |

### 3.1 Stable Envelope

To reduce coupling to any single upstream schema and keep client integrations resilient, the Metrics API SHOULD return records using a **stable envelope**: a small, Polaris-owned set of top-level fields that remain consistent across all metric report types, plus a versioned payload for the type-specific body.

The envelope enables clients to reliably paginate, deduplicate, and correlate records (request IDs / trace IDs) without needing to understand the full payload schema. Type-specific details live under `payload`, identified by `payload.type` and `payload.version`.

#### Envelope Fields (Conceptual)

| Field | Description |
|-------|-------------|
| `id` | Unique identifier for the report |
| `timestampMs` | Report timestamp (epoch milliseconds) |
| `catalog` | Catalog identifier |
| `realm` | Realm identifier (if applicable) |
| `actor` | Principal/service + optional client metadata |
| `request` | Request context (requestId, trace/span IDs) |
| `object` | Resource identity: namespace/table identifiers and optional UUID/snapshotId |
| `payload` | `{ type, version, data, extensions? }` |

#### Compatibility / Evolution Rules

1. **Envelope is additive-only**: New envelope fields may be added as optional; existing envelope fields MUST NOT change meaning or type, and MUST NOT be removed.

2. **Breaking changes require versioning at the API boundary**: Any breaking envelope change requires a new major API version (e.g., `/v2/...`) rather than changing `/v1`.

3. **Payload is independently versioned**: `payload.type` selects the schema family (e.g., `iceberg.metrics.scan`, `iceberg.metrics.commit`), and `payload.version` increments on breaking payload changes.

4. **Unknown payloads are safe to ignore**: Clients MUST treat unknown `payload.type` or higher `payload.version` as opaque and continue to operate using envelope fields (e.g., still paginate / display metadata).

5. **Payload changes within a version are additive-only**: Within a given `payload.type` + `payload.version`, new fields may be added as optional; removals/renames/type changes require a new `payload.version`.

6. **Flattening is a presentation choice**: The default representation SHOULD keep domain-specific structures nested under `payload.data`; alternative "flattened" views (if needed) should be offered as an explicit, separately-versioned representation rather than redefining the canonical schema.

---

## 4. API Specification

### 4.1 Endpoint Summary

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/metrics-reports/v1/catalogs/{catalogName}/namespaces/{namespace}/tables/{table}` | List metrics for a table (type specified via query parameter) |

> **Note:** The metrics API uses a dedicated `/api/metrics-reports/v1/` namespace since it exposes pre-populated records rather than managing catalog state.

### 4.2 Path Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `catalogName` | string | Name of the catalog |
| `namespace` | string | Namespace (URL-encoded, multi-level separated by `%1F`) |
| `table` | string | Table name |

### 4.3 Query Parameters

#### List Table Metrics (`/.../tables/{table}`)

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `metricType` | string | **Yes** | - | Type of metrics to retrieve: `scan` or `commit` |
| `pageToken` | string | No | - | Cursor for pagination |
| `pageSize` | integer | No | 100 | Results per page (max: 1000) |
| `snapshotId` | long | No | - | Filter by snapshot ID |
| `principalName` | string | No | - | Filter by principal |
| `timestampFrom` | long | No | - | Start of time range (epoch ms) |
| `timestampTo` | long | No | - | End of time range (epoch ms) |

> **Note:** The `metricType` parameter is required. This design allows for future extensibility as new metric types are added (e.g., compaction metrics, maintenance metrics) without requiring new endpoints.

**Non-goals (v1):** This endpoint is intentionally limited to **paged retrieval** of persisted metrics reports with basic filtering (primarily by time range and identifiers). It does not aim to be a general-purpose metrics query system (no aggregation/group-by, no derived computations, no complex query language); richer analytics are expected to be handled by exporting/sinking these reports to an external observability system.

### 4.4 Example Requests and Responses

#### List Metrics (Scan)

**Request:**
```http
GET /api/metrics-reports/v1/catalogs/my-catalog/namespaces/analytics%1Fevents/tables/page_views?metricType=scan&pageSize=2&timestampFrom=1709251200000
Authorization: Bearer <token>
```

**Response:**
```json
{
  "nextPageToken": null,
  "metricType": "scan",
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

#### List Metrics (Commit)

**Request:**
```http
GET /api/metrics-reports/v1/catalogs/my-catalog/namespaces/analytics%1Fevents/tables/page_views?metricType=commit&pageSize=2
Authorization: Bearer <token>
```

**Response:**
```json
{
  "nextPageToken": "eyJ0cyI6MTcwOTMzNzcwMDAwMCwiaWQiOiJjb21taXQtMDAyIn0=",
  "metricType": "commit",
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

This proposal introduces a **new dedicated privilege** for reading table metrics, following the principle of **separation of duties**. This ensures that:

- Monitoring tools can access metrics without requiring data read access
- Fine-grained access control is possible for different operational roles

| Endpoint | Required Privilege | Scope | New Privilege? |
|----------|-------------------|-------|----------------|
| List Scan Metrics | `TABLE_READ_METRICS` | Table | **Yes** |
| List Commit Metrics | `TABLE_READ_METRICS` | Table | **Yes** |

### 5.2 New Privilege Definition

| Privilege | Scope | Description |
|-----------|-------|-------------|
| `TABLE_READ_METRICS` | Table | Read-only access to table scan and commit metrics. Does not grant access to table data. |

### 5.3 Rationale: Separation of Duties

Introducing a dedicated read-only privilege enables proper **separation of duties**:

| Use Case | Required Privilege | Why Not Reuse Existing? |
|----------|-------------------|------------------------|
| Monitoring tool collecting table metrics | `TABLE_READ_METRICS` | Should not require `TABLE_READ_DATA` (data access) |
| Data analyst with table access | `TABLE_READ_DATA` implies `TABLE_READ_METRICS` | Users who can read data can also see metrics about their queries |

### 5.4 Privilege Hierarchy

The new privilege fits into the existing hierarchy as follows:

```
TABLE_FULL_METADATA / TABLE_READ_DATA
  └── TABLE_READ_METRICS (implied)
```

This means:
- Users with `TABLE_READ_DATA` automatically have `TABLE_READ_METRICS`
- But the reverse is **not** true: `TABLE_READ_METRICS` does not grant data access

### 5.5 Implementation Notes

New privileges require:
1. Adding entries to `PolarisPrivilege` enum
2. Updating the privilege hierarchy in the authorizer
3. Adding privilege checks in the new API endpoint

---

## 6. OpenAPI Schema

> **Note:** The OpenAPI specifications below are embedded in this proposal for review context. Upon approval, these should be extracted into separate files for ease of processing and proper integration:
> - **Metrics Reports API** → `spec/metrics-reports-service.yml` (new dedicated service spec with base path `/api/metrics-reports/v1/`)

### 6.1 Metrics API (New Metrics Reports Service)

Add the following to a new `spec/metrics-reports-service.yml`:

> **Note:** The metrics API uses `/api/metrics-reports/v1/` as the base path, separate from the management API. This reflects that metrics reports are read-only access to pre-populated data, not catalog management operations.

```yaml
paths:
  /catalogs/{catalogName}/namespaces/{namespace}/tables/{table}:
    parameters:
      - $ref: '#/components/parameters/catalogName'
      - $ref: '#/components/parameters/namespace'
      - $ref: '#/components/parameters/table'
    get:
      operationId: listTableMetrics
      summary: List metrics for a table
      description: >
        Returns metrics reports for the specified table. The type of metrics
        (scan or commit) must be specified via the required metricType parameter.
        This unified endpoint supports future extensibility as new metric types
        are added.
      tags:
        - Observability
      parameters:
        - name: metricType
          in: query
          required: true
          description: Type of metrics to retrieve
          schema:
            type: string
            enum: [scan, commit]
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
          description: Paginated list of metrics reports
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ListMetricsResponse'
        '400':
          description: Bad request (e.g., missing metricType, invalid parameter combination)
        '403':
          description: Insufficient privileges
        '404':
          description: Table not found
```

### 6.2 Metrics API Schemas

Add these schemas to `spec/metrics-reports-service.yml`:

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

    ListMetricsResponse:
      description: >
        Polymorphic response for metrics queries. The concrete type is determined
        by the metricType discriminator field.
      oneOf:
        - $ref: '#/components/schemas/ListScanMetricsResponse'
        - $ref: '#/components/schemas/ListCommitMetricsResponse'
      discriminator:
        propertyName: metricType
        mapping:
          scan: '#/components/schemas/ListScanMetricsResponse'
          commit: '#/components/schemas/ListCommitMetricsResponse'

    ListScanMetricsResponse:
      type: object
      required:
        - metricType
        - reports
      properties:
        nextPageToken:
          type: string
          description: Cursor for fetching the next page of results
        metricType:
          type: string
          const: scan
          description: Discriminator indicating this response contains scan metrics
        reports:
          type: array
          description: Array of scan metrics reports
          items:
            $ref: '#/components/schemas/ScanMetricsReport'

    ListCommitMetricsResponse:
      type: object
      required:
        - metricType
        - reports
      properties:
        nextPageToken:
          type: string
          description: Cursor for fetching the next page of results
        metricType:
          type: string
          const: commit
          description: Discriminator indicating this response contains commit metrics
        reports:
          type: array
          description: Array of commit metrics reports
          items:
            $ref: '#/components/schemas/CommitMetricsReport'

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
```

---

## 7. Implementation Notes

### 7.1 Database Queries

The endpoints will query existing tables with appropriate filtering and pagination:

```sql
-- List scan metrics
SELECT * FROM scan_metrics_report
WHERE realm_id = ?
  AND catalog_id = ?
  AND table_id = ?
  AND timestamp_ms >= ?
  AND timestamp_ms < ?
ORDER BY timestamp_ms DESC, report_id DESC
LIMIT ?;

-- List commit metrics
SELECT * FROM commit_metrics_report
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
-- Metrics indexes (may already exist)
CREATE INDEX IF NOT EXISTS idx_scan_report_lookup
    ON scan_metrics_report(realm_id, catalog_id, table_id, timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_commit_report_lookup
    ON commit_metrics_report(realm_id, catalog_id, table_id, timestamp_ms DESC);
```

### 7.3 Files to Modify

| File | Changes |
|------|---------|
| `spec/metrics-reports-service.yml` | **New file** - Metrics Reports API paths and schemas |
| `api/metrics-reports-service/` | **New** - Generated Metrics Reports API interfaces |
| `runtime/service/.../metrics/` | **New** - Metrics reports service implementation |
| `polaris-core/.../persistence/BasePersistence.java` | Add read methods |
| `persistence/relational-jdbc/.../JdbcBasePersistenceImpl.java` | Query implementations |

### 7.4 Pagination Token Format

Internal format (base64-encoded JSON, opaque to clients):

```json
{
  "ts": 1709337612345,
  "id": "report-abc123"
}
```

---

## Open Questions

1. **Aggregations**: Are aggregated metrics views needed (e.g., daily summaries)?
2. **Metric Retention**: What is the default retention period for metrics? Should it be configurable?

---

## References

- Database schema: `persistence/relational-jdbc/src/main/resources/postgres/schema-v4.sql`
- Metrics persistence: `runtime/service/src/main/java/org/apache/polaris/service/reporting/PersistingMetricsReporter.java`
- Metrics record converter: `polaris-core/src/main/java/org/apache/polaris/core/metrics/iceberg/MetricsRecordConverter.java`

