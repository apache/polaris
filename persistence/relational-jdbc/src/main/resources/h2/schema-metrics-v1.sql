--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file--
--  distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"). You may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--  http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--

-- ============================================================================
-- POLARIS METRICS SCHEMA VERSION 1 (H2)
-- ============================================================================
-- This schema is SEPARATE from the entity schema and can evolve independently.
-- It contains tables for storing Iceberg metrics reports.
--
-- Tables:
--   * `metrics_version` - Version tracking for the metrics schema
--   * `scan_metrics_report` - Scan metrics reports
--   * `commit_metrics_report` - Commit metrics reports
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS POLARIS_SCHEMA;
SET SCHEMA POLARIS_SCHEMA;

-- Metrics schema version tracking (separate from entity schema version)
CREATE TABLE IF NOT EXISTS metrics_version (
    version_key VARCHAR PRIMARY KEY,
    version_value INTEGER NOT NULL
);

MERGE INTO metrics_version (version_key, version_value)
    KEY (version_key)
    VALUES ('metrics_version', 1);

COMMENT ON TABLE metrics_version IS 'the version of the metrics schema in use';

-- ============================================================================
-- SCAN METRICS REPORT TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS scan_metrics_report (
    report_id TEXT NOT NULL,
    realm_id TEXT NOT NULL,
    catalog_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,

    -- Report metadata
    timestamp_ms BIGINT NOT NULL,
    principal_name TEXT,
    request_id TEXT,

    -- Trace correlation
    otel_trace_id TEXT,
    otel_span_id TEXT,
    report_trace_id TEXT,

    -- Scan context
    snapshot_id BIGINT,
    schema_id INTEGER,
    filter_expression TEXT,
    projected_field_ids TEXT,
    projected_field_names TEXT,

    -- Scan metrics
    result_data_files BIGINT DEFAULT 0,
    result_delete_files BIGINT DEFAULT 0,
    total_file_size_bytes BIGINT DEFAULT 0,
    total_data_manifests BIGINT DEFAULT 0,
    total_delete_manifests BIGINT DEFAULT 0,
    scanned_data_manifests BIGINT DEFAULT 0,
    scanned_delete_manifests BIGINT DEFAULT 0,
    skipped_data_manifests BIGINT DEFAULT 0,
    skipped_delete_manifests BIGINT DEFAULT 0,
    skipped_data_files BIGINT DEFAULT 0,
    skipped_delete_files BIGINT DEFAULT 0,
    total_planning_duration_ms BIGINT DEFAULT 0,

    -- Equality/positional delete metrics
    equality_delete_files BIGINT DEFAULT 0,
    positional_delete_files BIGINT DEFAULT 0,
    indexed_delete_files BIGINT DEFAULT 0,
    total_delete_file_size_bytes BIGINT DEFAULT 0,

    -- Additional metadata (for extensibility)
    metadata TEXT DEFAULT '{}',

    PRIMARY KEY (realm_id, report_id)
);

COMMENT ON TABLE scan_metrics_report IS 'Scan metrics reports as first-class entities';

-- Index for retention cleanup by timestamp
CREATE INDEX IF NOT EXISTS idx_scan_report_timestamp ON scan_metrics_report(realm_id, timestamp_ms);

-- ============================================================================
-- COMMIT METRICS REPORT TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS commit_metrics_report (
    report_id TEXT NOT NULL,
    realm_id TEXT NOT NULL,
    catalog_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,

    -- Report metadata
    timestamp_ms BIGINT NOT NULL,
    principal_name TEXT,
    request_id TEXT,

    -- Trace correlation
    otel_trace_id TEXT,
    otel_span_id TEXT,
    report_trace_id TEXT,

    -- Commit context
    snapshot_id BIGINT NOT NULL,
    sequence_number BIGINT,
    operation TEXT NOT NULL,

    -- File metrics
    added_data_files BIGINT DEFAULT 0,
    removed_data_files BIGINT DEFAULT 0,
    total_data_files BIGINT DEFAULT 0,
    added_delete_files BIGINT DEFAULT 0,
    removed_delete_files BIGINT DEFAULT 0,
    total_delete_files BIGINT DEFAULT 0,

    -- Equality delete files
    added_equality_delete_files BIGINT DEFAULT 0,
    removed_equality_delete_files BIGINT DEFAULT 0,

    -- Positional delete files
    added_positional_delete_files BIGINT DEFAULT 0,
    removed_positional_delete_files BIGINT DEFAULT 0,

    -- Record metrics
    added_records BIGINT DEFAULT 0,
    removed_records BIGINT DEFAULT 0,
    total_records BIGINT DEFAULT 0,

    -- Size metrics
    added_file_size_bytes BIGINT DEFAULT 0,
    removed_file_size_bytes BIGINT DEFAULT 0,
    total_file_size_bytes BIGINT DEFAULT 0,

    -- Duration and attempts
    total_duration_ms BIGINT DEFAULT 0,
    attempts INTEGER DEFAULT 1,

    -- Additional metadata (for extensibility)
    metadata TEXT DEFAULT '{}',

    PRIMARY KEY (realm_id, report_id)
);

COMMENT ON TABLE commit_metrics_report IS 'Commit metrics reports as first-class entities';

-- Index for retention cleanup by timestamp
CREATE INDEX IF NOT EXISTS idx_commit_report_timestamp ON commit_metrics_report(realm_id, timestamp_ms);
