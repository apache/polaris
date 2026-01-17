--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
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

-- Changes from v3:
--  * Added `scan_metrics_report` table for scan metrics as first-class entities
--  * Added `commit_metrics_report` table for commit metrics as first-class entities

CREATE SCHEMA IF NOT EXISTS POLARIS_SCHEMA;
SET search_path TO POLARIS_SCHEMA;

CREATE TABLE IF NOT EXISTS version (
    version_key TEXT PRIMARY KEY,
    version_value INTEGER NOT NULL
);
INSERT INTO version (version_key, version_value)
VALUES ('version', 4)
ON CONFLICT (version_key) DO UPDATE
SET version_value = EXCLUDED.version_value;
COMMENT ON TABLE version IS 'the version of the JDBC schema in use';

-- Include all tables from v3
-- (entities, grant_records, principal_authentication_data, policy_mapping_record, events)
-- These are assumed to already exist from v3 migration

-- Scan Metrics Report Entity Table
CREATE TABLE IF NOT EXISTS scan_metrics_report (
    report_id TEXT NOT NULL,
    realm_id TEXT NOT NULL,
    catalog_id TEXT NOT NULL,
    catalog_name TEXT NOT NULL,
    namespace TEXT NOT NULL,
    table_name TEXT NOT NULL,
    
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
    metadata JSONB DEFAULT '{}'::JSONB,
    
    PRIMARY KEY (report_id)
);

COMMENT ON TABLE scan_metrics_report IS 'Scan metrics reports as first-class entities';
COMMENT ON COLUMN scan_metrics_report.report_id IS 'Unique identifier for the report';
COMMENT ON COLUMN scan_metrics_report.realm_id IS 'Realm ID for multi-tenancy';
COMMENT ON COLUMN scan_metrics_report.catalog_id IS 'Catalog ID';
COMMENT ON COLUMN scan_metrics_report.otel_trace_id IS 'OpenTelemetry trace ID from HTTP headers';
COMMENT ON COLUMN scan_metrics_report.report_trace_id IS 'Trace ID from report metadata';

-- Indexes for scan_metrics_report
CREATE INDEX IF NOT EXISTS idx_scan_report_timestamp 
    ON scan_metrics_report(timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_scan_report_table 
    ON scan_metrics_report(catalog_name, namespace, table_name);
CREATE INDEX IF NOT EXISTS idx_scan_report_trace 
    ON scan_metrics_report(otel_trace_id) WHERE otel_trace_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_scan_report_principal 
    ON scan_metrics_report(principal_name) WHERE principal_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_scan_report_realm
    ON scan_metrics_report(realm_id);


-- Commit Metrics Report Entity Table
CREATE TABLE IF NOT EXISTS commit_metrics_report (
    report_id TEXT NOT NULL,
    realm_id TEXT NOT NULL,
    catalog_id TEXT NOT NULL,
    catalog_name TEXT NOT NULL,
    namespace TEXT NOT NULL,
    table_name TEXT NOT NULL,

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
    metadata JSONB DEFAULT '{}'::JSONB,

    PRIMARY KEY (report_id)
);

COMMENT ON TABLE commit_metrics_report IS 'Commit metrics reports as first-class entities';
COMMENT ON COLUMN commit_metrics_report.report_id IS 'Unique identifier for the report';
COMMENT ON COLUMN commit_metrics_report.realm_id IS 'Realm ID for multi-tenancy';
COMMENT ON COLUMN commit_metrics_report.operation IS 'Commit operation type: append, overwrite, delete, replace';
COMMENT ON COLUMN commit_metrics_report.otel_trace_id IS 'OpenTelemetry trace ID from HTTP headers';

-- Indexes for commit_metrics_report
CREATE INDEX IF NOT EXISTS idx_commit_report_timestamp
    ON commit_metrics_report(timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_commit_report_table
    ON commit_metrics_report(catalog_name, namespace, table_name);
CREATE INDEX IF NOT EXISTS idx_commit_report_trace
    ON commit_metrics_report(otel_trace_id) WHERE otel_trace_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_commit_report_principal
    ON commit_metrics_report(principal_name) WHERE principal_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_commit_report_operation
    ON commit_metrics_report(operation);
CREATE INDEX IF NOT EXISTS idx_commit_report_realm
    ON commit_metrics_report(realm_id);
CREATE INDEX IF NOT EXISTS idx_commit_report_snapshot
    ON commit_metrics_report(snapshot_id);

