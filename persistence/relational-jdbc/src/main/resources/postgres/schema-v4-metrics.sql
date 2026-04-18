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

-- Metrics schema for v4
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

CREATE TABLE IF NOT EXISTS scan_metrics_report (
    report_id TEXT NOT NULL,
    realm_id TEXT NOT NULL,
    catalog_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    principal_name TEXT,
    request_id TEXT,
    otel_trace_id TEXT,
    otel_span_id TEXT,
    report_trace_id TEXT,
    snapshot_id BIGINT,
    schema_id INTEGER,
    filter_expression TEXT,
    projected_field_ids TEXT,
    projected_field_names TEXT,
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
    equality_delete_files BIGINT DEFAULT 0,
    positional_delete_files BIGINT DEFAULT 0,
    indexed_delete_files BIGINT DEFAULT 0,
    total_delete_file_size_bytes BIGINT DEFAULT 0,
    metadata JSONB DEFAULT '{}'::JSONB,
    PRIMARY KEY (realm_id, report_id)
);

CREATE INDEX IF NOT EXISTS idx_scan_report_timestamp ON scan_metrics_report(realm_id, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_scan_report_lookup ON scan_metrics_report(realm_id, catalog_id, table_id, timestamp_ms);

CREATE TABLE IF NOT EXISTS commit_metrics_report (
    report_id TEXT NOT NULL,
    realm_id TEXT NOT NULL,
    catalog_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    principal_name TEXT,
    request_id TEXT,
    otel_trace_id TEXT,
    otel_span_id TEXT,
    report_trace_id TEXT,
    snapshot_id BIGINT NOT NULL,
    sequence_number BIGINT,
    operation TEXT NOT NULL,
    added_data_files BIGINT DEFAULT 0,
    removed_data_files BIGINT DEFAULT 0,
    total_data_files BIGINT DEFAULT 0,
    added_delete_files BIGINT DEFAULT 0,
    removed_delete_files BIGINT DEFAULT 0,
    total_delete_files BIGINT DEFAULT 0,
    added_equality_delete_files BIGINT DEFAULT 0,
    removed_equality_delete_files BIGINT DEFAULT 0,
    added_positional_delete_files BIGINT DEFAULT 0,
    removed_positional_delete_files BIGINT DEFAULT 0,
    added_records BIGINT DEFAULT 0,
    removed_records BIGINT DEFAULT 0,
    total_records BIGINT DEFAULT 0,
    added_file_size_bytes BIGINT DEFAULT 0,
    removed_file_size_bytes BIGINT DEFAULT 0,
    total_file_size_bytes BIGINT DEFAULT 0,
    total_duration_ms BIGINT DEFAULT 0,
    attempts INTEGER DEFAULT 1,
    metadata JSONB DEFAULT '{}'::JSONB,
    PRIMARY KEY (realm_id, report_id)
);

CREATE INDEX IF NOT EXISTS idx_commit_report_timestamp ON commit_metrics_report(realm_id, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_commit_report_lookup ON commit_metrics_report(realm_id, catalog_id, table_id, timestamp_ms);
