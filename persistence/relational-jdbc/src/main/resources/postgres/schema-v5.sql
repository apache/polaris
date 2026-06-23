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

-- Changes from v4:
--  * Reshaped `idempotency_records` table for the optimistic-commit (record-after-success)
--    idempotency model: dropped the in-progress/lease and stored-response columns, added
--    `binding_hash`, `metadata_location`, and made `http_status`/`expires_at`
--    NOT NULL. v4's `idempotency_records` was never wired to request handling, so there is no
--    data migration; existing v4 installs must re-bootstrap at v5 (or apply equivalent DDL) to
--    enable idempotency.

CREATE SCHEMA IF NOT EXISTS POLARIS_SCHEMA;
SET search_path TO POLARIS_SCHEMA;

CREATE TABLE IF NOT EXISTS version (
    version_key TEXT PRIMARY KEY,
    version_value INTEGER NOT NULL
);
INSERT INTO version (version_key, version_value)
VALUES ('version', 5)
ON CONFLICT (version_key) DO UPDATE
SET version_value = EXCLUDED.version_value;
COMMENT ON TABLE version IS 'the version of the JDBC schema in use';

CREATE TABLE IF NOT EXISTS entities (
    realm_id TEXT NOT NULL,
    catalog_id BIGINT NOT NULL,
    id BIGINT NOT NULL,
    parent_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    entity_version INT NOT NULL,
    type_code INT NOT NULL,
    sub_type_code INT NOT NULL,
    create_timestamp BIGINT NOT NULL,
    drop_timestamp BIGINT NOT NULL,
    purge_timestamp BIGINT NOT NULL,
    to_purge_timestamp BIGINT NOT NULL,
    last_update_timestamp BIGINT NOT NULL,
    properties JSONB not null default '{}'::JSONB,
    internal_properties JSONB not null default '{}'::JSONB,
    grant_records_version INT NOT NULL,
    location_without_scheme TEXT,
    PRIMARY KEY (realm_id, id),
    CONSTRAINT constraint_name UNIQUE (realm_id, catalog_id, parent_id, type_code, name)
);

-- TODO: create indexes based on all query pattern.
CREATE INDEX IF NOT EXISTS idx_entities ON entities (realm_id, catalog_id, id);
CREATE INDEX IF NOT EXISTS idx_entities_catalog_id_id ON entities (catalog_id, id);
CREATE INDEX IF NOT EXISTS idx_locations
    ON entities USING btree (realm_id, parent_id, location_without_scheme)
    WHERE location_without_scheme IS NOT NULL;

COMMENT ON TABLE entities IS 'all the entities';

COMMENT ON COLUMN entities.realm_id IS 'realm_id used for multi-tenancy';
COMMENT ON COLUMN entities.catalog_id IS 'catalog id';
COMMENT ON COLUMN entities.id IS 'entity id';
COMMENT ON COLUMN entities.parent_id IS 'entity id of parent';
COMMENT ON COLUMN entities.name IS 'entity name';
COMMENT ON COLUMN entities.entity_version IS 'version of the entity';
COMMENT ON COLUMN entities.type_code IS 'type code';
COMMENT ON COLUMN entities.sub_type_code IS 'sub type of entity';
COMMENT ON COLUMN entities.create_timestamp IS 'creation time of entity';
COMMENT ON COLUMN entities.drop_timestamp IS 'time of drop of entity';
COMMENT ON COLUMN entities.purge_timestamp IS 'time to start purging entity';
COMMENT ON COLUMN entities.last_update_timestamp IS 'last time the entity is touched';
COMMENT ON COLUMN entities.properties IS 'entities properties json';
COMMENT ON COLUMN entities.internal_properties IS 'entities internal properties json';
COMMENT ON COLUMN entities.grant_records_version IS 'the version of grant records change on the entity';

CREATE TABLE IF NOT EXISTS grant_records (
    realm_id TEXT NOT NULL,
    securable_catalog_id BIGINT NOT NULL,
    securable_id BIGINT NOT NULL,
    grantee_catalog_id BIGINT NOT NULL,
    grantee_id BIGINT NOT NULL,
    privilege_code INTEGER,
    PRIMARY KEY (realm_id, securable_catalog_id, securable_id, grantee_catalog_id, grantee_id, privilege_code)
);

COMMENT ON TABLE grant_records IS 'grant records for entities';

COMMENT ON COLUMN grant_records.securable_catalog_id IS 'catalog id of the securable';
COMMENT ON COLUMN grant_records.securable_id IS 'entity id of the securable';
COMMENT ON COLUMN grant_records.grantee_catalog_id IS 'catalog id of the grantee';
COMMENT ON COLUMN grant_records.grantee_id IS 'id of the grantee';
COMMENT ON COLUMN grant_records.privilege_code IS 'privilege code';

CREATE INDEX IF NOT EXISTS idx_grants_realm_grantee 
    ON grant_records (realm_id, grantee_id);
CREATE INDEX IF NOT EXISTS idx_grants_realm_securable 
    ON grant_records (realm_id, securable_id);

CREATE TABLE IF NOT EXISTS principal_authentication_data (
    realm_id TEXT NOT NULL,
    principal_id BIGINT NOT NULL,
    principal_client_id VARCHAR(255) NOT NULL,
    main_secret_hash VARCHAR(255) NOT NULL,
    secondary_secret_hash VARCHAR(255) NOT NULL,
    secret_salt VARCHAR(255) NOT NULL,
    PRIMARY KEY (realm_id, principal_client_id)
);

COMMENT ON TABLE principal_authentication_data IS 'authentication data for client';

CREATE TABLE IF NOT EXISTS policy_mapping_record (
    realm_id TEXT NOT NULL,
    target_catalog_id BIGINT NOT NULL,
    target_id BIGINT NOT NULL,
    policy_type_code INTEGER NOT NULL,
    policy_catalog_id BIGINT NOT NULL,
    policy_id BIGINT NOT NULL,
    parameters JSONB NOT NULL DEFAULT '{}'::JSONB,
    PRIMARY KEY (realm_id, target_catalog_id, target_id, policy_type_code, policy_catalog_id, policy_id)
);

CREATE INDEX IF NOT EXISTS idx_policy_mapping_record ON policy_mapping_record (realm_id, policy_type_code, policy_catalog_id, policy_id, target_catalog_id, target_id);

CREATE TABLE IF NOT EXISTS events (
    realm_id TEXT NOT NULL,
    catalog_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    request_id TEXT,
    event_type TEXT NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    principal_name TEXT,
    resource_type TEXT NOT NULL,
    resource_identifier TEXT NOT NULL,
    additional_properties JSONB NOT NULL DEFAULT '{}'::JSONB,
    PRIMARY KEY (event_id)
);

-- Idempotency records (optimistic-commit model; record-after-success, no stored response body).
-- A row is inserted at most once per (realm_id, idempotency_key) after the originating operation
-- succeeds, and is never updated afterwards.
CREATE TABLE IF NOT EXISTS idempotency_records (
    realm_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    operation_type TEXT NOT NULL,         -- logical operation, e.g. create-table (human-readable label only)
    binding_hash TEXT NOT NULL,           -- opaque hash over the full binding (caller principal + realm + roles, operation, and request-derived resource identity: namespace, name, delegation modes); does not include the request payload; checked on replay to detect reuse of the key for a different caller/operation/resource
    http_status INTEGER NOT NULL,         -- terminal HTTP status recorded for the originating (successful) request
    metadata_location TEXT,               -- table metadata pointer captured at record time; replay returns 422 if the table has advanced beyond it
    created_at TIMESTAMP NOT NULL,        -- when the record was inserted
    expires_at TIMESTAMP NOT NULL,        -- after which the record is eligible for purging

    PRIMARY KEY (realm_id, idempotency_key)
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_idemp_realm_expires
    ON idempotency_records (realm_id, expires_at);

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
    metadata JSONB DEFAULT '{}'::JSONB,

    PRIMARY KEY (realm_id, report_id)
);

COMMENT ON TABLE scan_metrics_report IS 'Scan metrics reports as first-class entities';

-- Index for retention cleanup by timestamp
CREATE INDEX IF NOT EXISTS idx_scan_report_timestamp ON scan_metrics_report(realm_id, timestamp_ms);

-- Index for query lookups by catalog_id and table_id
CREATE INDEX IF NOT EXISTS idx_scan_report_lookup ON scan_metrics_report(realm_id, catalog_id, table_id, timestamp_ms);

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
    metadata JSONB DEFAULT '{}'::JSONB,

    PRIMARY KEY (realm_id, report_id)
);

COMMENT ON TABLE commit_metrics_report IS 'Commit metrics reports as first-class entities';

-- Index for retention cleanup by timestamp
CREATE INDEX IF NOT EXISTS idx_commit_report_timestamp ON commit_metrics_report(realm_id, timestamp_ms);

-- Index for query lookups by catalog_id and table_id
CREATE INDEX IF NOT EXISTS idx_commit_report_lookup ON commit_metrics_report(realm_id, catalog_id, table_id, timestamp_ms);