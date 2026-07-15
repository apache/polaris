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

-- MySQL schema v4 (matching PostgreSQL schema v4)
-- Design notes:
--  * The POLARIS_SCHEMA database must be pre-created and the user must have privileges on it.
--  * In MySQL, schema is synonymous with database.
--  * Indexes are declared inside CREATE TABLE because MySQL does not support
--    "CREATE INDEX IF NOT EXISTS" — the bootstrap runs this script once per realm
--    and "CREATE TABLE IF NOT EXISTS" handles the idempotency.
--  * Uses VARCHAR(255) for PRIMARY KEY / UNIQUE KEY columns where PostgreSQL uses TEXT
--    (MySQL forbids TEXT columns in keys without a prefix length).
--  * Uses JSON type instead of JSONB.
--  * Uses ON DUPLICATE KEY UPDATE instead of ON CONFLICT ... DO UPDATE.
--  * Table identifiers use the same case as the Java `Model*.TABLE_NAME` constants
--    (UPPERCASE for everything except `idempotency_records`, which is lowercase to
--    match `ModelIdempotencyRecord.TABLE_NAME`). This keeps the schema working on
--    default Linux MySQL (`lower_case_table_names=0`, case-sensitive identifiers)
--    without any server-level flag.
--  * Every table sets `DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin` so string
--    comparisons are case-sensitive/binary, matching the PostgreSQL `TEXT` semantics
--    expected by Polaris (e.g. `entities.name` must treat `foo` and `Foo` as distinct).

USE POLARIS_SCHEMA;

CREATE TABLE IF NOT EXISTS VERSION (
    version_key VARCHAR(255) PRIMARY KEY COMMENT 'version key',
    version_value INTEGER NOT NULL COMMENT 'version value'
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin COMMENT = 'the version of the JDBC schema in use';

INSERT INTO VERSION (version_key, version_value)
VALUES ('version', 4)
ON DUPLICATE KEY UPDATE version_value = VALUES(version_value);

CREATE TABLE IF NOT EXISTS ENTITIES (
    realm_id VARCHAR(255) NOT NULL COMMENT 'realm_id used for multi-tenancy',
    catalog_id BIGINT NOT NULL COMMENT 'catalog id',
    id BIGINT NOT NULL COMMENT 'entity id',
    parent_id BIGINT NOT NULL COMMENT 'entity id of parent',
    -- Matches Polaris's MAX_IDENTIFIER_LENGTH = 256. Keeps the UNIQUE
    -- (realm_id, ..., name) index well within the InnoDB 3072-byte key-length
    -- limit: 255*4 + 8+8+4 + 256*4 = 2064 bytes (utf8mb4).
    name VARCHAR(256) NOT NULL COMMENT 'entity name',
    entity_version INT NOT NULL COMMENT 'version of the entity',
    type_code INT NOT NULL COMMENT 'type code',
    sub_type_code INT NOT NULL COMMENT 'sub type of entity',
    create_timestamp BIGINT NOT NULL COMMENT 'creation time of entity',
    drop_timestamp BIGINT NOT NULL COMMENT 'time of drop of entity',
    purge_timestamp BIGINT NOT NULL COMMENT 'time to start purging entity',
    to_purge_timestamp BIGINT NOT NULL,
    last_update_timestamp BIGINT NOT NULL COMMENT 'last time the entity is touched',
    properties JSON NOT NULL DEFAULT ('{}') COMMENT 'entities properties json',
    internal_properties JSON NOT NULL DEFAULT ('{}') COMMENT 'entities internal properties json',
    grant_records_version INT NOT NULL COMMENT 'the version of grant records change on the entity',
    location_without_scheme TEXT,
    PRIMARY KEY (realm_id, id),
    CONSTRAINT constraint_name UNIQUE (realm_id, catalog_id, parent_id, type_code, name),
    INDEX idx_entities (realm_id, catalog_id, id),
    INDEX idx_entities_catalog_id_id (catalog_id, id),
    -- location_without_scheme is TEXT, use prefix length 400 to stay under
    -- the InnoDB 3072-byte key length limit (utf8mb4 = 4 bytes/char).
    INDEX idx_locations (realm_id, parent_id, location_without_scheme(400))
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin COMMENT = 'all the entities';

CREATE TABLE IF NOT EXISTS GRANT_RECORDS (
    realm_id VARCHAR(255) NOT NULL,
    securable_catalog_id BIGINT NOT NULL COMMENT 'catalog id of the securable',
    securable_id BIGINT NOT NULL COMMENT 'entity id of the securable',
    grantee_catalog_id BIGINT NOT NULL COMMENT 'catalog id of the grantee',
    grantee_id BIGINT NOT NULL COMMENT 'id of the grantee',
    privilege_code INTEGER COMMENT 'privilege code',
    PRIMARY KEY (realm_id, securable_catalog_id, securable_id, grantee_catalog_id, grantee_id, privilege_code),
    INDEX idx_grants_realm_grantee (realm_id, grantee_id),
    INDEX idx_grants_realm_securable (realm_id, securable_id)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin COMMENT = 'grant records for entities';

CREATE TABLE IF NOT EXISTS PRINCIPAL_AUTHENTICATION_DATA (
    realm_id VARCHAR(255) NOT NULL,
    principal_id BIGINT NOT NULL,
    principal_client_id VARCHAR(255) NOT NULL,
    main_secret_hash VARCHAR(255) NOT NULL,
    secondary_secret_hash VARCHAR(255) NOT NULL,
    secret_salt VARCHAR(255) NOT NULL,
    PRIMARY KEY (realm_id, principal_client_id)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin COMMENT = 'authentication data for client';

CREATE TABLE IF NOT EXISTS POLICY_MAPPING_RECORD (
    realm_id VARCHAR(255) NOT NULL,
    target_catalog_id BIGINT NOT NULL,
    target_id BIGINT NOT NULL,
    policy_type_code INTEGER NOT NULL,
    policy_catalog_id BIGINT NOT NULL,
    policy_id BIGINT NOT NULL,
    parameters JSON NOT NULL DEFAULT ('{}'),
    PRIMARY KEY (realm_id, target_catalog_id, target_id, policy_type_code, policy_catalog_id, policy_id),
    INDEX idx_policy_mapping_record (realm_id, policy_type_code, policy_catalog_id, policy_id, target_catalog_id, target_id)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS EVENTS (
    realm_id VARCHAR(255) NOT NULL,
    catalog_id VARCHAR(255) NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    request_id VARCHAR(255),
    event_type VARCHAR(255) NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    principal_name VARCHAR(255),
    resource_type VARCHAR(255) NOT NULL,
    resource_identifier VARCHAR(1024) NOT NULL,
    additional_properties JSON NOT NULL DEFAULT ('{}'),
    PRIMARY KEY (event_id)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- Idempotency records (key-only idempotency; durable replay).
-- This is the one table whose Java constant (`ModelIdempotencyRecord.TABLE_NAME`) is
-- lowercase; matching the Java case is what lets the MySQL backend run on default
-- Linux MySQL without `lower_case_table_names=1`.
CREATE TABLE IF NOT EXISTS idempotency_records (
    realm_id VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    operation_type VARCHAR(255) NOT NULL,
    resource_id VARCHAR(1024) NOT NULL,

    http_status INTEGER,
    error_subtype VARCHAR(255),
    response_summary TEXT,
    response_headers TEXT,
    finalized_at TIMESTAMP NULL,

    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    heartbeat_at TIMESTAMP NULL,
    executor_id VARCHAR(255),
    expires_at TIMESTAMP NULL,

    PRIMARY KEY (realm_id, idempotency_key),
    INDEX idx_idemp_realm_expires (realm_id, expires_at)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- ============================================================================
-- SCAN METRICS REPORT TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS SCAN_METRICS_REPORT (
    report_id VARCHAR(255) NOT NULL,
    realm_id VARCHAR(255) NOT NULL,
    catalog_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,

    timestamp_ms BIGINT NOT NULL,
    principal_name VARCHAR(255),
    request_id VARCHAR(255),

    otel_trace_id VARCHAR(255),
    otel_span_id VARCHAR(255),
    report_trace_id VARCHAR(255),

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

    metadata JSON DEFAULT ('{}'),

    PRIMARY KEY (realm_id, report_id),
    INDEX idx_scan_report_timestamp (realm_id, timestamp_ms),
    INDEX idx_scan_report_lookup (realm_id, catalog_id, table_id, timestamp_ms)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin COMMENT = 'Scan metrics reports as first-class entities';

-- ============================================================================
-- COMMIT METRICS REPORT TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS COMMIT_METRICS_REPORT (
    report_id VARCHAR(255) NOT NULL,
    realm_id VARCHAR(255) NOT NULL,
    catalog_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,

    timestamp_ms BIGINT NOT NULL,
    principal_name VARCHAR(255),
    request_id VARCHAR(255),

    otel_trace_id VARCHAR(255),
    otel_span_id VARCHAR(255),
    report_trace_id VARCHAR(255),

    snapshot_id BIGINT NOT NULL,
    sequence_number BIGINT,
    operation VARCHAR(255) NOT NULL,

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

    metadata JSON DEFAULT ('{}'),

    PRIMARY KEY (realm_id, report_id),
    INDEX idx_commit_report_timestamp (realm_id, timestamp_ms),
    INDEX idx_commit_report_lookup (realm_id, catalog_id, table_id, timestamp_ms)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin COMMENT = 'Commit metrics reports as first-class entities';
