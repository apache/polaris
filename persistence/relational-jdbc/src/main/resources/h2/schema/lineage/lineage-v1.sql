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

CREATE SCHEMA IF NOT EXISTS POLARIS_SCHEMA;
SET SCHEMA POLARIS_SCHEMA;

CREATE TABLE IF NOT EXISTS version (
    version_key VARCHAR PRIMARY KEY,
    version_value INTEGER NOT NULL
);

MERGE INTO version (version_key, version_value)
    KEY (version_key)
    VALUES ('lineage', 1);

CREATE TABLE IF NOT EXISTS lineage_datasets (
    realm_id TEXT NOT NULL,
    dataset_id BIGINT NOT NULL,
    catalog TEXT NOT NULL,
    namespace TEXT NOT NULL,
    name TEXT NOT NULL,
    polaris_entity_id BIGINT,
    last_lineage_event_at BIGINT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (realm_id, dataset_id),
    UNIQUE (realm_id, namespace, name)
);

CREATE INDEX IF NOT EXISTS idx_lineage_datasets_identity
    ON lineage_datasets(realm_id, namespace, name);

CREATE TABLE IF NOT EXISTS lineage_edges (
    realm_id TEXT NOT NULL,
    source_dataset_id BIGINT NOT NULL,
    target_dataset_id BIGINT NOT NULL,
    last_event_at BIGINT NOT NULL,
    PRIMARY KEY (realm_id, source_dataset_id, target_dataset_id)
);

CREATE INDEX IF NOT EXISTS idx_lineage_edges_target
    ON lineage_edges(realm_id, target_dataset_id);

CREATE TABLE IF NOT EXISTS lineage_column_edges (
    realm_id TEXT NOT NULL,
    source_dataset_id BIGINT NOT NULL,
    source_field TEXT NOT NULL,
    target_dataset_id BIGINT NOT NULL,
    target_field TEXT NOT NULL,
    last_event_at BIGINT NOT NULL,
    PRIMARY KEY (realm_id, source_dataset_id, source_field, target_dataset_id, target_field)
);

CREATE INDEX IF NOT EXISTS idx_lineage_column_edges_target
    ON lineage_column_edges(realm_id, target_dataset_id, target_field);
