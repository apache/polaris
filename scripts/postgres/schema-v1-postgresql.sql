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

-- Note: Database and schema creation is not included in this script. Please create the database and
-- schema before running this script. for example in psql:
-- CREATE DATABASE polaris_db;
-- \c polaris_db
-- CREATE SCHEMA polaris_schema;
-- set search_path to polaris_schema;

CREATE TABLE IF NOT EXISTS entities (
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
    PRIMARY KEY (catalog_id, id),
    CONSTRAINT constraint_name UNIQUE (catalog_id, parent_id, type_code, name)
);

COMMENT ON TABLE entities IS 'all the entities';

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
COMMENT ON COLUMN entities.grant_records_version IS 'grant record version of the entity';

CREATE TABLE IF NOT EXISTS grant_records (
    securable_catalog_id BIGINT NOT NULL,
    securable_id BIGINT NOT NULL,
    grantee_catalog_id BIGINT NOT NULL,
    grantee_id BIGINT NOT NULL,
    privilege_code INTEGER,
    PRIMARY KEY (securable_catalog_id, securable_id, grantee_catalog_id, grantee_id, privilege_code)
);

COMMENT ON TABLE grant_records IS 'grant records for entities';

COMMENT ON COLUMN grant_records.securable_catalog_id IS 'catalog id of the securable';
COMMENT ON COLUMN grant_records.securable_id IS 'entity id of the securable';
COMMENT ON COLUMN grant_records.grantee_catalog_id IS 'catalog id of the grantee';
COMMENT ON COLUMN grant_records.grantee_id IS 'id of the grantee';
COMMENT ON COLUMN grant_records.privilege_code IS 'privilege id';


CREATE TABLE IF NOT EXISTS principal_authentication_data (
    principal_id BIGINT NOT NULL,
    principal_client_id VARCHAR(255) NOT NULL,
    main_secret_hash VARCHAR(255) NOT NULL,
    secondary_secret_hash VARCHAR(255) NOT NULL,
    secret_salt VARCHAR(255) NOT NULL,
    PRIMARY KEY (principal_client_id)
);

COMMENT ON TABLE principal_authentication_data IS 'authentication data for client';