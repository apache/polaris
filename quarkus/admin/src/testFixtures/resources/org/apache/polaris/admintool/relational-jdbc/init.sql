/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

-- Create two more databases for testing. The first database, polaris_realm1, is created
-- during container initialization. See PostgresTestResourceLifecycleManager.

-- Note: the database names must follow the pattern polaris_{realm}. That's the pattern
-- specified by the persistence.xml file used in tests.

CREATE DATABASE polaris_realm2;
GRANT ALL PRIVILEGES ON DATABASE polaris_realm2 TO polaris;

CREATE DATABASE polaris_realm3;
GRANT ALL PRIVILEGES ON DATABASE polaris_realm3 TO polaris;

CREATE SCHEMA IF NOT EXISTS POLARIS_SCHEMA;
SET SEARCH_PATH TO POLARIS_SCHEMA;
DROP TABLE IF EXISTS POLARIS_SCHEMA.ENTITIES;
CREATE TABLE IF NOT EXISTS POLARIS_SCHEMA.entities (
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
    properties TEXT NOT NULL DEFAULT '{}',
    internal_properties TEXT NOT NULL DEFAULT '{}',
    grant_records_version INT NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT constraint_name UNIQUE (catalog_id, parent_id, type_code, name)
);

-- TODO: create indexes based on all query pattern.
CREATE INDEX IF NOT EXISTS idx_entities ON POLARIS_SCHEMA.entities (catalog_id, id);

COMMENT ON TABLE POLARIS_SCHEMA.entities IS 'all the entities';

COMMENT ON COLUMN POLARIS_SCHEMA.entities.catalog_id IS 'catalog id';
COMMENT ON COLUMN POLARIS_SCHEMA.entities.id IS 'entity id';
COMMENT ON COLUMN POLARIS_SCHEMA.entities.parent_id IS 'entity id of parent';
COMMENT ON COLUMN POLARIS_SCHEMA.entities.name IS 'entity name';
COMMENT ON COLUMN POLARIS_SCHEMA.entities.entity_version IS 'version of the entity';
COMMENT ON COLUMN POLARIS_SCHEMA.entities.type_code IS 'type code';
COMMENT ON COLUMN POLARIS_SCHEMA.entities.sub_type_code IS 'sub type of entity';
COMMENT ON COLUMN POLARIS_SCHEMA.entities.create_timestamp IS 'creation time of entity';
COMMENT ON COLUMN POLARIS_SCHEMA.entities.drop_timestamp IS 'time of drop of entity';
COMMENT ON COLUMN POLARIS_SCHEMA.entities.purge_timestamp IS 'time to start purging entity';
COMMENT ON COLUMN POLARIS_SCHEMA.entities.last_update_timestamp IS 'last time the entity is touched';
COMMENT ON COLUMN POLARIS_SCHEMA.entities.properties IS 'entities properties json';
COMMENT ON COLUMN POLARIS_SCHEMA.entities.internal_properties IS 'entities internal properties json';
COMMENT ON COLUMN POLARIS_SCHEMA.entities.grant_records_version IS 'the version of grant records change on the entity';

DROP TABLE IF EXISTS POLARIS_SCHEMA.grant_records;
CREATE TABLE IF NOT EXISTS POLARIS_SCHEMA.grant_records (
    securable_catalog_id BIGINT NOT NULL,
    securable_id BIGINT NOT NULL,
    grantee_catalog_id BIGINT NOT NULL,
    grantee_id BIGINT NOT NULL,
    privilege_code INTEGER,
    PRIMARY KEY (securable_catalog_id, securable_id, grantee_catalog_id, grantee_id, privilege_code)
);

COMMENT ON TABLE POLARIS_SCHEMA.grant_records IS 'grant records for entities';
COMMENT ON COLUMN POLARIS_SCHEMA.grant_records.securable_catalog_id IS 'catalog id of the securable';
COMMENT ON COLUMN POLARIS_SCHEMA.grant_records.securable_id IS 'entity id of the securable';
COMMENT ON COLUMN POLARIS_SCHEMA.grant_records.grantee_catalog_id IS 'catalog id of the grantee';
COMMENT ON COLUMN POLARIS_SCHEMA.grant_records.grantee_id IS 'id of the grantee';
COMMENT ON COLUMN POLARIS_SCHEMA.grant_records.privilege_code IS 'privilege code';

DROP TABLE IF EXISTS POLARIS_SCHEMA.principal_authentication_data;
CREATE TABLE IF NOT EXISTS principal_authentication_data (
    principal_id BIGINT NOT NULL,
    principal_client_id VARCHAR(255) NOT NULL,
    main_secret_hash VARCHAR(255) NOT NULL,
    secondary_secret_hash VARCHAR(255) NOT NULL,
    secret_salt VARCHAR(255) NOT NULL,
    PRIMARY KEY (principal_client_id)
);

COMMENT ON TABLE POLARIS_SCHEMA.principal_authentication_data IS 'authentication data for client';