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

-- Events schema for v4 (H2)
CREATE SCHEMA IF NOT EXISTS POLARIS_SCHEMA;
SET SCHEMA POLARIS_SCHEMA;

CREATE TABLE IF NOT EXISTS version (
    version_key VARCHAR PRIMARY KEY,
    version_value INTEGER NOT NULL
);
MERGE INTO version (version_key, version_value)
    KEY (version_key)
    VALUES ('version', 4);
COMMENT ON TABLE version IS 'the version of the JDBC schema in use';

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
    additional_properties TEXT NOT NULL,
    PRIMARY KEY (event_id)
);
