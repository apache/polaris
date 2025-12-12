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

CREATE SEQUENCE IF NOT EXISTS Ids OPTIONS(sequence_kind = 'bit_reversed_positive');

CREATE TABLE IF NOT EXISTS Realms(
  RealmId STRING(MAX) NOT NULL
)
PRIMARY KEY (RealmId);

CREATE TABLE IF NOT EXISTS Entities(
RealmId STRING(MAX) NOT NULL,
CatalogId INT64 NOT NULL,
Id INT64 NOT NULL,
ParentId INT64 NOT NULL,
Name STRING(MAX),
EntityVersion INT64 NOT NULL,
TypeCode INT64 NOT NULL,
SubTypeCode INT64 NOT NULL,
CreateTimestamp INT64,
DropTimestamp INT64,
PurgeTimestamp INT64,
ToPurgeTimestamp INT64,
LastUpdateTimestamp INT64,
`Properties` JSON,
InternalProperties JSON,
GrantRecordsVersion INT64,
LocationWithoutScheme STRING(MAX))
PRIMARY KEY (RealmId,Id),
INTERLEAVE IN PARENT Realms ON DELETE CASCADE;

CREATE UNIQUE NULL_FILTERED INDEX IF NOT EXISTS EntityNameIndex ON Entities(RealmId,CatalogId,ParentId,TypeCode,Name);

CREATE INDEX IF NOT EXISTS EntityChildrenIndex ON Entities(RealmId,ParentId, TypeCode);
CREATE INDEX IF NOT EXISTS EntityLocationsIndex ON Entities (RealmId, ParentId, LocationWithoutScheme) WHERE LocationWithoutScheme IS NOT NULL;

CREATE TABLE IF NOT EXISTS GrantRecords(
  RealmId STRING(MAX) NOT NULL,
  SecurableCatalogId INT64 NOT NULL,
  SecurableId INT64 NOT NULL,
  GranteeCatalogId INT64 NOT NULL,
  GranteeId INT64 NOT NULL,
  PrivilegeCode INT64)
PRIMARY KEY (RealmId,SecurableCatalogId,SecurableId,GranteeCatalogId,GranteeId,PrivilegeCode),
INTERLEAVE IN PARENT Realms ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS GrantRecordsGranteeIndex ON GrantRecords(RealmId,GranteeCatalogId,GranteeId);

CREATE TABLE IF NOT EXISTS PrincipalAuthenticationData(
  RealmId STRING(MAX) NOT NULL,
  PrincipalClientId STRING(MAX) NOT NULL,
  PrincipalId INT64 NOT NULL,
  MainSecretHash STRING(MAX) NOT NULL,
  SecondarySecretHash STRING(MAX) NOT NULL,
  SecretSalt STRING(MAX) NOT NULL)
PRIMARY KEY (RealmId,PrincipalClientId,PrincipalId),
INTERLEAVE IN PARENT Realms ON DELETE CASCADE;


CREATE TABLE IF NOT EXISTS PolicyMapping(
  RealmId STRING(MAX) NOT NULL,
  TargetCatalogId INT64 NOT NULL,
  TargetId INT64 NOT NULL,
  PolicyTypeCode INT64 NOT NULL,
  PolicyCatalogId INT64 NOT NULL,
  PolicyId INT64 NOT NULL,
  Parameters JSON)
PRIMARY KEY (RealmId, TargetCatalogId, TargetId, PolicyTypeCode, PolicyCatalogId, PolicyId),
INTERLEAVE IN PARENT Realms ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS PolicyMappingPolicyIndex ON PolicyMapping(RealmId,PolicyCatalogId,PolicyId,PolicyTypeCode) STORING(Parameters);