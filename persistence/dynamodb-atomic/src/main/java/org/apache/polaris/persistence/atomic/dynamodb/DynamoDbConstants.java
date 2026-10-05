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
package org.apache.polaris.persistence.atomic.dynamodb;

/**
 * Attribute names, item-type markers, and key encodings for the single-table DynamoDB layout.
 *
 * <p>One base table holds three item types, each partitioned by its own identity so load spreads
 * across partitions; two sparse GSIs serve the non-key lookups. The {@code realmId} is only a key
 * prefix (tenant isolation), never a partition key on its own.
 */
public final class DynamoDbConstants {

  private DynamoDbConstants() {}

  // ---- Key attributes --------------------------------------------------------------------------
  public static final String PK = "PK";
  public static final String SK = "SK";

  // by-name GSI (list children): gsi1pk = realmId#catalogId#parentId#typeCode, gsi1sk = name
  public static final String GSI1 = "gsi1";
  public static final String GSI1_PK = "gsi1pk";
  public static final String GSI1_SK = "gsi1sk";

  // by-grantee GSI (grants by grantee): gsi2pk = realmId#granteeId, gsi2sk = securableId#privilege
  public static final String GSI2 = "gsi2";
  public static final String GSI2_PK = "gsi2pk";
  public static final String GSI2_SK = "gsi2sk";

  // ---- Sort-key / item-type markers
  // --------------------------------------------------------------
  public static final String SK_ENTITY = "ENTITY";
  public static final String SK_NAME = "NAME";
  public static final String SK_GRANT_PREFIX = "GRANT#"; // GRANT#<granteeId>#<privilegeCode>
  public static final String SK_SECRET = "SECRET";
  // policy mapping, target side (under the target entity partition):
  //   POLICY#<policyTypeCode>#<policyCatalogId>#<policyId>
  public static final String SK_POLICY_PREFIX = "POLICY#";
  // policy mapping, policy side (under the policy entity partition):
  //   POLICYREF#<targetCatalogId>#<targetId>
  public static final String SK_POLICY_REF_PREFIX = "POLICYREF#";

  // ---- Entity item attributes ------------------------------------------------------------------
  public static final String A_CATALOG_ID = "catalogId";
  public static final String A_PARENT_ID = "parentId";
  public static final String A_TYPE_CODE = "typeCode";
  public static final String A_SUB_TYPE_CODE = "subTypeCode";
  public static final String A_NAME = "name";
  public static final String A_ENTITY_VERSION = "entityVersion";
  public static final String A_GRANT_RECORDS_VERSION = "grantRecordsVersion";
  public static final String A_CREATE_TIMESTAMP = "createTimestamp";
  public static final String A_DROP_TIMESTAMP = "dropTimestamp";
  public static final String A_PURGE_TIMESTAMP = "purgeTimestamp";
  public static final String A_TO_PURGE_TIMESTAMP = "toPurgeTimestamp";
  public static final String A_LAST_UPDATE_TIMESTAMP = "lastUpdateTimestamp";
  public static final String A_PROPERTIES = "properties";
  public static final String A_INTERNAL_PROPERTIES = "internalProperties";

  // ---- Name-sentinel attributes ----------------------------------------------------------------
  public static final String A_ID = "id";

  // ---- Grant item attributes -------------------------------------------------------------------
  public static final String A_SECURABLE_ID = "securableId";
  public static final String A_SECURABLE_CATALOG_ID = "securableCatalogId";
  public static final String A_GRANTEE_ID = "granteeId";
  public static final String A_GRANTEE_CATALOG_ID = "granteeCatalogId";
  public static final String A_PRIVILEGE_CODE = "privilegeCode";

  // ---- Principal-secret item attributes --------------------------------------------------------
  public static final String A_PRINCIPAL_ID = "principalId";
  public static final String A_CLIENT_ID = "principalClientId";
  public static final String A_MAIN_SECRET_HASH = "mainSecretHash";
  public static final String A_SECONDARY_SECRET_HASH = "secondarySecretHash";
  public static final String A_SECRET_SALT = "secretSalt";

  // ---- Policy-mapping item attributes ----------------------------------------------------------
  public static final String A_TARGET_CATALOG_ID = "targetCatalogId";
  public static final String A_TARGET_ID = "targetId";
  public static final String A_POLICY_CATALOG_ID = "policyCatalogId";
  public static final String A_POLICY_ID = "policyId";
  public static final String A_POLICY_TYPE_CODE = "policyTypeCode";
  public static final String A_POLICY_PARAMETERS = "parameters";

  private static final String SEP = "#";

  // ---- Key encodings ---------------------------------------------------------------------------

  /** Entity / grant partition key: {@code realmId#id} (one partition per entity). */
  public static String entityPk(String realmId, long id) {
    return realmId + SEP + id;
  }

  /** Principal-secret partition key: {@code realmId#SECRET#clientId} (point lookup by clientId). */
  public static String secretPk(String realmId, String clientId) {
    return realmId + SEP + SK_SECRET + SEP + clientId;
  }

  /** Name-sentinel partition key: {@code realmId#catalogId#parentId#typeCode#name}. */
  public static String sentinelPk(
      String realmId, long catalogId, long parentId, int typeCode, String name) {
    return realmId + SEP + catalogId + SEP + parentId + SEP + typeCode + SEP + name;
  }

  /** Grant sort key: {@code GRANT#granteeId#privilegeCode}. */
  public static String grantSk(long granteeId, int privilegeCode) {
    return SK_GRANT_PREFIX + granteeId + SEP + privilegeCode;
  }

  /** by-name GSI partition key: {@code realmId#catalogId#parentId#typeCode}. */
  public static String byNameGsiPk(String realmId, long catalogId, long parentId, int typeCode) {
    return realmId + SEP + catalogId + SEP + parentId + SEP + typeCode;
  }

  /** by-grantee GSI partition key: {@code realmId#granteeId}. */
  public static String byGranteeGsiPk(String realmId, long granteeId) {
    return realmId + SEP + granteeId;
  }

  /** by-grantee GSI sort key: {@code securableId#privilegeCode}. */
  public static String byGranteeGsiSk(long securableId, int privilegeCode) {
    return securableId + SEP + privilegeCode;
  }

  /** Policy-mapping target-side sort key: {@code POLICY#typeCode#policyCatalogId#policyId}. */
  public static String policyTargetSk(int policyTypeCode, long policyCatalogId, long policyId) {
    return SK_POLICY_PREFIX + policyTypeCode + SEP + policyCatalogId + SEP + policyId;
  }

  /** Prefix matching all policies of one type on a target: {@code POLICY#typeCode#}. */
  public static String policyTargetTypePrefix(int policyTypeCode) {
    return SK_POLICY_PREFIX + policyTypeCode + SEP;
  }

  /** Policy-mapping policy-side sort key: {@code POLICYREF#targetCatalogId#targetId}. */
  public static String policyRefSk(long targetCatalogId, long targetId) {
    return SK_POLICY_REF_PREFIX + targetCatalogId + SEP + targetId;
  }
}
