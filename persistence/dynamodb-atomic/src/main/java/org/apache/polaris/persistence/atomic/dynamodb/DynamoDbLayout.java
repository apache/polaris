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

import java.util.Locale;

/**
 * Attribute names, item-type markers, and key encodings for the single-table DynamoDB layout.
 *
 * <p>One base table holds the Polaris item types. The entity item and its name sentinel are each
 * partitioned by their own identity, so load spreads across partitions. A child record is the
 * exception: it is partitioned by its parent's coordinates, so a parent's children share one
 * base-table partition key and can be listed by a strongly consistent {@code Query}. A busy parent,
 * or a realm's principals and catalogs under {@code realm#0#0#type}, therefore shares one partition
 * and pays base-table write capacity on every create and delete. The {@code realmId} is only a key
 * prefix (tenant isolation), never a partition key on its own.
 */
public final class DynamoDbLayout {

  private DynamoDbLayout() {}

  // ---- Key attributes --------------------------------------------------------------------------
  public static final String PK = "PK";
  public static final String SK = "SK";

  // ---- Sort-key / item-type markers ------------------------------------------------------------
  public static final String SK_ENTITY = "ENTITY";
  public static final String SK_NAME = "NAME";
  public static final String SK_CHILD_PREFIX = "CHILD#";
  // A grant is stored twice. The two items can share a partition (an entity that is a securable in
  // one grant and a grantee in another), so each side has its own sort-key prefix; otherwise a
  // begins_with query could not tell "grants on this securable" from "grants held by this grantee".
  public static final String SK_GRANT_ON_PREFIX = "GRANT#ON#";
  public static final String SK_GRANT_TO_PREFIX = "GRANT#TO#";

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

  // ---- Grant-record attributes -----------------------------------------------------------------
  public static final String A_SECURABLE_CATALOG_ID = "securableCatalogId";
  public static final String A_SECURABLE_ID = "securableId";
  public static final String A_GRANTEE_CATALOG_ID = "granteeCatalogId";
  public static final String A_GRANTEE_ID = "granteeId";
  public static final String A_PRIVILEGE_CODE = "privilegeCode";

  private static final String SEP = "#";

  // ---- Key encodings ---------------------------------------------------------------------------

  /** Entity partition key: {@code realmId#id} (one partition per entity). */
  public static String entityPk(String realmId, long id) {
    return realmId + SEP + id;
  }

  /**
   * Name-sentinel partition key. {@code realmId} is length-prefixed so the key stays unambiguous
   * when {@code realmId} or {@code name} contains the separator; both are free-form, whereas the
   * numeric coordinates cannot contain it.
   */
  public static String sentinelPk(
      String realmId, long catalogId, long parentId, int typeCode, String name) {
    return realmId.length()
        + SEP
        + realmId
        + SEP
        + catalogId
        + SEP
        + parentId
        + SEP
        + typeCode
        + SEP
        + name;
  }

  /**
   * Child-record partition key in the base table: {@code
   * realmId#catalogId#parentId#typeCode#subTypeCode}. A child record is a strongly consistent,
   * base-table stand-in for a listing copy, so a listing can read children with {@code
   * ConsistentRead=true}. The subtype is part of the key so that listing one subtype (for example
   * all Iceberg tables) is a single exact query; types with no subtype always use {@code 0}, so
   * their listing is unaffected.
   */
  public static String childPk(
      String realmId, long catalogId, long parentId, int typeCode, int subTypeCode) {
    return realmId + SEP + catalogId + SEP + parentId + SEP + typeCode + SEP + subTypeCode;
  }

  /**
   * Child-record sort key: {@code CHILD#} and the entity id, zero-padded to 19 digits so that
   * string order matches numeric order. The {@code CHILD#} prefix keeps these items apart from the
   * {@code ENTITY} and {@code NAME} items that may share a partition key. Entity ids are
   * non-negative.
   */
  public static String childSk(long id) {
    return SK_CHILD_PREFIX + String.format(Locale.ROOT, "%019d", id);
  }

  /**
   * Securable-side grant sort key: {@code GRANT#ON#}, the grantee id zero-padded to 19 digits, and
   * the privilege code. This item lives in the securable's partition ({@link #entityPk} of the
   * securable), so {@code begins_with(GRANT#ON#)} on that partition is a strongly-consistent read
   * of all grants on the securable, with no secondary index.
   */
  public static String grantOnSk(long granteeId, int privilegeCode) {
    return SK_GRANT_ON_PREFIX
        + String.format(Locale.ROOT, "%019d", granteeId)
        + SEP
        + privilegeCode;
  }

  /**
   * Grantee-side grant sort key: {@code GRANT#TO#}, the securable id zero-padded to 19 digits, and
   * the privilege code. This item lives in the grantee's partition, so {@code
   * begins_with(GRANT#TO#)} on that partition is a strongly-consistent read of all grants held by
   * the grantee. The distinct prefix keeps it apart from any securable-side grant that shares the
   * partition.
   */
  public static String grantToSk(long securableId, int privilegeCode) {
    return SK_GRANT_TO_PREFIX
        + String.format(Locale.ROOT, "%019d", securableId)
        + SEP
        + privilegeCode;
  }
}
