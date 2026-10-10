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
 * <p>One base table holds the Polaris item types, each partitioned by its own identity so load
 * spreads across partitions; a sparse GSI serves the by-name lookup. The {@code realmId} is only a
 * key prefix (tenant isolation), never a partition key on its own.
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

  // ---- Sort-key / item-type markers ------------------------------------------------------------
  public static final String SK_ENTITY = "ENTITY";
  public static final String SK_NAME = "NAME";

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

  private static final String SEP = "#";

  // ---- Key encodings ---------------------------------------------------------------------------

  /** Entity partition key: {@code realmId#id} (one partition per entity). */
  public static String entityPk(String realmId, long id) {
    return realmId + SEP + id;
  }

  /** Name-sentinel partition key: {@code realmId#catalogId#parentId#typeCode#name}. */
  public static String sentinelPk(
      String realmId, long catalogId, long parentId, int typeCode, String name) {
    return realmId + SEP + catalogId + SEP + parentId + SEP + typeCode + SEP + name;
  }

  /** by-name GSI partition key: {@code realmId#catalogId#parentId#typeCode}. */
  public static String byNameGsiPk(String realmId, long catalogId, long parentId, int typeCode) {
    return realmId + SEP + catalogId + SEP + parentId + SEP + typeCode;
  }
}
