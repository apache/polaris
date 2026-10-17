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
package org.apache.polaris.persistence.atomic.dynamodb.models;

import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_CATALOG_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_CREATE_TIMESTAMP;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_DROP_TIMESTAMP;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_ENTITY_VERSION;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_GRANT_RECORDS_VERSION;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_INTERNAL_PROPERTIES;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_LAST_UPDATE_TIMESTAMP;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_NAME;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_PARENT_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_PROPERTIES;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_PURGE_TIMESTAMP;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_SUB_TYPE_CODE;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_TO_PURGE_TIMESTAMP;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_TYPE_CODE;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.GSI1_PK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.GSI1_SK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.PK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.SK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.SK_ENTITY;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.byNameGsiPk;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.entityPk;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Maps a {@link PolarisBaseEntity} to and from its DynamoDB "entity item" representation.
 *
 * <p>The item is keyed by {@code PK = realmId#id}, {@code SK = "ENTITY"}, and carries the by-name
 * GSI keys so a parent's children can be listed.
 */
public final class EntityItem {

  private EntityItem() {}

  /** Serialize an entity to a DynamoDB item map for the given realm. */
  public static Map<String, AttributeValue> toItem(String realmId, PolarisBaseEntity e) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(PK, stringAttr(entityPk(realmId, e.getId())));
    item.put(SK, stringAttr(SK_ENTITY));

    item.put(A_ID, numberAttr(e.getId()));
    item.put(A_CATALOG_ID, numberAttr(e.getCatalogId()));
    item.put(A_PARENT_ID, numberAttr(e.getParentId()));
    item.put(A_TYPE_CODE, numberAttr(e.getTypeCode()));
    item.put(A_SUB_TYPE_CODE, numberAttr(e.getSubTypeCode()));
    item.put(A_NAME, stringAttr(e.getName()));
    item.put(A_ENTITY_VERSION, numberAttr(e.getEntityVersion()));
    item.put(A_GRANT_RECORDS_VERSION, numberAttr(e.getGrantRecordsVersion()));
    item.put(A_CREATE_TIMESTAMP, numberAttr(e.getCreateTimestamp()));
    item.put(A_DROP_TIMESTAMP, numberAttr(e.getDropTimestamp()));
    item.put(A_PURGE_TIMESTAMP, numberAttr(e.getPurgeTimestamp()));
    item.put(A_TO_PURGE_TIMESTAMP, numberAttr(e.getToPurgeTimestamp()));
    item.put(A_LAST_UPDATE_TIMESTAMP, numberAttr(e.getLastUpdateTimestamp()));
    item.put(A_PROPERTIES, mapAttr(e.getPropertiesAsMap()));
    item.put(A_INTERNAL_PROPERTIES, mapAttr(e.getInternalPropertiesAsMap()));

    // by-name GSI keys (sparse: only entity items carry them) -> serve "list children".
    item.put(
        GSI1_PK,
        stringAttr(byNameGsiPk(realmId, e.getCatalogId(), e.getParentId(), e.getTypeCode())));
    item.put(GSI1_SK, stringAttr(e.getName()));
    return item;
  }

  /** Deserialize a DynamoDB entity item back into a {@link PolarisBaseEntity}. */
  public static PolarisBaseEntity toEntity(Map<String, AttributeValue> item) {
    return new PolarisBaseEntity.Builder()
        .id(num(item, A_ID))
        .catalogId(num(item, A_CATALOG_ID))
        .parentId(num(item, A_PARENT_ID))
        .typeCode((int) num(item, A_TYPE_CODE))
        .subTypeCode((int) num(item, A_SUB_TYPE_CODE))
        .name(str(item, A_NAME))
        .entityVersion((int) num(item, A_ENTITY_VERSION))
        .grantRecordsVersion((int) num(item, A_GRANT_RECORDS_VERSION))
        .createTimestamp(num(item, A_CREATE_TIMESTAMP))
        .dropTimestamp(num(item, A_DROP_TIMESTAMP))
        .purgeTimestamp(num(item, A_PURGE_TIMESTAMP))
        .toPurgeTimestamp(num(item, A_TO_PURGE_TIMESTAMP))
        .lastUpdateTimestamp(num(item, A_LAST_UPDATE_TIMESTAMP))
        .propertiesAsMap(strMap(item, A_PROPERTIES))
        .internalPropertiesAsMap(strMap(item, A_INTERNAL_PROPERTIES))
        .build();
  }

  // ---- small AttributeValue helpers (shared shape across item mappers) -------------------------

  static AttributeValue stringAttr(String v) {
    return AttributeValue.fromS(v);
  }

  static AttributeValue numberAttr(long v) {
    return AttributeValue.fromN(Long.toString(v));
  }

  static AttributeValue mapAttr(Map<String, String> m) {
    Map<String, AttributeValue> out =
        m.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> stringAttr(e.getValue())));
    return AttributeValue.fromM(out);
  }

  static String str(Map<String, AttributeValue> item, String key) {
    AttributeValue v = item.get(key);
    return v == null ? null : v.s();
  }

  static long num(Map<String, AttributeValue> item, String key) {
    AttributeValue v = item.get(key);
    return v == null || v.n() == null ? 0L : Long.parseLong(v.n());
  }

  static Map<String, String> strMap(Map<String, AttributeValue> item, String key) {
    AttributeValue v = item.get(key);
    if (v == null || v.m() == null) {
      return Map.of();
    }
    return v.m().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().s()));
  }
}
