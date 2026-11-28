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

import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.A_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.PK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.SK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.childPk;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.childSk;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.num;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.numberAttr;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.stringAttr;

import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * The child record: a strongly consistent, base-table stand-in for the {@code gsi1} listing copy.
 * One small item per child, keyed by the parent's coordinates (realm, catalog, parent, type,
 * subtype) with the child's id in the sort key, so a parent's children, or one subtype of them, can
 * be listed by a base-table {@code Query} with {@code ConsistentRead=true}.
 *
 * <p>It is written in the same {@code TransactWriteItems} as the entity: added on create, moved on
 * re-parent, and removed on delete. A pure rename leaves it in place, since it is keyed by parent
 * and id, not name. So the list a correctness check reads never lags behind a write that already
 * completed. Its sole payload is the child entity's {@code id}.
 */
public final class ChildItem {

  private ChildItem() {}

  public static Map<String, AttributeValue> toItem(
      String realmId, long catalogId, long parentId, int typeCode, int subTypeCode, long entityId) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(PK, stringAttr(childPk(realmId, catalogId, parentId, typeCode, subTypeCode)));
    item.put(SK, stringAttr(childSk(entityId)));
    item.put(A_ID, numberAttr(entityId));
    return Map.copyOf(item);
  }

  /** The child entity id this record points at, or 0 if absent. */
  public static long entityId(Map<String, AttributeValue> item) {
    return num(item, A_ID);
  }
}
