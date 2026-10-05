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

import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.PK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.SK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.SK_NAME;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.sentinelPk;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.n;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.num;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.s;

import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * The name-sentinel item — DynamoDB's stand-in for a multi-column UNIQUE constraint. A small guard
 * item keyed by {@code realmId#catalogId#parentId#typeCode#name} whose sole payload is the owning
 * entity's {@code id}.
 *
 * <p>Its presence "claims" the name: a duplicate create fails its {@code attribute_not_exists}
 * condition inside the same {@code TransactWriteItems} that writes the entity, so uniqueness holds
 * atomically. There is no JDBC counterpart — the relational backend relies on a UNIQUE index.
 */
public final class NameSentinelItem {

  private NameSentinelItem() {}

  public static Map<String, AttributeValue> toItem(
      String realmId, long catalogId, long parentId, int typeCode, String name, long entityId) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(PK, s(sentinelPk(realmId, catalogId, parentId, typeCode, name)));
    item.put(SK, s(SK_NAME));
    item.put(A_ID, n(entityId));
    return item;
  }

  /** The entity id a sentinel points at, or 0 if absent. */
  public static long entityId(Map<String, AttributeValue> item) {
    return num(item, A_ID);
  }
}
