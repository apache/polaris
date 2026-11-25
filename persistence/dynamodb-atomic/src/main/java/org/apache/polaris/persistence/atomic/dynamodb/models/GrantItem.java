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

import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.A_GRANTEE_CATALOG_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.A_GRANTEE_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.A_PRIVILEGE_CODE;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.A_SECURABLE_CATALOG_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.A_SECURABLE_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.PK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.SK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.entityPk;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.grantOnSk;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout.grantToSk;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.num;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.numberAttr;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.stringAttr;

import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * A grant record, stored as two base-table items so both authorization reads are strongly
 * consistent without a secondary index. Each item carries all five grant fields, so a read of
 * either side rebuilds the full {@link PolarisGrantRecord}.
 *
 * <ul>
 *   <li>The <b>securable-side</b> item lives in the securable's partition ({@code realmId#
 *       securableId}) under the {@code GRANT#ON#} prefix, keyed by the grantee, and answers "grants
 *       on this securable".
 *   <li>The <b>grantee-side</b> item lives in the grantee's partition ({@code realmId#granteeId})
 *       under the {@code GRANT#TO#} prefix, keyed by the securable, and answers "grants held by
 *       this grantee".
 * </ul>
 *
 * <p>The two sides use distinct sort-key prefixes, so even a self-grant (securableId == granteeId)
 * is two separate items in the one partition, and the per-side queries never mix them.
 */
public final class GrantItem {

  private GrantItem() {}

  /** The grant's securable-side item, in the securable's partition. */
  public static Map<String, AttributeValue> securableItem(
      String realmId, PolarisGrantRecord grant) {
    return item(
        entityPk(realmId, grant.getSecurableId()),
        grantOnSk(grant.getGranteeId(), grant.getPrivilegeCode()),
        grant);
  }

  /** The grant's grantee-side mirror, in the grantee's partition. */
  public static Map<String, AttributeValue> granteeItem(String realmId, PolarisGrantRecord grant) {
    return item(
        entityPk(realmId, grant.getGranteeId()),
        grantToSk(grant.getSecurableId(), grant.getPrivilegeCode()),
        grant);
  }

  private static Map<String, AttributeValue> item(String pk, String sk, PolarisGrantRecord grant) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(PK, stringAttr(pk));
    item.put(SK, stringAttr(sk));
    item.put(A_SECURABLE_CATALOG_ID, numberAttr(grant.getSecurableCatalogId()));
    item.put(A_SECURABLE_ID, numberAttr(grant.getSecurableId()));
    item.put(A_GRANTEE_CATALOG_ID, numberAttr(grant.getGranteeCatalogId()));
    item.put(A_GRANTEE_ID, numberAttr(grant.getGranteeId()));
    item.put(A_PRIVILEGE_CODE, numberAttr(grant.getPrivilegeCode()));
    return Map.copyOf(item);
  }

  /** Rebuilds the grant record from either side's item. */
  public static PolarisGrantRecord toGrantRecord(Map<String, AttributeValue> item) {
    return new PolarisGrantRecord(
        num(item, A_SECURABLE_CATALOG_ID),
        num(item, A_SECURABLE_ID),
        num(item, A_GRANTEE_CATALOG_ID),
        num(item, A_GRANTEE_ID),
        (int) num(item, A_PRIVILEGE_CODE));
  }
}
