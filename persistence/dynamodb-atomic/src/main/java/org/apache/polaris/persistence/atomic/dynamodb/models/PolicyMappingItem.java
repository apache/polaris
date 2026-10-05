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

import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_POLICY_CATALOG_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_POLICY_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_POLICY_PARAMETERS;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_POLICY_TYPE_CODE;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_TARGET_CATALOG_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_TARGET_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.PK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.SK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.entityPk;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.policyRefSk;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.policyTargetSk;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.n;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.num;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.s;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.str;

import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Maps a {@link PolarisPolicyMappingRecord} to/from DynamoDB items. Mirrors {@code
 * ModelPolicyMappingRecord} on the JDBC side.
 *
 * <p>A single mapping is stored as two items (an adjacency-list edge, each co-located with one
 * endpoint entity) so both directions are strongly-consistent base-table queries, with no GSI:
 *
 * <ul>
 *   <li><b>target side</b> — {@code PK = realmId#targetId}, {@code SK =
 *       POLICY#typeCode#policyCatalogId#policyId}: serves "policies on a target" (optionally by
 *       type) and the exact-key lookup.
 *   <li><b>policy side</b> — {@code PK = realmId#policyId}, {@code SK =
 *       POLICYREF#targetCatalogId#targetId}: serves "targets of a policy".
 * </ul>
 *
 * <p>Both items carry the full record payload, so either can be read back into a {@link
 * PolarisPolicyMappingRecord}. Writers persist/delete the pair atomically via {@code
 * TransactWriteItems}.
 */
public final class PolicyMappingItem {

  private PolicyMappingItem() {}

  /** Target-side item (under the target entity's partition). */
  public static Map<String, AttributeValue> toTargetItem(
      String realmId, PolarisPolicyMappingRecord r) {
    Map<String, AttributeValue> item = payload(r);
    item.put(PK, s(entityPk(realmId, r.getTargetId())));
    item.put(SK, s(policyTargetSk(r.getPolicyTypeCode(), r.getPolicyCatalogId(), r.getPolicyId())));
    return item;
  }

  /** Policy-side item (under the policy entity's partition). */
  public static Map<String, AttributeValue> toPolicyItem(
      String realmId, PolarisPolicyMappingRecord r) {
    Map<String, AttributeValue> item = payload(r);
    item.put(PK, s(entityPk(realmId, r.getPolicyId())));
    item.put(SK, s(policyRefSk(r.getTargetCatalogId(), r.getTargetId())));
    return item;
  }

  public static PolarisPolicyMappingRecord toRecord(Map<String, AttributeValue> item) {
    return new PolarisPolicyMappingRecord(
        num(item, A_TARGET_CATALOG_ID),
        num(item, A_TARGET_ID),
        num(item, A_POLICY_CATALOG_ID),
        num(item, A_POLICY_ID),
        (int) num(item, A_POLICY_TYPE_CODE),
        str(item, A_POLICY_PARAMETERS));
  }

  private static Map<String, AttributeValue> payload(PolarisPolicyMappingRecord r) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(A_TARGET_CATALOG_ID, n(r.getTargetCatalogId()));
    item.put(A_TARGET_ID, n(r.getTargetId()));
    item.put(A_POLICY_CATALOG_ID, n(r.getPolicyCatalogId()));
    item.put(A_POLICY_ID, n(r.getPolicyId()));
    item.put(A_POLICY_TYPE_CODE, n(r.getPolicyTypeCode()));
    if (r.getParameters() != null) {
      item.put(A_POLICY_PARAMETERS, s(r.getParameters()));
    }
    return item;
  }
}
