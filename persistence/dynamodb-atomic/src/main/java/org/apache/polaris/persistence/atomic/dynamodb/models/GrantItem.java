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

import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_GRANTEE_CATALOG_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_GRANTEE_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_PRIVILEGE_CODE;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_SECURABLE_CATALOG_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_SECURABLE_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.GSI2_PK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.GSI2_SK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.PK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.SK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.byGranteeGsiPk;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.byGranteeGsiSk;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.entityPk;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.grantSk;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.n;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.num;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.s;

import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Maps a {@link PolarisGrantRecord} to/from the DynamoDB "grant item". Mirrors {@code
 * ModelGrantRecord} on the JDBC side.
 *
 * <p>Co-located with the securable: {@code PK = realmId#securableId}, {@code SK =
 * GRANT#granteeId#privilegeCode}, so "grants by securable" is a single partition Query. The
 * by-grantee GSI keys ({@code gsi2pk/gsi2sk}) serve "grants by grantee".
 */
public final class GrantItem {

  private GrantItem() {}

  public static Map<String, AttributeValue> toItem(String realmId, PolarisGrantRecord g) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(PK, s(entityPk(realmId, g.getSecurableId())));
    item.put(SK, s(grantSk(g.getGranteeId(), g.getPrivilegeCode())));

    item.put(A_SECURABLE_ID, n(g.getSecurableId()));
    item.put(A_SECURABLE_CATALOG_ID, n(g.getSecurableCatalogId()));
    item.put(A_GRANTEE_ID, n(g.getGranteeId()));
    item.put(A_GRANTEE_CATALOG_ID, n(g.getGranteeCatalogId()));
    item.put(A_PRIVILEGE_CODE, n(g.getPrivilegeCode()));

    // by-grantee GSI keys -> serve "grants by grantee".
    item.put(GSI2_PK, s(byGranteeGsiPk(realmId, g.getGranteeId())));
    item.put(GSI2_SK, s(byGranteeGsiSk(g.getSecurableId(), g.getPrivilegeCode())));
    return item;
  }

  public static PolarisGrantRecord toGrantRecord(Map<String, AttributeValue> item) {
    return new PolarisGrantRecord(
        num(item, A_SECURABLE_CATALOG_ID),
        num(item, A_SECURABLE_ID),
        num(item, A_GRANTEE_CATALOG_ID),
        num(item, A_GRANTEE_ID),
        (int) num(item, A_PRIVILEGE_CODE));
  }
}
