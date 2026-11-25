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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class GrantItemTest {

  private static final PolarisGrantRecord GRANT = new PolarisGrantRecord(100L, 428371L, 0L, 55L, 9);

  @Test
  void securableItemKeysBySecurablePartitionAndGranteeSortKey() {
    Map<String, AttributeValue> item = GrantItem.securableItem("realm1", GRANT);

    assertThat(item.get(DynamoDbLayout.PK).s()).isEqualTo("realm1#428371");
    assertThat(item.get(DynamoDbLayout.SK).s()).isEqualTo("GRANT#ON#0000000000000000055#9");
  }

  @Test
  void granteeItemKeysByGranteePartitionAndSecurableSortKey() {
    Map<String, AttributeValue> item = GrantItem.granteeItem("realm1", GRANT);

    assertThat(item.get(DynamoDbLayout.PK).s()).isEqualTo("realm1#55");
    assertThat(item.get(DynamoDbLayout.SK).s()).isEqualTo("GRANT#TO#0000000000000428371#9");
  }

  @Test
  void toGrantRecordRoundTripsFromEitherSide() {
    assertThat(GrantItem.toGrantRecord(GrantItem.securableItem("realm1", GRANT))).isEqualTo(GRANT);
    assertThat(GrantItem.toGrantRecord(GrantItem.granteeItem("realm1", GRANT))).isEqualTo(GRANT);
  }

  @Test
  void selfGrantIsTwoItemsInOnePartitionToldApartBySortKeyPrefix() {
    PolarisGrantRecord selfGrant = new PolarisGrantRecord(100L, 42L, 100L, 42L, 9);

    Map<String, AttributeValue> securable = GrantItem.securableItem("realm1", selfGrant);
    Map<String, AttributeValue> grantee = GrantItem.granteeItem("realm1", selfGrant);

    // Same partition (the entity grants to itself), but distinct sort keys, so both sides coexist.
    assertThat(securable.get(DynamoDbLayout.PK)).isEqualTo(grantee.get(DynamoDbLayout.PK));
    assertThat(securable.get(DynamoDbLayout.SK)).isNotEqualTo(grantee.get(DynamoDbLayout.SK));
    assertThat(securable.get(DynamoDbLayout.SK).s()).startsWith("GRANT#ON#");
    assertThat(grantee.get(DynamoDbLayout.SK).s()).startsWith("GRANT#TO#");
  }
}
