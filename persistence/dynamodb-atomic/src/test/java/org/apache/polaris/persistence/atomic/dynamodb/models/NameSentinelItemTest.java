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
import org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class NameSentinelItemTest {

  @Test
  void toItemKeysByNameCoordinatesAndCarriesEntityId() {
    Map<String, AttributeValue> item =
        NameSentinelItem.toItem("realm1", 7L, 3L, 2, "my_table", 42L);

    assertThat(item.get(DynamoDbConstants.PK).s()).isEqualTo("realm1#7#3#2#my_table");
    assertThat(item.get(DynamoDbConstants.SK).s()).isEqualTo(DynamoDbConstants.SK_NAME);
    assertThat(item.get(DynamoDbConstants.A_ID).n()).isEqualTo("42");
  }

  @Test
  void entityIdReadsBackTheOwningId() {
    Map<String, AttributeValue> item =
        NameSentinelItem.toItem("realm1", 7L, 3L, 2, "my_table", 42L);

    assertThat(NameSentinelItem.entityId(item)).isEqualTo(42L);
  }

  @Test
  void entityIdDefaultsToZeroWhenAbsent() {
    assertThat(NameSentinelItem.entityId(Map.of())).isZero();
  }
}
