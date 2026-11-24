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
import org.apache.polaris.persistence.atomic.dynamodb.DynamoDbLayout;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class ChildItemTest {

  @Test
  void toItemKeysByParentCoordinatesAndCarriesChildId() {
    Map<String, AttributeValue> item = ChildItem.toItem("realm1", 7L, 3L, 2, 3, 42L);

    assertThat(item.get(DynamoDbLayout.PK).s()).isEqualTo("realm1#7#3#2#3");
    assertThat(item.get(DynamoDbLayout.SK).s()).isEqualTo("CHILD#0000000000000000042");
    assertThat(item.get(DynamoDbLayout.A_ID).n()).isEqualTo("42");
  }

  @Test
  void entityIdReadsBackTheChildId() {
    Map<String, AttributeValue> item = ChildItem.toItem("realm1", 7L, 3L, 2, 3, 42L);

    assertThat(ChildItem.entityId(item)).isEqualTo(42L);
  }

  @Test
  void entityIdDefaultsToZeroWhenAbsent() {
    assertThat(ChildItem.entityId(Map.of())).isZero();
  }
}
