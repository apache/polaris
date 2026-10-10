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
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class EntityItemTest {

  private static PolarisBaseEntity sampleEntity() {
    return new PolarisBaseEntity.Builder()
        .id(42L)
        .catalogId(7L)
        .parentId(3L)
        .typeCode(2)
        .subTypeCode(5)
        .name("my_table")
        .entityVersion(4)
        .grantRecordsVersion(6)
        .createTimestamp(1000L)
        .dropTimestamp(0L)
        .purgeTimestamp(0L)
        .toPurgeTimestamp(0L)
        .lastUpdateTimestamp(2000L)
        .propertiesAsMap(Map.of("k1", "v1", "k2", "v2"))
        .internalPropertiesAsMap(Map.of("ik", "iv"))
        .build();
  }

  @Test
  void toItemWritesKeysAndGsiAttributes() {
    Map<String, AttributeValue> item = EntityItem.toItem("realm1", sampleEntity());

    assertThat(item.get(DynamoDbConstants.PK).s()).isEqualTo("realm1#42");
    assertThat(item.get(DynamoDbConstants.SK).s()).isEqualTo(DynamoDbConstants.SK_ENTITY);
    assertThat(item.get(DynamoDbConstants.GSI1_PK).s()).isEqualTo("realm1#7#3#2");
    assertThat(item.get(DynamoDbConstants.GSI1_SK).s()).isEqualTo("my_table");
  }

  @Test
  void roundTripPreservesAllFields() {
    PolarisBaseEntity original = sampleEntity();

    PolarisBaseEntity back = EntityItem.toEntity(EntityItem.toItem("realm1", original));

    assertThat(back.getId()).isEqualTo(42L);
    assertThat(back.getCatalogId()).isEqualTo(7L);
    assertThat(back.getParentId()).isEqualTo(3L);
    assertThat(back.getTypeCode()).isEqualTo(2);
    assertThat(back.getSubTypeCode()).isEqualTo(5);
    assertThat(back.getName()).isEqualTo("my_table");
    assertThat(back.getEntityVersion()).isEqualTo(4);
    assertThat(back.getGrantRecordsVersion()).isEqualTo(6);
    assertThat(back.getCreateTimestamp()).isEqualTo(1000L);
    assertThat(back.getLastUpdateTimestamp()).isEqualTo(2000L);
    assertThat(back.getPropertiesAsMap())
        .containsExactlyInAnyOrderEntriesOf(Map.of("k1", "v1", "k2", "v2"));
    assertThat(back.getInternalPropertiesAsMap())
        .containsExactlyInAnyOrderEntriesOf(Map.of("ik", "iv"));
  }

  @Test
  void emptyPropertyMapsRoundTripToEmpty() {
    PolarisBaseEntity entity =
        new PolarisBaseEntity.Builder()
            .id(1L)
            .catalogId(1L)
            .parentId(0L)
            .typeCode(1)
            .subTypeCode(0)
            .name("n")
            .entityVersion(1)
            .grantRecordsVersion(1)
            .createTimestamp(1L)
            .dropTimestamp(0L)
            .purgeTimestamp(0L)
            .toPurgeTimestamp(0L)
            .lastUpdateTimestamp(1L)
            .propertiesAsMap(Map.of())
            .internalPropertiesAsMap(Map.of())
            .build();

    PolarisBaseEntity back = EntityItem.toEntity(EntityItem.toItem("realm1", entity));

    assertThat(back.getPropertiesAsMap()).isEmpty();
    assertThat(back.getInternalPropertiesAsMap()).isEmpty();
  }
}
