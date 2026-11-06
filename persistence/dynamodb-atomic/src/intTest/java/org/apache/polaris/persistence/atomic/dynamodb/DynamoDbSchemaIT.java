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
package org.apache.polaris.persistence.atomic.dynamodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.tuple;

import java.util.Map;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

class DynamoDbSchemaIT extends AbstractDynamoDbTest {

  @Test
  void createIfAbsentProvisionsTableAndByNameGsi() {
    String table = uniqueTableName();
    DynamoDbSchema.createIfAbsent(client, table);
    try {
      TableDescription description = client.describeTable(b -> b.tableName(table)).table();

      assertThat(description.keySchema())
          .extracting(KeySchemaElement::attributeName, KeySchemaElement::keyType)
          .containsExactly(
              tuple(DynamoDbLayout.PK, KeyType.HASH), tuple(DynamoDbLayout.SK, KeyType.RANGE));

      assertThat(description.globalSecondaryIndexes()).hasSize(1);
      GlobalSecondaryIndexDescription gsi = description.globalSecondaryIndexes().get(0);
      assertThat(gsi.indexName()).isEqualTo(DynamoDbLayout.GSI1);
      assertThat(gsi.keySchema())
          .extracting(KeySchemaElement::attributeName, KeySchemaElement::keyType)
          .containsExactly(
              tuple(DynamoDbLayout.GSI1_PK, KeyType.HASH),
              tuple(DynamoDbLayout.GSI1_SK, KeyType.RANGE));
      assertThat(gsi.projection().projectionType()).isEqualTo(ProjectionType.ALL);
    } finally {
      DynamoDbSchema.deleteIfExists(client, table);
    }
  }

  @Test
  void createIfAbsentIsIdempotent() {
    String table = uniqueTableName();
    DynamoDbSchema.createIfAbsent(client, table);
    try {
      assertThatCode(() -> DynamoDbSchema.createIfAbsent(client, table)).doesNotThrowAnyException();
    } finally {
      DynamoDbSchema.deleteIfExists(client, table);
    }
  }

  @Test
  void byNameGsiReturnsSiblingsWrittenThroughTheMappers() {
    String table = uniqueTableName();
    DynamoDbSchema.createIfAbsent(client, table);
    try {
      String realmId = "realm1";
      PolarisBaseEntity sales = entity(1L, 7L, 3L, 2, "sales");
      PolarisBaseEntity orders = entity(2L, 7L, 3L, 2, "orders");

      client.putItem(b -> b.tableName(table).item(EntityItem.toItem(realmId, sales)));
      client.putItem(b -> b.tableName(table).item(EntityItem.toItem(realmId, orders)));

      // A direct read reconstructs the entity through the mapper.
      Map<String, AttributeValue> stored =
          client
              .getItem(
                  b ->
                      b.tableName(table)
                          .key(
                              Map.of(
                                  DynamoDbLayout.PK,
                                  AttributeValue.fromS(DynamoDbLayout.entityPk(realmId, 1L)),
                                  DynamoDbLayout.SK,
                                  AttributeValue.fromS(DynamoDbLayout.SK_ENTITY))))
              .item();
      PolarisBaseEntity roundTripped = EntityItem.toEntity(stored);
      assertThat(roundTripped.getId()).isEqualTo(1L);
      assertThat(roundTripped.getName()).isEqualTo("sales");
      assertThat(roundTripped.getParentId()).isEqualTo(3L);

      // The by-name GSI lists both children of the same parent.
      String gsiPk = DynamoDbLayout.byNameGsiPk(realmId, 7L, 3L, 2);
      QueryResponse response =
          client.query(
              b ->
                  b.tableName(table)
                      .indexName(DynamoDbLayout.GSI1)
                      .keyConditionExpression("#pk = :pk")
                      .expressionAttributeNames(Map.of("#pk", DynamoDbLayout.GSI1_PK))
                      .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS(gsiPk))));
      assertThat(response.items())
          .extracting(item -> item.get(DynamoDbLayout.GSI1_SK).s())
          .containsExactlyInAnyOrder("sales", "orders");
    } finally {
      DynamoDbSchema.deleteIfExists(client, table);
    }
  }

  private static PolarisBaseEntity entity(
      long id, long catalogId, long parentId, int typeCode, String name) {
    return new PolarisBaseEntity.Builder()
        .id(id)
        .catalogId(catalogId)
        .parentId(parentId)
        .typeCode(typeCode)
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .name(name)
        .propertiesAsMap(Map.of())
        .internalPropertiesAsMap(Map.of())
        .build();
  }
}
