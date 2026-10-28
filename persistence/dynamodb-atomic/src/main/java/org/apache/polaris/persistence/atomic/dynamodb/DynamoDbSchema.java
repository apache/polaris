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

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

/**
 * Provisions the single DynamoDB table the backend uses: the base table keyed by {@link
 * DynamoDbLayout#PK}/{@link DynamoDbLayout#SK} plus the by-name global secondary index {@link
 * DynamoDbLayout#GSI1} keyed by {@link DynamoDbLayout#GSI1_PK}/{@link DynamoDbLayout#GSI1_SK}.
 *
 * <p>Provisioning is a control-plane step run at bootstrap, not on the request path.
 */
public final class DynamoDbSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDbSchema.class);

  private DynamoDbSchema() {}

  /**
   * Creates the table and its by-name GSI if they do not already exist, then blocks until the table
   * is active. Safe to call repeatedly: whether this call created the table, found it already
   * present, or lost a create race to another caller, the table that ends up active is validated
   * against the expected layout and otherwise left untouched.
   */
  public static void createIfAbsent(DynamoDbClient client, String tableName) {
    if (!tableExists(client, tableName)) {
      try {
        client.createTable(
            builder ->
                builder
                    .tableName(tableName)
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .attributeDefinitions(
                        stringAttribute(DynamoDbLayout.PK),
                        stringAttribute(DynamoDbLayout.SK),
                        stringAttribute(DynamoDbLayout.GSI1_PK),
                        stringAttribute(DynamoDbLayout.GSI1_SK))
                    .keySchema(
                        key(DynamoDbLayout.PK, KeyType.HASH), key(DynamoDbLayout.SK, KeyType.RANGE))
                    .globalSecondaryIndexes(byNameIndex()));
      } catch (ResourceInUseException concurrentCreate) {
        // Another caller created the table between the existence check and here; that is fine. Its
        // layout is validated after the waiter below, so a racing miscreation is still surfaced.
      }
    }
    client.waiter().waitUntilTableExists(builder -> builder.tableName(tableName));
    // Validate after the waiter so every path -- we created it, it already existed, or we lost the
    // create race -- checks the table that actually ended up active against the expected layout.
    verifyLayout(client.describeTable(builder -> builder.tableName(tableName)).table());
  }

  /** Deletes the table if it exists, blocking until it is gone. Intended for test teardown. */
  public static void deleteIfExists(DynamoDbClient client, String tableName) {
    try {
      client.deleteTable(builder -> builder.tableName(tableName));
      client.waiter().waitUntilTableNotExists(builder -> builder.tableName(tableName));
    } catch (ResourceNotFoundException alreadyGone) {
      // Table already gone -- the expected case for deleteIfExists, so no warning.
      LOGGER.debug("Table {} already absent; nothing to delete.", tableName);
    }
  }

  private static boolean tableExists(DynamoDbClient client, String tableName) {
    try {
      client.describeTable(builder -> builder.tableName(tableName));
      return true;
    } catch (ResourceNotFoundException notFound) {
      return false;
    }
  }

  /**
   * Fails if the active table does not match the layout this backend expects -- key schema and the
   * by-name GSI's key schema and {@code ALL} projection -- surfacing a misconfigured table instead
   * of letting reads and writes misbehave against it.
   */
  private static void verifyLayout(TableDescription table) {
    if (!keySchemaMatches(table.keySchema(), DynamoDbLayout.PK, DynamoDbLayout.SK)) {
      throw new IllegalStateException(
          String.format(
              "Existing table %s has an unexpected key schema; expected %s (HASH) and %s (RANGE).",
              table.tableName(), DynamoDbLayout.PK, DynamoDbLayout.SK));
    }
    GlobalSecondaryIndexDescription gsi =
        table.globalSecondaryIndexes().stream()
            .filter(index -> DynamoDbLayout.GSI1.equals(index.indexName()))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "Existing table %s is missing the %s index.",
                            table.tableName(), DynamoDbLayout.GSI1)));
    if (!keySchemaMatches(gsi.keySchema(), DynamoDbLayout.GSI1_PK, DynamoDbLayout.GSI1_SK)) {
      throw new IllegalStateException(
          String.format(
              "Index %s on table %s has an unexpected key schema; expected %s (HASH) and %s (RANGE).",
              DynamoDbLayout.GSI1,
              table.tableName(),
              DynamoDbLayout.GSI1_PK,
              DynamoDbLayout.GSI1_SK));
    }
    ProjectionType projectionType =
        gsi.projection() == null ? null : gsi.projection().projectionType();
    if (projectionType != ProjectionType.ALL) {
      throw new IllegalStateException(
          String.format(
              "Index %s on table %s has projection %s; expected %s so listing returns full entities.",
              DynamoDbLayout.GSI1, table.tableName(), projectionType, ProjectionType.ALL));
    }
  }

  private static boolean keySchemaMatches(
      List<KeySchemaElement> schema, String hashKey, String rangeKey) {
    String hash = null;
    String range = null;
    for (KeySchemaElement element : schema) {
      if (element.keyType() == KeyType.HASH) {
        hash = element.attributeName();
      } else if (element.keyType() == KeyType.RANGE) {
        range = element.attributeName();
      }
    }
    return schema.size() == 2 && hashKey.equals(hash) && rangeKey.equals(range);
  }

  private static GlobalSecondaryIndex byNameIndex() {
    return GlobalSecondaryIndex.builder()
        .indexName(DynamoDbLayout.GSI1)
        .keySchema(
            key(DynamoDbLayout.GSI1_PK, KeyType.HASH), key(DynamoDbLayout.GSI1_SK, KeyType.RANGE))
        .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
        .build();
  }

  private static AttributeDefinition stringAttribute(String name) {
    return AttributeDefinition.builder()
        .attributeName(name)
        .attributeType(ScalarAttributeType.S)
        .build();
  }

  private static KeySchemaElement key(String name, KeyType type) {
    return KeySchemaElement.builder().attributeName(name).keyType(type).build();
  }
}
