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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Thin wrapper around the {@link DynamoDbClient} for one Polaris deployment: holds the client and
 * table name, and owns schema setup. Mirrors {@code DatasourceOperations} on the JDBC side; here
 * {@link #setupSchema()} replaces the {@code schema-vN.sql} DDL with a single {@code CreateTable}
 * (base table + two GSIs).
 */
public class DynamoDbOperations {

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDbOperations.class);

  private final DynamoDbClient client;
  private final String tableName;

  public DynamoDbOperations(DynamoDbClient client, String tableName) {
    this.client = client;
    this.tableName = tableName;
  }

  public DynamoDbClient client() {
    return client;
  }

  public String tableName() {
    return tableName;
  }

  /**
   * Create the single Polaris table (PK/SK) plus the two sparse GSIs (by-name, by-grantee),
   * on-demand billing. Idempotent: an already-existing table is treated as success.
   *
   * <p>GSIs use {@code ProjectionType.ALL}; a production build could narrow this to save storage.
   */
  public void setupSchema() {
    try {
      client.createTable(
          CreateTableRequest.builder()
              .tableName(tableName)
              .billingMode(BillingMode.PAY_PER_REQUEST)
              .attributeDefinitions(
                  attr(DynamoDbConstants.PK),
                  attr(DynamoDbConstants.SK),
                  attr(DynamoDbConstants.GSI1_PK),
                  attr(DynamoDbConstants.GSI1_SK),
                  attr(DynamoDbConstants.GSI2_PK),
                  attr(DynamoDbConstants.GSI2_SK))
              .keySchema(
                  key(DynamoDbConstants.PK, KeyType.HASH), key(DynamoDbConstants.SK, KeyType.RANGE))
              .globalSecondaryIndexes(
                  gsi(DynamoDbConstants.GSI1, DynamoDbConstants.GSI1_PK, DynamoDbConstants.GSI1_SK),
                  gsi(DynamoDbConstants.GSI2, DynamoDbConstants.GSI2_PK, DynamoDbConstants.GSI2_SK))
              .build());
      LOGGER.info("Created DynamoDB table {} for Polaris persistence", tableName);
    } catch (ResourceInUseException alreadyExists) {
      LOGGER.debug("DynamoDB table {} already exists; skipping create", tableName);
    }
  }

  private static AttributeDefinition attr(String name) {
    return AttributeDefinition.builder()
        .attributeName(name)
        .attributeType(ScalarAttributeType.S)
        .build();
  }

  private static KeySchemaElement key(String name, KeyType type) {
    return KeySchemaElement.builder().attributeName(name).keyType(type).build();
  }

  private static GlobalSecondaryIndex gsi(String indexName, String pk, String sk) {
    return GlobalSecondaryIndex.builder()
        .indexName(indexName)
        .keySchema(key(pk, KeyType.HASH), key(sk, KeyType.RANGE))
        .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
        .build();
  }
}
