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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

class DynamoDbSchemaIT extends AbstractDynamoDbTest {

  @Test
  void createIfAbsentProvisionsTheBaseTable() {
    String table = uniqueTableName();
    DynamoDbSchema.createIfAbsent(client, table);
    try {
      TableDescription description = client.describeTable(b -> b.tableName(table)).table();

      assertThat(description.keySchema())
          .extracting(KeySchemaElement::attributeName, KeySchemaElement::keyType)
          .containsExactly(
              tuple(DynamoDbLayout.PK, KeyType.HASH), tuple(DynamoDbLayout.SK, KeyType.RANGE));

      // Listing reads child records on the base table, so there is no secondary index.
      assertThat(description.globalSecondaryIndexes()).isEmpty();
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
}
