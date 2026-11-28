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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

class DynamoDbBasePersistenceConditionalDeleteTest {
  private static final PolarisBaseEntity ENTITY =
      new PolarisBaseEntity.Builder()
          .id(10)
          .catalogId(7)
          .parentId(9)
          .name("table")
          .typeCode(PolarisEntityType.TABLE_LIKE.getCode())
          .subTypeCode(PolarisEntitySubType.ICEBERG_TABLE.getCode())
          .entityVersion(3)
          .grantRecordsVersion(2)
          .build();

  private static final class DeleteClient implements DynamoDbClient {
    private TransactWriteItemsRequest request;
    private int calls;
    private boolean conflict;

    @Override
    public TransactWriteItemsResponse transactWriteItems(TransactWriteItemsRequest value) {
      request = value;
      calls++;
      if (conflict) {
        throw TransactionCanceledException.builder()
            .cancellationReasons(
                CancellationReason.builder().code("ConditionalCheckFailed").build())
            .build();
      }
      return TransactWriteItemsResponse.builder().build();
    }

    @Override
    public String serviceName() {
      return SERVICE_NAME;
    }

    @Override
    public void close() {}
  }

  @Test
  void deletesRegistryRecordsWithExpectedVersions() {
    DeleteClient client = new DeleteClient();
    DynamoDbBasePersistence persistence = new DynamoDbBasePersistence(client, "table", "realm");
    persistence.deleteEntityIfNotChanged(
        new PolarisCallContext(() -> "realm", persistence), ENTITY);

    assertThat(client.request.transactItems()).hasSize(3).allMatch(item -> item.delete() != null);
    assertThat(client.request.transactItems().getFirst().delete().conditionExpression())
        .isNotBlank();
    assertThat(client.request.transactItems().getFirst().delete().expressionAttributeValues())
        .containsAllEntriesOf(
            Map.of(":ev", AttributeValue.fromN("3"), ":grv", AttributeValue.fromN("2")));
  }

  @Test
  void concurrentUpdateDoesNotRetryWithNewerState() {
    DeleteClient client = new DeleteClient();
    client.conflict = true;
    DynamoDbBasePersistence persistence = new DynamoDbBasePersistence(client, "table", "realm");
    assertThatThrownBy(() ->
            persistence.deleteEntityIfNotChanged(
                new PolarisCallContext(() -> "realm", persistence), ENTITY))
        .isInstanceOf(RetryOnConcurrencyException.class);
    assertThat(client.calls).isEqualTo(1);
  }
}
