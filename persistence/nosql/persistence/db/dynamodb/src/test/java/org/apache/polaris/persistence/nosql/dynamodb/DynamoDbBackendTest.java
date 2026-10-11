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
package org.apache.polaris.persistence.nosql.dynamodb;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.persistence.nosql.api.exceptions.UnknownOperationResultException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

class DynamoDbBackendTest {

  @Test
  void conditionalCheckFailureAfterRetryHasUnknownOutcome() {
    var exception = ConditionalCheckFailedException.builder().numAttempts(2).build();

    var thrown =
        catchThrowable(() -> DynamoDbBackend.throwIfAmbiguousConditionalCheckFailure(exception));

    assertThat(thrown).isInstanceOf(UnknownOperationResultException.class);
    assertThat(thrown.getCause()).isSameAs(exception);
  }

  @Test
  void singleAttemptConditionalCheckFailureHasKnownOutcome() {
    var exception = ConditionalCheckFailedException.builder().numAttempts(1).build();

    assertThatCode(() -> DynamoDbBackend.throwIfAmbiguousConditionalCheckFailure(exception))
        .doesNotThrowAnyException();
  }

  @Test
  void batchWriteItemWithRetryResendsUnprocessedItems() {
    var client = mock(DynamoDbClient.class);
    var backend =
        spy(new DynamoDbBackend(new DynamoDbBackendConfig(false, client, Optional.empty())));
    doNothing().when(backend).sleepBeforeBatchWriteRetry(anyInt());

    var requestItems = singletonMap("objs", List.of(sampleWriteRequest("k1")));
    var unprocessed = singletonMap("objs", List.of(sampleWriteRequest("k1")));

    when(client.batchWriteItem(any(BatchWriteItemRequest.class)))
        .thenReturn(BatchWriteItemResponse.builder().unprocessedItems(unprocessed).build())
        .thenReturn(BatchWriteItemResponse.builder().unprocessedItems(Map.of()).build());

    backend.batchWriteItemWithRetry(requestItems);

    verify(client, times(2)).batchWriteItem(any(BatchWriteItemRequest.class));
    verify(backend).sleepBeforeBatchWriteRetry(0);
  }

  @Test
  void batchWriteItemWithRetryThrowsAfterMaxAttempts() {
    var client = mock(DynamoDbClient.class);
    var backend =
        spy(new DynamoDbBackend(new DynamoDbBackendConfig(false, client, Optional.empty())));
    doNothing().when(backend).sleepBeforeBatchWriteRetry(anyInt());

    var requestItems = singletonMap("objs", List.of(sampleWriteRequest("k1")));
    var unprocessed = singletonMap("objs", List.of(sampleWriteRequest("k1")));
    when(client.batchWriteItem(any(BatchWriteItemRequest.class)))
        .thenReturn(BatchWriteItemResponse.builder().unprocessedItems(unprocessed).build());

    assertThatThrownBy(() -> backend.batchWriteItemWithRetry(requestItems))
        .isInstanceOf(UnknownOperationResultException.class)
        .hasMessageContaining("unprocessed items");

    verify(client, times(DynamoDbBackend.MAX_BATCH_WRITE_ATTEMPTS))
        .batchWriteItem(any(BatchWriteItemRequest.class));
    verify(backend, times(DynamoDbBackend.MAX_BATCH_WRITE_ATTEMPTS - 1))
        .sleepBeforeBatchWriteRetry(anyInt());
  }

  private static WriteRequest sampleWriteRequest(String key) {
    return WriteRequest.builder()
        .putRequest(
            PutRequest.builder().item(Map.of("k", AttributeValue.builder().s(key).build())).build())
        .build();
  }
}
