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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.ItemResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactGetItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactGetItemsResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

/** Retry behaviour of multi-entity snapshot reads, without a DynamoDB endpoint. */
class DynamoDbBasePersistenceSnapshotReadTest {

  private static final PolarisEntityId ID = new PolarisEntityId(7L, 10L);

  /** Answers {@code TransactGetItems} from a script; every other call is unsupported. */
  private static final class ScriptedClient implements DynamoDbClient {
    private final List<Object> script;
    private int calls;

    ScriptedClient(Object... script) {
      this.script = new ArrayList<>(List.of(script));
    }

    @Override
    public TransactGetItemsResponse transactGetItems(TransactGetItemsRequest request) {
      calls++;
      Object next = script.removeFirst();
      if (next instanceof RuntimeException e) {
        throw e;
      }
      return (TransactGetItemsResponse) next;
    }

    @Override
    public String serviceName() {
      return SERVICE_NAME;
    }

    @Override
    public void close() {}
  }

  private static TransactionCanceledException cancelled(String code) {
    return TransactionCanceledException.builder()
        .cancellationReasons(CancellationReason.builder().code(code).build())
        .build();
  }

  private static TransactGetItemsResponse versions(int entityVersion, int grantRecordsVersion) {
    return TransactGetItemsResponse.builder()
        .responses(
            ItemResponse.builder()
                .item(
                    Map.of(
                        DynamoDbLayout.A_ID, AttributeValue.fromN("10"),
                        DynamoDbLayout.A_CATALOG_ID, AttributeValue.fromN("7"),
                        DynamoDbLayout.A_ENTITY_VERSION,
                            AttributeValue.fromN(Integer.toString(entityVersion)),
                        DynamoDbLayout.A_GRANT_RECORDS_VERSION,
                            AttributeValue.fromN(Integer.toString(grantRecordsVersion))))
                .build())
        .build();
  }

  private static DynamoDbBasePersistence persistence(DynamoDbClient client) {
    return new DynamoDbBasePersistence(client, "table", "realm1");
  }

  private static PolarisCallContext callCtx(DynamoDbClient client) {
    return new PolarisCallContext(() -> "realm1", persistence(client));
  }

  @Test
  void conflictingSnapshotReadIsRetried() {
    ScriptedClient client = new ScriptedClient(cancelled("TransactionConflict"), versions(3, 2));

    assertThat(persistence(client).lookupEntityVersions(callCtx(client), List.of(ID)))
        .containsExactly(new PolarisChangeTrackingVersions(3, 2));
    assertThat(client.calls).isEqualTo(2);
  }

  @Test
  void persistentConflictBecomesRetryOnConcurrency() {
    TransactionCanceledException conflict = cancelled("TransactionConflict");
    ScriptedClient client = new ScriptedClient(conflict, conflict, conflict, conflict, conflict);

    assertThatThrownBy(() -> persistence(client).lookupEntityVersions(callCtx(client), List.of(ID)))
        .isInstanceOf(RetryOnConcurrencyException.class);
    assertThat(client.calls).isEqualTo(5);
  }

  @Test
  void otherCancellationsAreNotRetried() {
    TransactionCanceledException validation = cancelled("ValidationError");
    ScriptedClient client = new ScriptedClient(validation);

    assertThatThrownBy(() -> persistence(client).lookupEntityVersions(callCtx(client), List.of(ID)))
        .isSameAs(validation);
    assertThat(client.calls).isEqualTo(1);
  }
}
