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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

class TransactionFailuresTest {

  private static TransactionCanceledException cancelled(String... codes) {
    CancellationReason[] reasons = new CancellationReason[codes.length];
    for (int i = 0; i < codes.length; i++) {
      reasons[i] = CancellationReason.builder().code(codes[i]).build();
    }
    return TransactionCanceledException.builder().cancellationReasons(reasons).build();
  }

  @Test
  void reportsTheActionWhoseConditionFailed() {
    TransactionCanceledException e =
        cancelled("None", TransactionFailures.CONDITIONAL_CHECK_FAILED);

    assertThat(TransactionFailures.conditionFailedAt(e, 0)).isFalse();
    assertThat(TransactionFailures.conditionFailedAt(e, 1)).isTrue();
  }

  @Test
  void otherCancellationCodesAreNotConditionFailures() {
    TransactionCanceledException e = cancelled("TransactionConflict", "None");

    assertThat(TransactionFailures.conditionFailedAt(e, 0)).isFalse();
    assertThat(TransactionFailures.conditionFailedAt(e, 1)).isFalse();
  }

  @Test
  void outOfRangeOrMissingReasonsAreNotConditionFailures() {
    assertThat(
            TransactionFailures.conditionFailedAt(
                cancelled(TransactionFailures.CONDITIONAL_CHECK_FAILED), 1))
        .isFalse();
    assertThat(
            TransactionFailures.conditionFailedAt(
                cancelled(TransactionFailures.CONDITIONAL_CHECK_FAILED), -1))
        .isFalse();
    assertThat(
            TransactionFailures.conditionFailedAt(
                TransactionCanceledException.builder().build(), 0))
        .isFalse();
  }
}
