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
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

/**
 * Reads the per-item cancellation reasons of a failed {@code TransactWriteItems}.
 *
 * <p>DynamoDB reports one reason per action, in request order, so the index of an action in the
 * request identifies which condition failed (for example, the entity item versus its name
 * sentinel).
 */
final class TransactionFailures {

  static final String CONDITIONAL_CHECK_FAILED = "ConditionalCheckFailed";

  private TransactionFailures() {}

  /** Whether the action at {@code index} was cancelled because its condition did not hold. */
  static boolean conditionFailedAt(TransactionCanceledException e, int index) {
    List<CancellationReason> reasons = e.hasCancellationReasons() ? e.cancellationReasons() : null;
    return reasons != null
        && index >= 0
        && index < reasons.size()
        && CONDITIONAL_CHECK_FAILED.equals(reasons.get(index).code());
  }
}
