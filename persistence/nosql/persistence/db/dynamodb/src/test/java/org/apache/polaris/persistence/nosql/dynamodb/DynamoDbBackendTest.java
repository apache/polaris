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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import org.apache.polaris.persistence.nosql.api.exceptions.UnknownOperationResultException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

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
}
