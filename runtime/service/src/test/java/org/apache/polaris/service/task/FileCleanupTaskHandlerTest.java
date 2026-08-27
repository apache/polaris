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
package org.apache.polaris.service.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/** Unit tests for the shared {@link FileCleanupTaskHandler#awaitCompletion} deadline logic. */
class FileCleanupTaskHandlerTest {

  private static FileCleanupTaskHandler handler(Duration timeout) {
    // Only the timeout is exercised here; the file IO supplier and executor are unused.
    return new BatchFileCleanupTaskHandler(null, null, timeout);
  }

  @Test
  void awaitCompletionFailsWhenDeletionStalls() {
    CompletableFuture<Void> neverCompletes = new CompletableFuture<>();
    assertThatThrownBy(
            () ->
                handler(Duration.ofMillis(50)).awaitCompletion(neverCompletes, "stalled deletion"))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Timed out")
        .hasMessageContaining("stalled deletion");
    assertThat(neverCompletes.isCancelled()).isTrue();
  }

  @Test
  void awaitCompletionReturnsWhenDeletionCompletes() {
    assertThatCode(
            () ->
                handler(Duration.ofSeconds(30))
                    .awaitCompletion(CompletableFuture.completedFuture(null), "deletion"))
        .doesNotThrowAnyException();
  }

  @Test
  void awaitCompletionWithNonPositiveTimeoutDoesNotApplyDeadline() {
    // A zero/negative timeout disables the deadline; a completed future must still return.
    assertThatCode(
            () ->
                handler(Duration.ZERO)
                    .awaitCompletion(CompletableFuture.completedFuture(null), "deletion"))
        .doesNotThrowAnyException();
  }
}
