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
package org.apache.polaris.persistence.nosql.impl.commits;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.commit.FairRetriesType;
import org.apache.polaris.persistence.nosql.api.commit.RetryConfig;
import org.apache.polaris.persistence.nosql.api.obj.CommitTestObj;
import org.apache.polaris.persistence.nosql.impl.commits.retry.RetryLoop;
import org.apache.polaris.persistence.nosql.testextension.BackendSpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@BackendSpec
public class TestCommitterImpl extends BaseTestCommitterImpl {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void localSynchronizationOverridesFairnessAndReleasesPermit(boolean synchronizing)
      throws Exception {
    var config =
        RetryConfig.BuildableRetryConfig.builder().fairRetries(FairRetriesType.SLEEPING).build();
    var params = PersistenceParams.BuildablePersistenceParams.builder().retryConfig(config).build();
    var persistence = mock(Persistence.class);
    when(persistence.params()).thenReturn(params);
    var clock = mock(MonotonicClock.class);
    when(persistence.monotonicClock()).thenReturn(clock);
    when(persistence.realmId()).thenReturn("fairness-policy-test");
    var committer =
        new CommitterImpl<>(persistence, "fairness-policy-test", CommitTestObj.class, String.class);
    if (synchronizing) {
      committer.synchronizingLocally();
    }
    var expected = synchronizing ? FairRetriesType.UNFAIR : FairRetriesType.SLEEPING;
    try (var factory = mockStatic(RetryLoop.class, CALLS_REAL_METHODS)) {
      var failure = new IllegalStateException("attempt failed");
      assertThatThrownBy(
              () ->
                  committer.commit(
                      (state, reference) -> {
                        throw failure;
                      }))
          .isSameAs(failure);
      assertThat(committer.commit((state, reference) -> state.noCommit("done"))).contains("done");
      factory.verify(() -> RetryLoop.newRetryLoop(config, clock, expected), times(2));
    }
  }

  @Test
  public void localConflictReleasesPermitBeforeBackoffAndRetries() throws Exception {
    var config =
        RetryConfig.BuildableRetryConfig.builder()
            .retries(1)
            .initialSleepLower(Duration.ofMillis(10))
            .initialSleepUpper(Duration.ofMillis(10))
            .maxSleep(Duration.ofMillis(10))
            .build();
    var params = PersistenceParams.BuildablePersistenceParams.builder().retryConfig(config).build();
    var persistence = mock(Persistence.class);
    var clock = mock(MonotonicClock.class);
    when(persistence.params()).thenReturn(params);
    when(persistence.monotonicClock()).thenReturn(clock);
    when(persistence.realmId()).thenReturn("retry-permit-test");
    var sync = ExclusiveCommitSynchronizer.forKey("retry-permit-test", "retry-permit-test");
    var attempts = new AtomicInteger();
    doAnswer(
            ignored -> {
              assertThat(attempts).hasValue(1);
              var acquired = sync.before(0);
              try {
                assertThat(acquired).isTrue();
              } finally {
                if (acquired) {
                  sync.after();
                }
              }
              return null;
            })
        .when(clock)
        .sleepMillis(10);
    var committer =
        new CommitterImpl<>(persistence, "retry-permit-test", CommitTestObj.class, String.class)
            .synchronizingLocally();

    assertThat(
            committer.commit(
                (state, reference) ->
                    attempts.incrementAndGet() == 1 ? Optional.empty() : state.noCommit("done")))
        .contains("done");

    assertThat(attempts).hasValue(2);
    verify(clock).sleepMillis(10);
    assertThat(sync.before(0)).isTrue();
    sync.after();
  }
}
