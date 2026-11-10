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
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.commit.RetryConfig;
import org.apache.polaris.persistence.nosql.api.obj.CommitTestObj;
import org.apache.polaris.persistence.nosql.testextension.BackendSpec;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@BackendSpec
public class TestCommitterImpl extends BaseTestCommitterImpl {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void localSynchronizationOverridesFairnessAndReleasesPermit(boolean synchronizing)
      throws Exception {
    var config = spy(RetryConfig.BuildableRetryConfig.builder().build());
    var params = PersistenceParams.BuildablePersistenceParams.builder().retryConfig(config).build();
    var persistence = mock(Persistence.class);
    when(persistence.params()).thenReturn(params);
    when(persistence.monotonicClock()).thenReturn(mock(MonotonicClock.class));
    when(persistence.realmId()).thenReturn("fairness-policy-test");
    var committer =
        new CommitterImpl<>(persistence, "fairness-policy-test", CommitTestObj.class, String.class);
    if (synchronizing) {
      committer.synchronizingLocally();
    }
    clearInvocations(config);
    var failure = new IllegalStateException("attempt failed");
    assertThatThrownBy(
            () ->
                committer.commit(
                    (state, reference) -> {
                      throw failure;
                    }))
        .isSameAs(failure);
    assertThat(committer.commit((state, reference) -> state.noCommit("done"))).contains("done");
    verify(config, times(synchronizing ? 0 : 2)).fairRetries();
  }
}
