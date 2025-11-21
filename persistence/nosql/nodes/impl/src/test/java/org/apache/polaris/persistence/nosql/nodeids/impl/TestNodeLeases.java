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
package org.apache.polaris.persistence.nosql.nodeids.impl;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.nosql.nodeids.impl.NodeManagementImpl.RESCHEDULE_AFTER_FAILURE;
import static org.assertj.core.api.InstanceOfAssertFactories.BOOLEAN;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.polaris.ids.api.IdGeneratorSpec;
import org.apache.polaris.ids.mocks.MutableMonotonicClock;
import org.apache.polaris.nosql.async.MockAsyncExec;
import org.apache.polaris.persistence.nosql.nodeids.api.NodeManagementConfig;
import org.apache.polaris.persistence.nosql.nodeids.spi.NodeState;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestNodeLeases {
  @InjectSoftAssertions protected SoftAssertions soft;

  MockAsyncExec scheduler;
  MutableMonotonicClock clock;

  @BeforeEach
  void beforeEach() {
    clock = new MutableMonotonicClock();
    scheduler = new MockAsyncExec(clock);
  }

  @Test
  public void leaseAgainAfterStalledExecutor() {
    var config =
        NodeManagementConfig.BuildableNodeManagementConfig.builder()
            .idGeneratorSpec(IdGeneratorSpec.BuildableIdGeneratorSpec.builder().build())
            .build();
    var renewInterval = config.leaseDuration().minus(config.renewalPeriod());
    try (var mgmt = new NodeManagementImpl(config, clock, new MockNodeStoreFactory(), scheduler)) {
      mgmt.init();

      soft.assertThat(scheduler.tasks()).isEmpty();

      var lease = mgmt.lease();
      var node = requireNonNull(lease.node());
      soft.assertThat(lease.nodeIdIfValid()).isNotEqualTo(-1);

      soft.assertThat(scheduler.tasks())
          .hasSize(1)
          .first()
          .extracting(MockAsyncExec.Task::nextExecution)
          .isEqualTo(clock.currentInstant().plus(renewInterval));
      soft.assertThat(lease.nodeIdIfValid()).isNotEqualTo(-1);

      soft.assertThat(scheduler.nextReady()).isEmpty();

      clock.advanceBoth(renewInterval);
      soft.assertThat(scheduler.nextReady())
          .map(MockAsyncExec.Task::call)
          .get()
          .extracting(MockAsyncExec.CallResult::called, BOOLEAN)
          .isTrue();

      soft.assertThat(
              scheduler.readyCount(clock.currentInstant().plus(renewInterval).minus(1, MILLIS)))
          .isEqualTo(0L);
      soft.assertThat(scheduler.readyCount(clock.currentInstant().plus(renewInterval)))
          .isEqualTo(1L);

      soft.assertThat(requireNonNull(lease.node()).id()).isEqualTo(node.id());
      soft.assertThat(requireNonNull(lease.node()).expirationTimestamp())
          .isEqualTo(node.expirationTimestamp().plus(renewInterval));

      // Advance the clock _after/at_ expiration so it the node-lease has to re-lease

      clock.advanceBoth(config.leaseDuration());
      soft.assertThat(lease.nodeIdIfValid()).isEqualTo(-1);

      soft.assertThat(scheduler.nextReady())
          .map(MockAsyncExec.Task::call)
          .get()
          .extracting(MockAsyncExec.CallResult::called, BOOLEAN)
          .isTrue();

      soft.assertThat(lease.nodeIdIfValid()).isNotEqualTo(-1);
      soft.assertThat(requireNonNull(lease.node()).id()).isNotEqualTo(node.id());
      soft.assertThat(requireNonNull(lease.node()).expirationTimestamp())
          .isEqualTo(clock.currentInstant().plus(config.leaseDuration()));
    }
    soft.assertThat(scheduler.tasks()).isEmpty();
  }

  @Test
  public void rescheduleWorksAfterPersistFailures() {
    var config =
        NodeManagementConfig.BuildableNodeManagementConfig.builder()
            .idGeneratorSpec(IdGeneratorSpec.BuildableIdGeneratorSpec.builder().build())
            .build();
    var renewInterval = config.leaseDuration().minus(config.renewalPeriod());
    var persistFailure = new AtomicBoolean(false);
    var mockStore =
        new MockNodeStore() {
          @Nullable
          @Override
          public NodeState persist(
              int nodeId, Optional<NodeState> expectedNodeState, @Nonnull NodeState newState) {
            if (persistFailure.compareAndSet(true, false)) {
              throw new RuntimeException("forced persist failure");
            }
            return super.persist(nodeId, expectedNodeState, newState);
          }
        };
    try (var mgmt =
        new NodeManagementImpl(config, clock, new MockNodeStoreFactory(mockStore), scheduler)) {
      mgmt.init();

      soft.assertThat(scheduler.tasks()).isEmpty();

      var lease = mgmt.lease();
      var node = requireNonNull(lease.node());

      soft.assertThat(scheduler.tasks())
          .hasSize(1)
          .first()
          .extracting(MockAsyncExec.Task::nextExecution)
          .isEqualTo(clock.currentInstant().plus(renewInterval));

      soft.assertThat(scheduler.nextReady()).isEmpty();

      clock.advanceBoth(renewInterval);

      // Force a failure during renewal
      persistFailure.set(true);

      soft.assertThat(scheduler.nextReady())
          .map(MockAsyncExec.Task::call)
          .get()
          .extracting(MockAsyncExec.CallResult::called, MockAsyncExec.CallResult::failure)
          .containsExactly(true, null);

      soft.assertThat(lease.node()).isEqualTo(node);

      soft.assertThat(
              scheduler.readyCount(
                  clock.currentInstant().plus(RESCHEDULE_AFTER_FAILURE).minus(1, MILLIS)))
          .isEqualTo(0L);
      soft.assertThat(scheduler.readyCount(clock.currentInstant().plus(RESCHEDULE_AFTER_FAILURE)))
          .isEqualTo(1L);

      clock.advanceBoth(RESCHEDULE_AFTER_FAILURE);

      // renewal working

      soft.assertThat(scheduler.nextReady())
          .map(MockAsyncExec.Task::call)
          .get()
          .extracting(MockAsyncExec.CallResult::called, MockAsyncExec.CallResult::failure)
          .containsExactly(true, null);

      soft.assertThat(requireNonNull(lease.node()).id()).isEqualTo(node.id());
      soft.assertThat(requireNonNull(lease.node()).expirationTimestamp())
          .isEqualTo(node.expirationTimestamp().plus(RESCHEDULE_AFTER_FAILURE).plus(renewInterval));

      node = lease.node();

      // simulate a persistence failure during re-lease operation

      clock.advanceBoth(config.leaseDuration());
      persistFailure.set(true);
      soft.assertThat(scheduler.nextReady())
          .map(MockAsyncExec.Task::call)
          .get()
          .extracting(MockAsyncExec.CallResult::called, MockAsyncExec.CallResult::failure)
          .containsExactly(true, null);
      soft.assertThat(lease.nodeIdIfValid()).isEqualTo(-1);
      soft.assertThat(lease.node()).isEqualTo(node);

      clock.advanceBoth(RESCHEDULE_AFTER_FAILURE);

      soft.assertThat(scheduler.nextReady())
          .map(MockAsyncExec.Task::call)
          .get()
          .extracting(MockAsyncExec.CallResult::called, MockAsyncExec.CallResult::failure)
          .containsExactly(true, null);

      soft.assertThat(lease.nodeIdIfValid()).isNotEqualTo(-1);
      soft.assertThat(requireNonNull(lease.node()).expirationTimestamp())
          .isEqualTo(clock.currentInstant().plus(config.leaseDuration()));

      soft.assertThat(scheduler.nextReady()).isEmpty();

      lease.release();

      soft.assertThat(scheduler.tasks()).isEmpty();
    }
    soft.assertThat(scheduler.tasks()).isEmpty();
  }
}
