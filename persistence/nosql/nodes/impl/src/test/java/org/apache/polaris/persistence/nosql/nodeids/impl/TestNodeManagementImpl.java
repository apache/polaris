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

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import jakarta.annotation.Nonnull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.IdGeneratorSpec;
import org.apache.polaris.ids.api.ImmutableIdGeneratorSpec;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.ids.api.SnowflakeIdGenerator;
import org.apache.polaris.ids.impl.MonotonicClockImpl;
import org.apache.polaris.ids.mocks.MutableMonotonicClock;
import org.apache.polaris.nosql.async.AsyncExec;
import org.apache.polaris.nosql.async.java.JavaPoolAsyncExec;
import org.apache.polaris.persistence.nosql.nodeids.api.NodeLease;
import org.apache.polaris.persistence.nosql.nodeids.api.NodeManagementConfig;
import org.apache.polaris.persistence.nosql.nodeids.spi.ImmutableBuildableNodeManagementState;
import org.apache.polaris.persistence.nosql.nodeids.spi.NodeManagementState;
import org.apache.polaris.persistence.nosql.nodeids.spi.NodeStore;
import org.apache.polaris.persistence.nosql.nodeids.spi.NodeStoreFactory;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestNodeManagementImpl {
  @InjectSoftAssertions protected SoftAssertions soft;

  AsyncExec scheduler;
  MonotonicClock clock;

  @BeforeEach
  void beforeEach() {
    clock = MonotonicClockImpl.newDefaultInstance();
    scheduler = new JavaPoolAsyncExec();
  }

  @AfterEach
  void afterEach() throws Exception {
    ((AutoCloseable) scheduler).close();
    clock.close();
  }

  @Test
  public void incompatibleNodeManagementConfig() {
    var incompatibleExistingState =
        ImmutableBuildableNodeManagementState.builder()
            .idGeneratorSpec(
                ImmutableIdGeneratorSpec.builder()
                    .type("snowflake")
                    .params(Map.of("offset", "2024-11-21T12:44:51.134Z"))
                    .build())
            .build();

    var config =
        NodeManagementConfig.BuildableNodeManagementConfig.builder()
            .idGeneratorSpec(
                ImmutableIdGeneratorSpec.builder()
                    .type("snowflake")
                    .params(
                        Map.of(
                            "timestamp-bits",
                            "1",
                            "node-id-bits",
                            "2",
                            "sequence-bits",
                            "3",
                            "offset",
                            "2024-11-21T12:44:51.134Z"))
                    .build())
            .build();
    var incompatible = new AtomicBoolean(false);
    try (var mgmt =
        new NodeManagementImpl(
            config,
            clock,
            new NodeStoreFactory() {
              @Nonnull
              @Override
              public NodeStore createNodeStore(@Nonnull IdGenerator idGenerator) {
                return new MockNodeStore();
              }

              @Override
              public Optional<NodeManagementState> fetchManagementState() {
                return Optional.of(incompatibleExistingState);
              }

              @Override
              public boolean storeManagementState(@Nonnull NodeManagementState state) {
                throw new UnsupportedOperationException();
              }
            },
            scheduler) {
          @Override
          void warnOnIncompatibleIdGeneratorSpec(IdGeneratorSpec spec) {
            incompatible.set(true);
            super.warnOnIncompatibleIdGeneratorSpec(spec);
          }
        }) {
      mgmt.init();

      soft.assertThat(incompatible).isTrue();
      var node = mgmt.lease();
      var idGen = mgmt.buildIdGenerator(node);
      soft.assertThat(idGen)
          .isInstanceOf(SnowflakeIdGenerator.class)
          .asInstanceOf(type(SnowflakeIdGenerator.class))
          .extracting(
              SnowflakeIdGenerator::timestampBits,
              SnowflakeIdGenerator::nodeIdBits,
              SnowflakeIdGenerator::sequenceBits)
          .containsExactly(
              SnowflakeIdGenerator.DEFAULT_TIMESTAMP_BITS,
              SnowflakeIdGenerator.DEFAULT_NODE_ID_BITS,
              SnowflakeIdGenerator.DEFAULT_SEQUENCE_BITS);
    }
  }

  @Test
  public void simple() {
    var config =
        NodeManagementConfig.BuildableNodeManagementConfig.builder()
            .idGeneratorSpec(
                ImmutableIdGeneratorSpec.builder()
                    .type("snowflake")
                    .params(Map.of("offset", "2024-11-21T12:44:51.134Z"))
                    .build())
            .build();
    try (var mgmt = new NodeManagementImpl(config, clock, new MockNodeStoreFactory(), scheduler)) {
      mgmt.init();

      soft.assertThat(mgmt.maxNumberOfNodes()).isEqualTo(config.numNodes());
      var lease = mgmt.lease();
      soft.assertThat(lease).isNotNull();
      soft.assertThat(lease.nodeIdIfValid()).isNotEqualTo(-1);
    }
  }

  @Test
  public void allocateAll() {
    NodeManagementImpl m;
    var config =
        NodeManagementConfig.BuildableNodeManagementConfig.builder()
            .idGeneratorSpec(
                ImmutableIdGeneratorSpec.builder()
                    .type("snowflake")
                    .params(Map.of("offset", "2024-11-21T12:44:51.134Z"))
                    .build())
            .build();
    try (var mutableClock = new MutableMonotonicClock();
        var mgmt =
            new NodeManagementImpl(config, mutableClock, new MockNodeStoreFactory(), scheduler)) {
      mgmt.init();

      var numNodeIds = 1 << SnowflakeIdGenerator.DEFAULT_NODE_ID_BITS;
      var leases = new ArrayList<NodeLease>();
      for (int i = 0; i < numNodeIds; i++) {
        soft.assertThatCode(() -> leases.add(mgmt.lease()))
            .describedAs("n = %d", i)
            .doesNotThrowAnyException();
      }
      soft.assertThatIllegalStateException()
          .isThrownBy(mgmt::lease)
          .withMessage("Could not lease any node ID");

      soft.assertThat(leases).hasSize(numNodeIds);

      for (var lease : leases) {
        soft.assertThat(lease.nodeIdIfValid()).isNotEqualTo(-1);
        soft.assertThat(
                requireNonNull(lease.node())
                    .valid(lease.node().expirationTimestamp().toEpochMilli()))
            .isFalse();
      }

      // Release all leases

      for (var node : leases) {
        node.release();
        soft.assertThat(node.nodeIdIfValid()).isEqualTo(-1);
      }

      leases.clear();

      // Lease fails, because the clock is not 'after' the 'expirationTimestamp'

      soft.assertThatIllegalStateException()
          .isThrownBy(mgmt::lease)
          .withMessage("Could not lease any node ID");

      // Advance clock

      mutableClock.advanceBoth(Duration.ofSeconds(1));

      // Repeat allocation of all nodes

      for (int i = 0; i < numNodeIds; i++) {
        soft.assertThatCode(() -> leases.add(mgmt.lease()))
            .describedAs("n = %d", i)
            .doesNotThrowAnyException();
      }
      soft.assertThatIllegalStateException()
          .isThrownBy(mgmt::lease)
          .withMessage("Could not lease any node ID");

      soft.assertThat(leases).hasSize(numNodeIds);

      m = mgmt;
    }

    soft.assertThatIllegalStateException()
        .isThrownBy(m::lease)
        .withMessage("NodeManagement instance has been closed");
  }
}
