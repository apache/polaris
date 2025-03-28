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
package org.apache.polaris.nodes.impl;

import static org.assertj.core.api.InstanceOfAssertFactories.type;

import jakarta.annotation.Nonnull;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.IdGeneratorSpec;
import org.apache.polaris.ids.api.ImmutableIdGeneratorSpec;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.ids.api.SnowflakeIdGenerator;
import org.apache.polaris.nodes.api.Node;
import org.apache.polaris.nodes.api.NodeManagementConfig;
import org.apache.polaris.nodes.spi.ImmutableBuildableNodeManagementState;
import org.apache.polaris.nodes.spi.NodeManagementState;
import org.apache.polaris.nodes.spi.NodeStore;
import org.apache.polaris.nodes.spi.NodeStoreFactory;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.jboss.weld.junit5.EnableWeld;
import org.jboss.weld.junit5.WeldInitiator;
import org.jboss.weld.junit5.WeldSetup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
@EnableWeld
public class TestNodeManagementImpl {
  @InjectSoftAssertions protected SoftAssertions soft;
  @WeldSetup WeldInitiator weld;

  {
    try {
      weld = WeldInitiator.of(Class.forName("org.apache.polaris.ids.impl.MonotonicClockImpl"));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Inject MonotonicClock clock;

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
            }) {
          @Override
          void warnOnIncompatibleIdGeneratorSpec(IdGeneratorSpec spec) {
            incompatible.set(true);
            super.warnOnIncompatibleIdGeneratorSpec(spec);
          }
        }) {
      soft.assertThat(incompatible).isTrue();
      var node = mgmt.lease();
      var idGen = mgmt.buildIdGenerator(node, clock::currentTimeMillis);
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
    try (var mgmt = new NodeManagementImpl(config, clock, new MockNodeStoreFactory())) {
      var node = mgmt.lease();
      soft.assertThat(node).isNotNull();
      soft.assertThat(node.valid(clock.currentTimeMillis())).isTrue();
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
    var nowHolder = new AtomicReference<>(Instant.now());
    try (var mgmt = new NodeManagementImpl(config, nowHolder::get, new MockNodeStoreFactory())) {
      var numNodeIds = 1 << SnowflakeIdGenerator.DEFAULT_NODE_ID_BITS;
      var nodes = new ArrayList<Node>();
      for (int i = 0; i < numNodeIds; i++) {
        soft.assertThatCode(() -> nodes.add(mgmt.lease()))
            .describedAs("n = %d", i)
            .doesNotThrowAnyException();
      }
      soft.assertThatIllegalStateException()
          .isThrownBy(mgmt::lease)
          .withMessage("Could not lease any node ID");

      soft.assertThat(nodes).hasSize(numNodeIds);

      for (var node : nodes) {
        soft.assertThat(node.valid(nowHolder.get().toEpochMilli())).isTrue();
        soft.assertThat(node.valid(node.expirationTimestamp().toEpochMilli())).isFalse();
      }

      // Release all leases

      for (var node : nodes) {
        mgmt.releaseLease(node);
        soft.assertThat(node.valid(nowHolder.get().toEpochMilli())).isFalse();
      }

      nodes.clear();

      // Lease fails, because the clock is not 'after' the 'expirationTimestamp'

      soft.assertThatIllegalStateException()
          .isThrownBy(mgmt::lease)
          .withMessage("Could not lease any node ID");

      // Advance clock

      nowHolder.set(nowHolder.get().plus(Duration.ofSeconds(1)));

      // Repeat allocation of all nodes

      for (int i = 0; i < numNodeIds; i++) {
        soft.assertThatCode(() -> nodes.add(mgmt.lease()))
            .describedAs("n = %d", i)
            .doesNotThrowAnyException();
      }
      soft.assertThatIllegalStateException()
          .isThrownBy(mgmt::lease)
          .withMessage("Could not lease any node ID");

      soft.assertThat(nodes).hasSize(numNodeIds);

      m = mgmt;
    }

    soft.assertThatIllegalStateException()
        .isThrownBy(m::lease)
        .withMessage("NodeManagement instance has been closed");
  }
}
