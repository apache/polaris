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
package org.apache.polaris.nodes.store;

import static org.apache.polaris.nodes.impl.Util.idgenSpecFromManagementState;

import jakarta.inject.Inject;
import java.util.ArrayList;
import org.apache.polaris.ids.api.IdGeneratorSpec;
import org.apache.polaris.ids.api.ImmutableIdGeneratorSpec;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.nodes.api.Node;
import org.apache.polaris.nodes.api.NodeManagement;
import org.apache.polaris.nodes.spi.ImmutableBuildableNodeManagementState;
import org.apache.polaris.nodes.spi.NodeManagementState;
import org.apache.polaris.nodes.spi.NodeStoreFactory;
import org.assertj.core.api.InstanceOfAssertFactories;
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
public class TestNodeStoreIntegration {
  @InjectSoftAssertions protected SoftAssertions soft;
  @WeldSetup WeldInitiator weld = WeldInitiator.performDefaultDiscovery();

  @Inject NodeStoreFactory nodeStoreFactory;
  @Inject NodeManagement nodeManagement;
  @Inject MonotonicClock clock;

  @Test
  public void managementState() {
    soft.assertThat(nodeStoreFactory.fetchManagementState()).isEmpty();

    var buildableIdgenSpec = IdGeneratorSpec.BuildableIdGeneratorSpec.builder().build();
    var idgenSpec = ImmutableIdGeneratorSpec.builder().from(buildableIdgenSpec).build();
    var nodeManagementSpec =
        ImmutableBuildableNodeManagementState.builder().idGeneratorSpec(idgenSpec).build();

    soft.assertThat(nodeStoreFactory.storeManagementState(nodeManagementSpec)).isTrue();
    var fetched = nodeStoreFactory.fetchManagementState();
    soft.assertThat(fetched).isPresent();
    soft.assertThat(fetched)
        .get()
        .extracting(
            NodeManagementState::idGeneratorSpec,
            InstanceOfAssertFactories.optional(IdGeneratorSpec.class))
        .get()
        .isEqualTo(idgenSpec);
    var specFromFetched = idgenSpecFromManagementState(fetched);
    soft.assertThat(specFromFetched).isEqualTo(idgenSpec);
    soft.assertThat(nodeStoreFactory.storeManagementState(nodeManagementSpec)).isFalse();
  }

  @Test
  public void simple() {
    var node = nodeManagement.lease();
    soft.assertThat(node).isNotNull();
    soft.assertThat(node.valid(clock.currentTimeMillis())).isTrue();
  }

  @Test
  public void allocateAll() {
    var numNodeIds = nodeManagement.maxNumberOfNodes();
    var nodes = new ArrayList<Node>();
    for (int i = 0; i < numNodeIds; i++) {
      soft.assertThatCode(() -> nodes.add(nodeManagement.lease()))
          .describedAs("n = %d", i)
          .doesNotThrowAnyException();
    }
    soft.assertThatIllegalStateException()
        .isThrownBy(nodeManagement::lease)
        .withMessage("Could not lease any node ID");

    soft.assertThat(nodes).hasSize(numNodeIds);

    for (var node : nodes) {
      soft.assertThat(node.valid(clock.currentTimeMillis())).isTrue();
      soft.assertThat(node.valid(node.expirationTimestamp().toEpochMilli())).isFalse();
    }

    clock.waitUntilTimeMillisAdvanced();

    // Renew all leases

    for (var node : nodes) {
      var beforeRelease = nodeManagement.getNodeInfo(node.id()).orElseThrow();
      soft.assertThat(beforeRelease).isSameAs(node);
      var beforeExpire = node.expirationTimestamp();
      var beforeRenew = node.renewLeaseTimestamp();
      var beforeLease = node.leaseTimestamp();

      nodeManagement.renewLease(node);
      var fetched = nodeManagement.getNodeInfo(node.id()).orElseThrow();
      soft.assertThat(fetched).isSameAs(node);

      soft.assertThat(node.expirationTimestamp()).isAfter(beforeExpire);
      soft.assertThat(node.renewLeaseTimestamp()).isAfter(beforeRenew);
      soft.assertThat(node.leaseTimestamp()).isEqualTo(beforeLease);

      soft.assertAll();
    }

    // Release all leases

    for (var node : nodes) {
      var beforeRelease = nodeManagement.getNodeInfo(node.id()).orElseThrow();
      soft.assertThat(beforeRelease).isSameAs(node);
      var beforeExpire = node.expirationTimestamp();
      var beforeLease = node.leaseTimestamp();

      nodeManagement.releaseLease(node);
      soft.assertThat(node.valid(clock.currentTimeMillis())).isFalse();
      soft.assertThat(node.expirationTimestamp())
          .isBeforeOrEqualTo(clock.currentInstant())
          .isNotEqualTo(beforeExpire);
      soft.assertThat(node.renewLeaseTimestamp()).isNull();
      soft.assertThat(node.leaseTimestamp()).isEqualTo(beforeLease);

      var fetched = nodeManagement.getNodeInfo(node.id()).orElseThrow();
      soft.assertThat(fetched).isNotSameAs(node);

      soft.assertThat(fetched)
          .extracting(Node::expirationTimestamp, Node::leaseTimestamp, Node::renewLeaseTimestamp)
          .containsExactly(node.expirationTimestamp(), node.leaseTimestamp(), null);

      soft.assertThat(fetched.expirationTimestamp()).isEqualTo(node.expirationTimestamp());
      soft.assertThat(fetched.valid(clock.currentTimeMillis())).isFalse();
      nodeManagement.getNodeInfo(node.id());

      soft.assertAll();
    }

    nodes.clear();

    // Repeat allocation of all nodes

    clock.waitUntilTimeMillisAdvanced();

    for (int i = 0; i < numNodeIds; i++) {
      soft.assertThatCode(() -> nodes.add(nodeManagement.lease()))
          .describedAs("n = %d", i)
          .doesNotThrowAnyException();

      soft.assertAll();
    }
    soft.assertThatIllegalStateException()
        .isThrownBy(nodeManagement::lease)
        .withMessage("Could not lease any node ID");

    soft.assertThat(nodes).hasSize(numNodeIds);
  }
}
