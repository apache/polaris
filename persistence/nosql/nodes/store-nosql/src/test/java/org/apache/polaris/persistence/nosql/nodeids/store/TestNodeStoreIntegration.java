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
package org.apache.polaris.persistence.nosql.nodeids.store;

import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.nosql.nodeids.impl.Util.idgenSpecFromManagementState;

import jakarta.inject.Inject;
import java.util.ArrayList;
import org.apache.polaris.ids.api.IdGeneratorSpec;
import org.apache.polaris.ids.api.ImmutableIdGeneratorSpec;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.nodeids.api.NodeLease;
import org.apache.polaris.persistence.nosql.nodeids.api.NodeManagement;
import org.apache.polaris.persistence.nosql.nodeids.spi.ImmutableBuildableNodeManagementState;
import org.apache.polaris.persistence.nosql.nodeids.spi.NodeManagementState;
import org.apache.polaris.persistence.nosql.nodeids.spi.NodeStoreFactory;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.jboss.weld.junit5.EnableWeld;
import org.jboss.weld.junit5.WeldInitiator;
import org.jboss.weld.junit5.WeldSetup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@SuppressWarnings("CdiInjectionPointsInspection")
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
    var lease = nodeManagement.lease();
    soft.assertThat(lease).isNotNull();
    soft.assertThat(lease.nodeIdIfValid()).isNotEqualTo(-1);
  }

  @Test
  public void allocateAll() {
    var numNodeIds = nodeManagement.maxNumberOfNodes();
    var leases = new ArrayList<NodeLease>();
    for (int i = 0; i < numNodeIds; i++) {
      soft.assertThatCode(() -> leases.add(nodeManagement.lease()))
          .describedAs("n = %d", i)
          .doesNotThrowAnyException();
    }
    soft.assertThatIllegalStateException()
        .isThrownBy(nodeManagement::lease)
        .withMessage("Could not lease any node ID");

    soft.assertThat(leases).hasSize(numNodeIds);

    for (var lease : leases) {
      soft.assertThat(lease.nodeIdIfValid()).isNotEqualTo(-1);
      soft.assertThat(
              requireNonNull(lease.node())
                  .valid(requireNonNull(lease.node()).expirationTimestamp().toEpochMilli()))
          .isFalse();
    }

    clock.waitUntilTimeMillisAdvanced();

    // Renew all leases

    for (var lease : leases) {
      var nodeId = lease.nodeIdIfValid();
      var beforeRelease = nodeManagement.getNodeInfo(nodeId).orElseThrow();
      soft.assertThat(beforeRelease).isEqualTo(lease.node());
      var n = requireNonNull(lease.node());
      var beforeExpire = n.expirationTimestamp();
      var beforeRenew = n.renewLeaseTimestamp();
      var beforeLease = n.leaseTimestamp();

      lease.renew();
      var fetched = nodeManagement.getNodeInfo(nodeId).orElseThrow();
      soft.assertThat(fetched).isEqualTo(lease.node());

      n = requireNonNull(lease.node());
      soft.assertThat(n.expirationTimestamp()).isAfter(beforeExpire);
      soft.assertThat(n.renewLeaseTimestamp()).isAfter(beforeRenew);
      soft.assertThat(n.leaseTimestamp()).isEqualTo(beforeLease);

      soft.assertAll();
    }

    // Release all leases

    for (var lease : leases) {
      var nodeId = lease.nodeIdIfValid();
      var beforeRelease = nodeManagement.getNodeInfo(nodeId).orElseThrow();
      var n = requireNonNull(lease.node());
      soft.assertThat(beforeRelease).isEqualTo(n);
      var beforeExpire = n.expirationTimestamp();
      var beforeLease = n.leaseTimestamp();

      lease.release();
      soft.assertThat(lease.nodeIdIfValid()).isEqualTo(-1);

      var fetched = nodeManagement.getNodeInfo(nodeId).orElseThrow();
      soft.assertThat(fetched.expirationTimestamp())
          .isBeforeOrEqualTo(clock.currentInstant())
          .isNotEqualTo(beforeExpire);
      soft.assertThat(fetched.leaseTimestamp()).isEqualTo(beforeLease);

      soft.assertThat(fetched.valid(clock.currentTimeMillis())).isFalse();
      nodeManagement.getNodeInfo(nodeId);

      soft.assertAll();
    }

    leases.clear();

    // Repeat allocation of all nodes

    clock.waitUntilTimeMillisAdvanced();

    for (int i = 0; i < numNodeIds; i++) {
      soft.assertThatCode(() -> leases.add(nodeManagement.lease()))
          .describedAs("n = %d", i)
          .doesNotThrowAnyException();

      soft.assertAll();
    }
    soft.assertThatIllegalStateException()
        .isThrownBy(nodeManagement::lease)
        .withMessage("Could not lease any node ID");

    soft.assertThat(leases).hasSize(numNodeIds);
  }
}
