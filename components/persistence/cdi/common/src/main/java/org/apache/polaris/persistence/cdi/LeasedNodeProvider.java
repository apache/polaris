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
package org.apache.polaris.persistence.cdi;

import static java.util.Objects.requireNonNull;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import java.time.Duration;
import org.apache.polaris.async.AsyncExec;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.nodes.api.Node;
import org.apache.polaris.nodes.api.NodeManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application-scoped leased {@link Node} providers, scheduling lease renewal.
 *
 * <p>Requires an {@link ApplicationScoped} instance of {@link AsyncExec} provided by the
 * application.
 */
@ApplicationScoped
class LeasedNodeProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeasedNodeProvider.class);

  @Inject AsyncExec scheduler;
  @Inject MonotonicClock monotonicClock;
  @Inject NodeManagement nodeManagement;

  private volatile boolean nodeLeaseActive;

  @Produces
  @ApplicationScoped
  Node leasedNode() {
    var leased = nodeManagement.lease();
    nodeLeaseActive = true;
    scheduler.schedule(
        () -> renewNodeLease(leased),
        Duration.ofMillis(
            requireNonNull(leased.renewLeaseTimestamp()).toEpochMilli()
                - monotonicClock.currentTimeMillis()));
    LOGGER.info(
        "Lease for node#{} acquired, valid until {} (renewal scheduled).",
        leased.id(),
        leased.expirationTimestamp());
    return leased;
  }

  private void renewNodeLease(Node leased) {
    if (leased.valid(monotonicClock.currentTimeMillis())) {
      LOGGER.info("Renewing lease for node#{}...", leased.id());
      // The call to 'renewLease()` can throw an ISE if this call races w/ NodeManagement.close().
      // In that case, it's not just an annoyance, nothing more.
      nodeManagement.renewLease(leased);
      LOGGER.info("Lease for node#{} renewed until {}.", leased.id(), leased.expirationTimestamp());
      scheduler.schedule(
          () -> renewNodeLease(leased),
          Duration.ofMillis(
              requireNonNull(leased.renewLeaseTimestamp()).toEpochMilli()
                  - monotonicClock.currentTimeMillis()));
    } else if (nodeLeaseActive) {
      LOGGER.warn(
          "Not renewing node lease for node#{}, because the lease is currently no longer valid. The node lease should have been valid, but was not. This may happen if the node massively overloaded.",
          leased.id());
    }
  }
}
