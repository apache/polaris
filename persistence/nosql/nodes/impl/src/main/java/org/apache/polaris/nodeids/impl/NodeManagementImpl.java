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
package org.apache.polaris.nodeids.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Integer.bitCount;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import jakarta.annotation.Nonnull;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.IdGeneratorSpec;
import org.apache.polaris.ids.api.ImmutableIdGeneratorSpec;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.ids.spi.IdGeneratorFactory;
import org.apache.polaris.ids.spi.IdGeneratorSource;
import org.apache.polaris.nodeids.api.ImmutableNode;
import org.apache.polaris.nodeids.api.Node;
import org.apache.polaris.nodeids.api.NodeLease;
import org.apache.polaris.nodeids.api.NodeManagement;
import org.apache.polaris.nodeids.api.NodeManagementConfig;
import org.apache.polaris.nodeids.spi.ImmutableBuildableNodeManagementState;
import org.apache.polaris.nodeids.spi.ImmutableNodeState;
import org.apache.polaris.nodeids.spi.NodeState;
import org.apache.polaris.nodeids.spi.NodeStore;
import org.apache.polaris.nodeids.spi.NodeStoreFactory;
import org.apache.polaris.nosql.async.AsyncExec;
import org.apache.polaris.nosql.async.Cancelable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
class NodeManagementImpl implements NodeManagement {
  private static final Logger LOGGER = LoggerFactory.getLogger(NodeManagementImpl.class);

  // should be a power of 2
  private static final int CHECK_BATCH_SIZE = 16;
  static final Duration RESCHEDULE_AFTER_FAILURE = Duration.ofSeconds(10);
  static final Duration RESCHEDULE_UNTIL_EXPIRATION = Duration.ofMinutes(1);
  static final Duration RENEWAL_MIN_LEFT_FOR_RENEWAL = Duration.ofSeconds(30);
  private final NodeManagementConfig config;
  private final MonotonicClock clock;
  private final int numNodeIds;
  private final Set<NodeLeaseImpl> registeredLeases = ConcurrentHashMap.newKeySet();
  private final AsyncExec scheduler;
  private final NodeStoreFactory nodeStoreFactory;

  private NodeStore nodeStore;
  private IdGeneratorFactory<?> idGenFactory;
  private IdGeneratorSpec idGenSpec;
  private IdGenerator systemIdGen;

  private volatile boolean closed;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  NodeManagementImpl(
      NodeManagementConfig config,
      MonotonicClock clock,
      NodeStoreFactory nodeStoreFactory,
      AsyncExec scheduler) {
    var activePeriod = config.leaseDuration().minus(config.renewalPeriod());
    this.nodeStoreFactory = nodeStoreFactory;
    this.numNodeIds = config.numNodes();
    checkArgs(
        () ->
            checkArgument(
                config.leaseDuration().compareTo(Duration.ofMinutes(15)) > 0,
                "leaseDuration must be at least 15 minutes"),
        () ->
            checkArgument(
                activePeriod.isPositive(), "leaseDuration must be greater than renewalPeriod"),
        () ->
            checkArgument(
                activePeriod.compareTo(Duration.ofMinutes(15)) > 0,
                "active period (leaseDuration - renewalPeriod) must be at least 15 minutes"),
        () ->
            checkArgument(
                numNodeIds >= CHECK_BATCH_SIZE && bitCount(numNodeIds) == 1,
                "numNodeIds %s must not be smaller than %s and a power of 2",
                numNodeIds,
                CHECK_BATCH_SIZE));
    this.config = config;
    this.clock = clock;
    this.scheduler = scheduler;
  }

  @SuppressWarnings("BusyWait")
  @PostConstruct
  void init() {
    var idGenSpec =
        (IdGeneratorSpec) ImmutableIdGeneratorSpec.builder().from(config.idGeneratorSpec()).build();
    var validationIdGeneratorSource =
        new IdGeneratorSource() {
          @Override
          public long currentTimeMillis() {
            return clock.currentTimeMillis();
          }

          @Override
          public int nodeId() {
            return 0;
          }
        };

    // If this loop doesn't complete within 10 minutes, we can only give up.
    var timeout = clock.currentInstant().plus(Duration.ofMinutes(10));
    while (true) {
      var existingNodeManagementState = nodeStoreFactory.fetchManagementState();
      if (existingNodeManagementState.isPresent()) {
        var spec = Util.idgenSpecFromManagementState(existingNodeManagementState);
        if (!idGenSpec.equals(spec)) {
          warnOnIncompatibleIdGeneratorSpec(spec);
          idGenSpec = spec;
        }
        var factory = IdGeneratorFactory.lookupFactory(idGenSpec.type());
        // try to build an ID generator instance to validate the spec
        factory.validateParameters(idGenSpec.params(), validationIdGeneratorSource);
        idGenFactory = factory;
        break;
      } else {
        var factory = IdGeneratorFactory.lookupFactory(idGenSpec.type());
        // try to build an ID generator instance to validate the spec
        factory.validateParameters(idGenSpec.params(), validationIdGeneratorSource);

        if (nodeStoreFactory.storeManagementState(
            ImmutableBuildableNodeManagementState.builder().idGeneratorSpec(idGenSpec).build())) {
          LOGGER.info("Persisted node management configuration.");
          idGenFactory = factory;
          break;
        }
      }
      if (timeout.isBefore(clock.currentInstant())) {
        throw new IllegalStateException(
            "Timed out to fetch and/or persist node management configuration. This is likely due to an overloaded backend database.");
      }
      try {
        // random sleep
        Thread.sleep(ThreadLocalRandom.current().nextInt(10, 500));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(
            "Interrupted while waiting for node management configuration to be fetched/persisted",
            e);
      }
    }

    this.idGenSpec = idGenSpec;

    var nodeIdGen = idGenFactory.buildSystemIdGenerator(idGenSpec.params());
    this.systemIdGen = nodeIdGen;
    this.nodeStore = nodeStoreFactory.createNodeStore(nodeIdGen);
  }

  static void checkArgs(Runnable... checks) {
    var violations = new ArrayList<String>();
    for (Runnable check : checks) {
      try {
        check.run();
      } catch (IllegalArgumentException iae) {
        violations.add(iae.getMessage());
      }
    }
    if (!violations.isEmpty()) {
      throw new IllegalArgumentException(String.join(", ", violations));
    }
  }

  @Override
  public long systemIdForNode(int nodeId) {
    return systemIdGen.systemIdForNode(nodeId);
  }

  @Override
  public IdGenerator buildIdGenerator(@Nonnull NodeLease leasedNode) {
    var idGenSource =
        new IdGeneratorSource() {
          @Override
          public long currentTimeMillis() {
            return clock.currentTimeMillis();
          }

          @Override
          public int nodeId() {
            return leasedNode.nodeIdIfValid();
          }
        };
    return idGenFactory.buildIdGenerator(idGenSpec.params(), idGenSource);
  }

  @VisibleForTesting
  void warnOnIncompatibleIdGeneratorSpec(IdGeneratorSpec spec) {
    LOGGER.warn(
        "Provided ID generator specification does not match the persisted, unmodifiable one: provided: {} - persisted: {}",
        idGenSpec,
        spec);
  }

  @Override
  public int maxNumberOfNodes() {
    return numNodeIds;
  }

  @Override
  public Optional<Node> getNodeInfo(int nodeId) {
    checkArgument(nodeId >= 0 && nodeId < numNodeIds, "Illegal node ID " + nodeId);
    var leased =
        registeredLeases.stream()
            .map(NodeLeaseImpl::node)
            .filter(Objects::nonNull)
            .filter(n -> n.id() == nodeId)
            .findFirst();
    if (leased.isPresent()) {
      return leased;
    }

    return nodeStore
        .fetch(nodeId)
        .map(
            nodeObj ->
                ImmutableNode.builder()
                    .id(nodeId)
                    .leaseTimestamp(nodeObj.leaseTimestamp())
                    .expirationTimestamp(nodeObj.expirationTimestamp())
                    .build());
  }

  @Override
  @Nonnull
  public NodeLease lease() {
    var leaseParams = leaseInternal();
    var lease = new NodeLeaseImpl(leaseParams);
    registeredLeases.add(lease);
    return lease;
  }

  private LeaseParams leaseInternal() {
    LOGGER.debug("Leasing a node ID...");

    checkState(!closed, "NodeManagement instance has been closed");

    var now = clock.currentInstant();

    // First, try with a hash of the local network address.
    // The node ID in this attempt is somewhat deterministic for the local machine.
    // This is not really necessary, but it can reduce the overall number of ever-allocated node
    // IDs.
    try {
      var hashOverNetworkInterfaces =
          NetworkInterface.networkInterfaces()
              .mapToInt(
                  ni -> {
                    try {
                      if (ni.isUp()
                          && !ni.isLoopback()
                          && !ni.isVirtual()
                          && !ni.isPointToPoint()) {
                        return ni.getInterfaceAddresses().stream()
                            .mapToInt(InterfaceAddress::hashCode)
                            .reduce((a, b) -> 31 * a + b)
                            .orElse(0);
                      }
                    } catch (SocketException e) {
                      // ignore
                    }
                    return 0;
                  })
              .reduce((a, b) -> 31 * a + b)
              .orElse(0);

      var nodeId = hashOverNetworkInterfaces & (numNodeIds - 1);

      var leased = tryLeaseFromCandidates(new int[] {nodeId}, now);
      if (leased != null) {
        return leased;
      }
    } catch (SocketException e) {
      // ignore
    }

    // If the lease-attempt using the hash over network-interfaced did not succeed, try with
    // randomly picked node IDs.

    var nodeIdsToCheck = IntStream.range(0, numNodeIds).toArray();
    Ints.rotate(nodeIdsToCheck, ThreadLocalRandom.current().nextInt(numNodeIds));

    for (int i = 0; i < numNodeIds; i += CHECK_BATCH_SIZE) {
      var ids = Arrays.copyOfRange(nodeIdsToCheck, i, i + CHECK_BATCH_SIZE);
      var leased = tryLeaseFromCandidates(ids, now);
      if (leased != null) {
        return leased;
      }
    }

    // No approach worked, give up - boom!
    throw new IllegalStateException("Could not lease any node ID");
  }

  @Override
  @PreDestroy
  public void close() {
    LOGGER.debug("Closing NodeManagement");
    closed = true;
    RuntimeException ex = null;
    // Iterating over a snapshot of registeredLeases, as NodeLeaseImpl.release() modifies it.
    // Note: NodeLeaseImpl.release() checks against 'registeredLeases'.
    for (var leased : new ArrayList<>(registeredLeases)) {
      try {
        leased.release();
      } catch (RuntimeException e) {
        if (ex == null) {
          ex = e;
        } else {
          ex.addSuppressed(e);
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

  private LeaseParams tryLeaseFromCandidates(int[] nodeIds, Instant now) {
    var nodesFetched = nodeStore.fetchMany(nodeIds);
    for (int i = 0; i < nodeIds.length; i++) {
      // NodeStore.fetchMany MUST return as many elements as the input array.
      var nodeId = nodeIds[i];
      var node = nodesFetched[i];
      if (canLease(node, now)) {
        var leased = tryLease(nodeId, node);
        if (leased != null) {
          return leased;
        }
      }
    }
    return null;
  }

  private boolean canLease(NodeState node, Instant now) {
    if (node == null) {
      return true;
    }

    return node.expirationTimestamp().compareTo(now) < 0;
  }

  private LeaseParams tryLease(int nodeId, NodeState node) {
    var now = clock.currentInstant();
    var expire = now.plus(config.leaseDuration());

    LOGGER.debug("Try lease node ID {} ...", nodeId);

    var newNode =
        ImmutableNodeState.builder().leaseTimestamp(now).expirationTimestamp(expire).build();
    var persisted = nodeStore.persist(nodeId, Optional.ofNullable(node), newNode);
    if (persisted == null) {
      return null;
    }
    checkState(
        persisted.equals(newNode),
        "Internal error: value of persisted %s != to-persist %s",
        persisted,
        newNode);

    // conditional insert/update succeeded - got the lease!

    var renewal = persisted.expirationTimestamp().minus(config.renewalPeriod());
    return new LeaseParams(nodeId, persisted, renewal);
  }

  record LeaseParams(
      int nodeId,
      NodeState nodeState,
      Instant renewLeaseTimestamp,
      long expirationTimestampMillis) {
    LeaseParams(int nodeId, NodeState nodeState, Instant renewLeaseTimestamp) {
      this(nodeId, nodeState, renewLeaseTimestamp, nodeState.expirationTimestamp().toEpochMilli());
    }
  }

  class NodeLeaseImpl implements NodeLease {
    private volatile LeaseParams leaseParams;
    private volatile Cancelable<Void> renewFuture;
    private final Lock lock = new ReentrantLock();

    NodeLeaseImpl(LeaseParams leaseParams) {
      newLease(leaseParams);
    }

    private void newLease(LeaseParams leaseParams) {
      this.leaseParams = leaseParams;
      var renewalIn =
          Duration.ofMillis(
              leaseParams.renewLeaseTimestamp.toEpochMilli() - clock.currentTimeMillis());
      reschedule(renewalIn);
      LOGGER.info(
          "New lease for node#{} acquired, valid until {} (renewal scheduled in {}).",
          leaseParams.nodeId,
          leaseParams.nodeState.expirationTimestamp(),
          renewalIn.truncatedTo(ChronoUnit.SECONDS));
    }

    private void reschedule(Duration delay) {
      // 'delay' can safely be negative, which is handled by the scheduler.
      this.renewFuture = scheduler.schedule(this::renewAndReschedule, delay);
    }

    private void renewAndReschedule() {
      if (closed) {
        return;
      }
      lock.lock();
      try {
        var lp = leaseParams;
        if (lp == null) {
          return;
        }
        var id = lp.nodeId;
        var validFor = Duration.ofMillis(lp.expirationTimestampMillis - clock.currentTimeMillis());
        if (validFor.compareTo(RENEWAL_MIN_LEFT_FOR_RENEWAL) > 0) {
          LOGGER.debug("Renewing lease for node#{}...", id);
          try {
            // The call to 'renewLease()` can throw an ISE if this call races w/
            // NodeManagement.close().
            // In that case, it's not just an annoyance, nothing more.
            var newRenewalTimestamp = renewInternal(lp);
            LOGGER.info("Lease for node#{} renewed until {}.", id, newRenewalTimestamp);
            reschedule(
                Duration.ofMillis(newRenewalTimestamp.toEpochMilli() - clock.currentTimeMillis()));
            return;
          } catch (Exception e) {
            if (validFor.compareTo(RESCHEDULE_UNTIL_EXPIRATION) > 0) {
              LOGGER.warn(
                  "Could not renew lease for node#{} (lease still valid for {}), retrying in 10 seconds",
                  id,
                  validFor,
                  e);
              reschedule(RESCHEDULE_AFTER_FAILURE);
              return;
            } else {
              LOGGER.warn(
                  "Could not renew lease for node#{} (lease still valid for {}), attempting to get a new lease ...",
                  id,
                  validFor,
                  e);
            }
          }
        } else {
          LOGGER.warn(
              "Not renewing node lease for node#{}, because the lease is currently no longer valid. "
                  + "The node lease should have been valid, but was not. This may happen if the node was massively overloaded.",
              id);
        }

        try {
          var newLease = leaseInternal();
          newLease(newLease);
        } catch (Exception e) {
          LOGGER.warn("Could not get a new lease, reattempting in 10 seconds", e);
          reschedule(RESCHEDULE_AFTER_FAILURE);
        }

      } finally {
        lock.unlock();
      }
    }

    @Override
    public int nodeIdIfValid() {
      var lp = leaseParams;
      if (lp == null || lp.expirationTimestampMillis <= clock.currentTimeMillis()) {
        return -1;
      }
      return lp.nodeId;
    }

    @Override
    public Node node() {
      var lp = leaseParams;
      return lp != null
          ? ImmutableNode.builder()
              .id(lp.nodeId)
              .expirationTimestamp(lp.nodeState.expirationTimestamp())
              .leaseTimestamp(lp.nodeState.leaseTimestamp())
              .renewLeaseTimestamp(lp.renewLeaseTimestamp)
              .build()
          : null;
    }

    @Override
    public void release() {
      lock.lock();
      try {
        if (!registeredLeases.remove(this)) {
          return;
        }

        // Shall never be null, but be safe here.
        if (renewFuture != null) {
          renewFuture.cancel();
        }

        var lp = leaseParams;
        if (lp == null) {
          return;
        }
        var id = lp.nodeId;
        LOGGER.info("Releasing lease for node id {}", id);

        var now = clock.currentInstant();
        var activeState = lp.nodeState;
        var nodeAsReleased =
            ImmutableNodeState.builder().from(activeState).expirationTimestamp(now).build();

        leaseParams = null;

        var updated = nodeStore.persist(id, Optional.of(activeState), nodeAsReleased);
        checkState(
            updated != null && updated.equals(nodeAsReleased),
            "State of the node %s has been unexpectedly changed: value of persisted %s != to-persist %s",
            id,
            updated,
            nodeAsReleased);
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void renew() {
      lock.lock();
      try {
        var lp = leaseParams;
        checkState(lp != null, "Lease has already been released");
        renewInternal(lp);
      } finally {
        lock.unlock();
      }
    }

    private Instant renewInternal(LeaseParams lp) {
      var id = lp.nodeId;
      LOGGER.debug("Renewing lease for node id {}", id);

      var now = clock.currentInstant();
      var expire = now.plus(config.leaseDuration());
      var renewal = expire.minus(config.renewalPeriod());

      var newNode =
          ImmutableNodeState.builder()
              .leaseTimestamp(lp.nodeState.leaseTimestamp())
              .expirationTimestamp(expire)
              .build();

      var persisted = nodeStore.persist(id, Optional.of(lp.nodeState), newNode);
      checkState(
          persisted != null,
          "State of the NodeState for nodeId %s has been unexpectedly changed!",
          id);
      checkState(
          persisted.equals(newNode),
          "Internal error: value of persisted %s != to-persist %s",
          persisted,
          newNode);

      leaseParams = new LeaseParams(id, persisted, renewal);

      return renewal;
    }
  }
}
