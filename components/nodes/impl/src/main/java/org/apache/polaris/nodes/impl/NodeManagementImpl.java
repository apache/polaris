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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Integer.bitCount;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.IdGeneratorSpec;
import org.apache.polaris.ids.api.ImmutableIdGeneratorSpec;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.ids.spi.IdGeneratorFactory;
import org.apache.polaris.nodes.api.Node;
import org.apache.polaris.nodes.api.NodeManagement;
import org.apache.polaris.nodes.api.NodeManagementConfig;
import org.apache.polaris.nodes.spi.ImmutableBuildableNodeManagementState;
import org.apache.polaris.nodes.spi.ImmutableNodeState;
import org.apache.polaris.nodes.spi.NodeState;
import org.apache.polaris.nodes.spi.NodeStore;
import org.apache.polaris.nodes.spi.NodeStoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
class NodeManagementImpl implements NodeManagement {
  private static final Logger LOGGER = LoggerFactory.getLogger(NodeManagementImpl.class);

  // should be a power of 2
  private static final int CHECK_BATCH_SIZE = 16;
  private final NodeStore nodeStore;
  private final NodeManagementConfig config;
  private final Supplier<Instant> nowSupplier;
  private final int numNodeIds;
  private final IdGeneratorFactory<?> idGenFactory;
  private final IdGeneratorSpec idGenSpec;

  private final Set<NodeImpl> activeLeases = ConcurrentHashMap.newKeySet();
  private final IdGenerator systemIdGen;

  private volatile boolean closed;

  @Inject
  NodeManagementImpl(
      NodeManagementConfig config, MonotonicClock clock, NodeStoreFactory nodeStoreFactory) {
    this(config, clock::currentInstant, nodeStoreFactory);
  }

  @VisibleForTesting
  NodeManagementImpl(
      NodeManagementConfig config,
      Supplier<Instant> nowSupplier,
      NodeStoreFactory nodeStoreFactory) {
    var activePeriod = config.leaseDuration().minus(config.renewalPeriod());
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
    this.nowSupplier = nowSupplier;

    var idGenSpec =
        (IdGeneratorSpec) ImmutableIdGeneratorSpec.builder().from(config.idGeneratorSpec()).build();
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
        factory.validateParameters(idGenSpec.params(), 0, () -> nowSupplier.get().toEpochMilli());
        idGenFactory = factory;
        break;
      } else {
        var factory = IdGeneratorFactory.lookupFactory(idGenSpec.type());
        // try to build an ID generator instance to validate the spec
        factory.validateParameters(idGenSpec.params(), 0, () -> nowSupplier.get().toEpochMilli());

        if (nodeStoreFactory.storeManagementState(
            ImmutableBuildableNodeManagementState.builder().idGeneratorSpec(idGenSpec).build())) {
          LOGGER.info("Persisted node management configuration.");
          idGenFactory = factory;
          break;
        }
      }
    }

    this.idGenSpec = idGenSpec;

    var nodeIdGen = idGenFactory.buildSystemIdGenerator(idGenSpec.params(), () -> 0L);
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
  public IdGenerator buildIdGenerator(@Nonnull Node leasedNode, @Nonnull LongSupplier clockMillis) {
    return idGenFactory.buildIdGenerator(
        idGenSpec.params(),
        leasedNode.id(),
        clockMillis,
        () -> leasedNode.valid(clockMillis.getAsLong()));
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
        activeLeases.stream().filter(n -> n.id() == nodeId).map(Node.class::cast).findFirst();
    if (leased.isPresent()) {
      return leased;
    }

    return nodeStore.fetch(nodeId).map(nodeObj -> new NodeImpl(nodeObj, nodeId, null));
  }

  @Override
  @Nonnull
  public Node lease() {
    LOGGER.debug("Leasing a node ID...");

    checkState(!closed, "NodeManagement instance has been closed");

    var now = nowSupplier.get();

    var checkedIds = new BitSet(numNodeIds); // 128 bytes for 1024 nodes - not too much
    var rand = ThreadLocalRandom.current();

    var generateNodeId =
        (IntSupplier)
            () -> {
              if (checkedIds.cardinality() == numNodeIds) {
                return -1;
              }
              while (true) {
                var nodeId = rand.nextInt(numNodeIds);
                if (!checkedIds.get(nodeId)) {
                  checkedIds.set(nodeId);
                  return nodeId;
                }
              }
            };

    var ids = new Integer[CHECK_BATCH_SIZE];

    // First, try with a hash of the local network address...
    // The node ID in this attempt is somewhat deterministic for the local machine.
    // This is not really a necessity, but it can reduce the overall amount of ever allocated node
    // IDs. It is rather a cosmetic thing, so that a LIST NODES command in the Persistence-REPL
    // doesn't flood users after a couple of restarts.
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

      var leased = tryLeaseFromCandidates(new Integer[] {nodeId}, now);
      if (leased != null) {
        return leased;
      }
    } catch (SocketException e) {
      // ignore
    }

    // Next, try with randomly picked node IDs and attempt to lease...
    for (int i = 0; i < 3 * numNodeIds / CHECK_BATCH_SIZE; i++) {
      Arrays.fill(ids, null);
      for (int j = 0; j < ids.length; j++) {
        var nodeId = generateNodeId.getAsInt();
        if (nodeId == -1) {
          break;
        }
        ids[j] = nodeId;
      }

      var leased = tryLeaseFromCandidates(ids, now);
      if (leased != null) {
        return leased;
      }
    }

    // Next, try again, but using sequential nodeIds starting at a random offset...
    var offset = rand.nextInt(numNodeIds);
    for (int i = 0; i < numNodeIds; i += CHECK_BATCH_SIZE) {
      Arrays.fill(ids, null);
      for (int j = 0; j < ids.length; j++) {
        var nodeId = (offset + i + j) % numNodeIds;
        ids[j] = nodeId;
      }
      var leased = tryLeaseFromCandidates(ids, now);
      if (leased != null) {
        return leased;
      }
    }

    // No approach worked, give up - boom!
    throw new IllegalStateException("Could not lease any node ID");
  }

  @Override
  public void releaseLease(@Nonnull Node lease) {
    checkState(!closed, "NodeManagement instance has been closed");
    checkArgument(lease instanceof NodeImpl);
    if (!activeLeases.remove(lease)) {
      return;
    }

    doReleaseLease(lease);
  }

  private void doReleaseLease(Node lease) {
    LOGGER.debug("Releasing node id {}", lease.id());

    var n = (NodeImpl) lease;

    var now = nowSupplier.get();
    var nodeAsReleased =
        ImmutableNodeState.builder().from(n.nodeState()).expirationTimestamp(now).build();

    var activeState = n.nodeState();
    n.deactivate();
    n.updateState(nodeAsReleased, null);

    var updated = nodeStore.persist(lease.id(), Optional.of(activeState), nodeAsReleased);
    checkState(
        updated != null && updated.equals(nodeAsReleased),
        "State of the node %s has been unexpectedly changed!",
        lease.id());
    checkState(
        updated.equals(nodeAsReleased),
        "Internal error: value of persisted %s != to-persist %s",
        updated,
        nodeAsReleased);
  }

  @Override
  public void renewLease(@Nonnull Node existing) {
    checkState(!closed, "NodeManagement instance has been closed");
    checkArgument(existing instanceof NodeImpl);
    checkState(
        activeLeases.contains(existing),
        "Node is not actively maintained by this NodeManagement instance");

    var n = (NodeImpl) existing;
    doRenewLease(n);
  }

  @Override
  @PreDestroy
  public void close() {
    LOGGER.debug("Closing NodeManagement");
    closed = true;
    RuntimeException ex = null;
    for (NodeImpl n : activeLeases) {
      try {
        doReleaseLease(n);
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

  private Node tryLeaseFromCandidates(Integer[] nodeIds, Instant now) {
    var nodesFetched = nodeStore.fetchMany(nodeIds);
    for (int i = 0; i < nodeIds.length; i++) {
      var nodeId = nodeIds[i];
      if (nodeId != null) {
        var id = (int) nodeId;
        var node = nodesFetched[i];
        if (canLease(node, now)) {
          var leased = tryLease(id, node);
          if (leased != null) {
            return leased;
          }
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

  private Node tryLease(int nodeId, NodeState node) {
    var now = nowSupplier.get();
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
    return registerLease(nodeId, persisted);
  }

  private Node registerLease(int nodeId, NodeState nodeState) {
    var renewal = nodeState.expirationTimestamp().minus(config.renewalPeriod());
    var node = new NodeImpl(nodeState, nodeId, renewal);

    activeLeases.add(node);

    return node;
  }

  private void doRenewLease(NodeImpl n) {
    LOGGER.debug("Renewing lease for node id {}", n.id());

    var now = nowSupplier.get();
    var expire = now.plus(config.leaseDuration());
    var renewal = expire.minus(config.renewalPeriod());

    var newNode =
        ImmutableNodeState.builder()
            .leaseTimestamp(n.leaseTimestamp())
            .expirationTimestamp(expire)
            .build();

    var persisted = nodeStore.persist(n.id(), Optional.of(n.nodeState()), newNode);
    checkState(
        persisted != null,
        "State of the NodeState for nodeId %s has been unexpectedly changed!",
        n.id());
    checkState(
        persisted.equals(newNode),
        "Internal error: value of persisted %s != to-persist %s",
        persisted,
        newNode);

    n.updateState(persisted, renewal);
  }
}
