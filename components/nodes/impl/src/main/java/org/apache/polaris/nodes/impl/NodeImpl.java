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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Instant;
import org.apache.polaris.nodes.api.Node;
import org.apache.polaris.nodes.spi.NodeState;

final class NodeImpl implements Node {
  private final int id;
  private final Instant creationTimestamp;

  private volatile NodeState nodeState;
  private volatile boolean valid;
  private volatile long expirationTimestampMillis;
  private volatile Instant renewLeaseTimestamp;

  NodeImpl(NodeState nodeState, int id, Instant renewLeaseTimestamp) {
    this.nodeState = nodeState;
    this.id = id;
    this.creationTimestamp = nodeState.leaseTimestamp();
    this.renewLeaseTimestamp = renewLeaseTimestamp;
    this.expirationTimestampMillis = nodeState.expirationTimestamp().toEpochMilli();
    this.valid = true;
  }

  void deactivate() {
    this.valid = false;
  }

  void updateState(NodeState newNodeState, Instant renewLeaseTimestamp) {
    this.nodeState = newNodeState;
    this.renewLeaseTimestamp = renewLeaseTimestamp;
    this.expirationTimestampMillis = newNodeState.expirationTimestamp().toEpochMilli();
  }

  NodeState nodeState() {
    return nodeState;
  }

  @Override
  public int id() {
    return id;
  }

  @Override
  public boolean valid(long nowInMillis) {
    return this.valid && nowInMillis < expirationTimestampMillis;
  }

  @Override
  @Nonnull
  public Instant leaseTimestamp() {
    return nodeState.leaseTimestamp();
  }

  @Override
  @Nullable
  public Instant renewLeaseTimestamp() {
    return renewLeaseTimestamp;
  }

  @Override
  @Nonnull
  public Instant expirationTimestamp() {
    return nodeState.expirationTimestamp();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;

    NodeImpl node = (NodeImpl) o;
    return id == node.id;
  }

  @Override
  public int hashCode() {
    return id;
  }

  @Override
  public String toString() {
    return "NodeImpl{"
        + "id="
        + id
        + ", creationTimestamp="
        + creationTimestamp
        + ", valid="
        + valid
        + ", renewLeaseTimestamp="
        + renewLeaseTimestamp
        + '}';
  }
}
