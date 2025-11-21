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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.polaris.persistence.nosql.nodeids.spi.NodeState;
import org.apache.polaris.persistence.nosql.nodeids.spi.NodeStore;

public class MockNodeStore implements NodeStore {
  private final Map<Integer, NodeState> nodeStates = new ConcurrentHashMap<>();

  @Nullable
  @Override
  public NodeState persist(
      int nodeId, Optional<NodeState> expectedNodeState, @Nonnull NodeState newState) {
    if (nodeId >= 0 && expectedNodeState.isPresent()) {
      var result =
          nodeStates.computeIfPresent(
              nodeId,
              (key, existing) -> {
                if (expectedNodeState.get().equals(existing)) {
                  return newState;
                }
                return existing;
              });
      if (result == newState) {
        return newState;
      }
    } else {
      if (nodeStates.putIfAbsent(nodeId, newState) == null) {
        return newState;
      }
    }
    return null;
  }

  @Nonnull
  @Override
  public NodeState[] fetchMany(@Nonnull int... nodeIds) {
    var r = new NodeState[nodeIds.length];
    for (int i = 0; i < nodeIds.length; i++) {
      var id = nodeIds[i];
      if (id >= 0) {
        r[i] = nodeStates.get(id);
      }
    }
    return r;
  }

  @Override
  public Optional<NodeState> fetch(int nodeId) {
    return Optional.ofNullable(nodeStates.get(nodeId));
  }
}
