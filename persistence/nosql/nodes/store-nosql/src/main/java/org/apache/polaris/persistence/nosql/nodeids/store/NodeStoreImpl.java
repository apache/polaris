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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.polaris.persistence.nosql.api.Realms.SYSTEM_REALM_ID;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import java.util.UUID;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.nodeids.spi.ImmutableNodeState;
import org.apache.polaris.persistence.nosql.nodeids.spi.NodeState;
import org.apache.polaris.persistence.nosql.nodeids.spi.NodeStore;

record NodeStoreImpl(Persistence startupPersistence, IdGenerator idGenerator) implements NodeStore {
  NodeStoreImpl {
    checkArgument(
        SYSTEM_REALM_ID.equals(startupPersistence.realmId()),
        "Realms management must happen in the %s realm",
        SYSTEM_REALM_ID);
  }

  @Override
  @Nullable
  public NodeState persist(
      int nodeId, Optional<NodeState> expectedNodeState, @Nonnull NodeState newState) {
    checkArgument(nodeId >= 0, "Illegal node ID %s", nodeId);

    var persistenceId = idGenerator.systemIdForNode(nodeId);
    var newObj =
        ImmutableNodeObj.builder()
            .leaseTimestamp(newState.leaseTimestamp())
            .expirationTimestamp(newState.expirationTimestamp())
            .id(persistenceId)
            .versionToken(UUID.randomUUID().toString())
            .build();

    var existing = startupPersistence.fetch(constructObjId(persistenceId), NodeObj.class);
    if (expectedNodeState.isEmpty()) {
      return existing == null
          ? asNodeState(startupPersistence.conditionalInsert(newObj, NodeObj.class))
          : null;
    } else {
      if (existing == null) {
        return null;
      }
      var expected = expectedNodeState.get();
      var real = asNodeState(existing);
      if (!expected.equals(real)) {
        return null;
      }
      return asNodeState(startupPersistence.conditionalUpdate(existing, newObj, NodeObj.class));
    }
  }

  @Override
  @Nonnull
  public NodeState[] fetchMany(@Nonnull int... nodeIds) {
    var objIds = new ObjRef[nodeIds.length];
    for (int i = 0; i < nodeIds.length; i++) {
      var nodeId = nodeIds[i];
      checkArgument(nodeId >= 0, "Illegal node ID %s", nodeId);
      objIds[i] = objIdForNode(nodeId);
    }
    var fetched = startupPersistence.fetchMany(NodeObj.class, objIds);
    var result = new NodeState[nodeIds.length];
    for (int i = 0; i < nodeIds.length; i++) {
      result[i] = asNodeState(fetched[i]);
    }
    return result;
  }

  @Override
  public Optional<NodeState> fetch(int nodeId) {
    var objId = objIdForNode(nodeId);
    return Optional.ofNullable(asNodeState(startupPersistence.fetch(objId, NodeObj.class)));
  }

  private static NodeState asNodeState(NodeObj result) {
    return result != null
        ? ImmutableNodeState.builder()
            .leaseTimestamp(result.leaseTimestamp())
            .expirationTimestamp(result.expirationTimestamp())
            .build()
        : null;
  }

  ObjRef objIdForNode(int nodeId) {
    return constructObjId(idGenerator.systemIdForNode(nodeId));
  }

  static ObjRef constructObjId(long persistenceId) {
    return objRef(NodeObj.TYPE.id(), persistenceId, 1);
  }
}
