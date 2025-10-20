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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.StartupPersistence;
import org.apache.polaris.persistence.nosql.nodeids.spi.NodeManagementState;
import org.apache.polaris.persistence.nosql.nodeids.spi.NodeStore;
import org.apache.polaris.persistence.nosql.nodeids.spi.NodeStoreFactory;

@ApplicationScoped
class NodeStoreFactoryImpl implements NodeStoreFactory {
  private final Persistence startupPersistence;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  NodeStoreFactoryImpl(@StartupPersistence Persistence startupPersistence) {
    checkArgument(
        SYSTEM_REALM_ID.equals(startupPersistence.realmId()),
        "Realms management must happen in the %s realm",
        SYSTEM_REALM_ID);
    this.startupPersistence = startupPersistence;
  }

  @Override
  @Nonnull
  public NodeStore createNodeStore(@Nonnull IdGenerator idGenerator) {
    return new NodeStoreImpl(startupPersistence, idGenerator);
  }

  @Override
  public Optional<NodeManagementState> fetchManagementState() {
    return Optional.ofNullable(
        startupPersistence.fetch(
            objRef(NodeManagementObj.TYPE, NodeManagementObj.CONSTANT_ID, 1),
            NodeManagementObj.class));
  }

  @Override
  public boolean storeManagementState(@Nonnull NodeManagementState state) {
    return startupPersistence.conditionalInsert(
            ImmutableNodeManagementObj.builder().from(state).build(), NodeManagementObj.class)
        != null;
  }
}
