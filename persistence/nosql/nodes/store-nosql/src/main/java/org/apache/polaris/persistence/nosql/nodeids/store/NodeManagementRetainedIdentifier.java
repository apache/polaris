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

import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;

import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.stream.IntStream;
import org.apache.polaris.persistence.nosql.maintenance.spi.PerRealmRetainedIdentifier;
import org.apache.polaris.persistence.nosql.maintenance.spi.RetainedCollector;
import org.apache.polaris.persistence.nosql.nodeids.api.NodeManagement;

@ApplicationScoped
class NodeManagementRetainedIdentifier implements PerRealmRetainedIdentifier {
  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  NodeManagement nodeManagement;

  @Override
  public String name() {
    return "Nodes";
  }

  @Override
  public boolean identifyRetained(@Nonnull RetainedCollector collector) {
    if (!collector.isSystemRealm()) {
      return false;
    }

    IntStream.range(0, nodeManagement.maxNumberOfNodes())
        .mapToLong(nodeId -> nodeManagement.systemIdForNode(nodeId))
        .mapToObj(NodeStoreImpl::constructObjId)
        .forEach(collector::retainObject);

    collector.retainObject(objRef(NodeManagementObj.TYPE, NodeManagementObj.CONSTANT_ID));

    // Intentionally return false, let the maintenance service's identifier decide
    return false;
  }
}
