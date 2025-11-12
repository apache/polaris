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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.nodeids.spi.NodeManagementState;

@PolarisImmutable
@JsonSerialize(as = ImmutableNodeManagementObj.class)
@JsonDeserialize(as = ImmutableNodeManagementObj.class)
public interface NodeManagementObj extends Obj, NodeManagementState {
  ObjType TYPE = new NodeManagementObjType();
  long CONSTANT_ID = Long.MAX_VALUE;

  @Nullable
  @Override
  default String versionToken() {
    return "immutable";
  }

  @Override
  default long id() {
    return CONSTANT_ID; // constant
  }

  @Override
  default ObjType type() {
    return TYPE;
  }

  final class NodeManagementObjType extends AbstractObjType<NodeManagementObj> {
    public NodeManagementObjType() {
      super("nodes", "Nodes", NodeManagementObj.class);
    }
  }
}
