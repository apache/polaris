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
package org.apache.polaris.persistence.nosql.coretypes.content;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;

/** Namespace that represents a remote catalog. */
@PolarisImmutable
@JsonSerialize(as = ImmutableRemoteNamespaceObj.class)
@JsonDeserialize(as = ImmutableRemoteNamespaceObj.class)
public interface RemoteNamespaceObj extends NamespaceObj {
  ObjType TYPE = new RemoteNamespaceObjType();

  @Override
  default ObjType type() {
    return TYPE;
  }

  static Builder builder() {
    return ImmutableRemoteNamespaceObj.builder();
  }

  final class RemoteNamespaceObjType extends AbstractObjType<RemoteNamespaceObj> {
    public RemoteNamespaceObjType() {
      super("ns-r", "Namespace (remote)", RemoteNamespaceObj.class);
    }
  }

  interface Builder extends NamespaceObj.Builder<RemoteNamespaceObj, Builder> {

    @CanIgnoreReturnValue
    Builder from(RemoteNamespaceObj from);
  }
}
