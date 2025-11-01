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
package org.apache.polaris.persistence.nosql.coretypes.principals;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.immutables.value.Value;

@PolarisImmutable
@JsonSerialize(as = ImmutablePrincipalRoleObj.class)
@JsonDeserialize(as = ImmutablePrincipalRoleObj.class)
public interface PrincipalRoleObj extends ObjBase {
  ObjType TYPE = new PrincipalRoleObjType();

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  @Value.Default
  default boolean federated() {
    return false;
  }

  @Override
  default ObjType type() {
    return TYPE;
  }

  static Builder builder() {
    return ImmutablePrincipalRoleObj.builder();
  }

  final class PrincipalRoleObjType extends AbstractObjType<PrincipalRoleObj> {
    public PrincipalRoleObjType() {
      super("pr-r", "Principal Role", PrincipalRoleObj.class);
    }
  }

  interface Builder extends ObjBase.Builder<PrincipalRoleObj, Builder> {

    @CanIgnoreReturnValue
    Builder from(PrincipalRoleObj from);

    @CanIgnoreReturnValue
    Builder federated(boolean federated);
  }
}
