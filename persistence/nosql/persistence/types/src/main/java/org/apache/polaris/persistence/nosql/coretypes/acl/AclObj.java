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
package org.apache.polaris.persistence.nosql.coretypes.acl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.OptionalInt;
import org.apache.polaris.authz.api.Acl;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;

/** */
@PolarisImmutable
@JsonSerialize(as = ImmutableAclObj.class)
@JsonDeserialize(as = ImmutableAclObj.class)
public interface AclObj extends Obj {

  ObjType TYPE = new AclObjType();

  // TODO add acl-entries using space-efficient bit sets
  // TODO for bit-sets we probably need a persisted dictionary of privilege-name to privilege-ID -
  //  for extensibility

  /**
   * Refers to the {@linkplain PolarisBaseEntity#getId() entity id} / {@linkplain ObjBase#stableId()
   * stable ID}.
   */
  long securableId();

  /** Refers to {@link org.apache.polaris.core.entity.PolarisEntityType}. */
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  OptionalInt securableTypeCode();

  Acl acl();

  @Override
  default ObjType type() {
    return TYPE;
  }

  static ImmutableAclObj.Builder builder() {
    return ImmutableAclObj.builder();
  }

  final class AclObjType extends AbstractObjType<AclObj> {
    public AclObjType() {
      super("acl", "ACL", AclObj.class);
    }
  }
}
