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
package org.apache.polaris.authz.store.nosql;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.polaris.authz.spi.PrivilegesMapping;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;

@PolarisImmutable
@JsonSerialize(as = ImmutablePrivilegesMappingObj.class)
@JsonDeserialize(as = ImmutablePrivilegesMappingObj.class)
public interface PrivilegesMappingObj extends Obj {

  String PRIVILEGES_MAPPING_REF_NAME = "privileges-mapping";

  ObjType TYPE = new PrivilegesMappingObjType();

  @Override
  default ObjType type() {
    return TYPE;
  }

  PrivilegesMapping privilegesMapping();

  static ImmutablePrivilegesMappingObj.Builder builder() {
    return ImmutablePrivilegesMappingObj.builder();
  }

  final class PrivilegesMappingObjType extends AbstractObjType<PrivilegesMappingObj> {
    public PrivilegesMappingObjType() {
      super("privileges-mapping", "Privileges Mapping", PrivilegesMappingObj.class);
    }
  }
}
