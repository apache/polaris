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

package org.apache.polaris.persistence.nosql.coretypes.mapping;

import static org.apache.polaris.core.entity.table.federated.FederatedEntities.FEDERATED_ENTITY;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.BaseTestMapping.MappingSample.entityWithInternalProperty;
import static org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalRolesObj.PRINCIPAL_ROLES_REF_NAME;

import java.util.stream.Stream;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalRoleObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalRolesObj;
import org.junit.jupiter.params.provider.MethodSource;

@MethodSource("parameters")
public class TestPrincipalRoleMapping extends BaseTestMapping {
  @Override
  public ObjType containerObjType() {
    return PrincipalRolesObj.TYPE;
  }

  @Override
  public Class<? extends ObjBase> baseObjClass() {
    return null;
  }

  @Override
  public String refName() {
    return PRINCIPAL_ROLES_REF_NAME;
  }

  @Override
  public boolean isCatalogContent() {
    return false;
  }

  @Override
  public boolean isCatalogRelated() {
    return false;
  }

  @Override
  public boolean isWithStorage() {
    return false;
  }

  static Stream<BaseTestParameter> parameters() {
    return Stream.of(
        new BaseTestParameter(
            PolarisEntityType.PRINCIPAL_ROLE,
            PolarisEntitySubType.NULL_SUBTYPE,
            PrincipalRoleObj.TYPE) {
          @Override
          public PrincipalRoleObj.Builder objBuilder() {
            return PrincipalRoleObj.builder();
          }

          @Override
          public Stream<MappingSample> typeVariations(MappingSample base) {
            return Stream.of(
                base,
                //
                new MappingSample(
                    objBuilder().from(base.obj()).federated(true).build(),
                    entityWithInternalProperty(base.entity(), FEDERATED_ENTITY, "true"))
                //
                );
          }
        });
  }
}
