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

import static org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalRolesObj.PRINCIPAL_ROLES_REF_NAME;

import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.table.federated.FederatedEntities;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalRoleObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalRolesObj;

final class PrincipalRoleMapping extends BaseMapping<PrincipalRoleObj, PrincipalRoleObj.Builder> {
  PrincipalRoleMapping() {
    super(
        PrincipalRoleObj.TYPE,
        PrincipalRolesObj.TYPE,
        PRINCIPAL_ROLES_REF_NAME,
        PolarisEntityType.PRINCIPAL_ROLE);
  }

  @Override
  public PrincipalRoleObj.Builder newObjBuilder(@Nonnull PolarisEntitySubType subType) {
    return PrincipalRoleObj.builder();
  }

  @Override
  void mapToObjTypeSpecific(
      PrincipalRoleObj.Builder baseBuilder,
      @Nonnull PolarisBaseEntity entity,
      Optional<PolarisPrincipalSecrets> principalSecrets,
      Map<String, String> properties,
      Map<String, String> internalProperties) {
    super.mapToObjTypeSpecific(
        baseBuilder, entity, principalSecrets, properties, internalProperties);
    var federated =
        Boolean.parseBoolean(internalProperties.remove(FederatedEntities.FEDERATED_ENTITY));
    baseBuilder.federated(federated);
  }

  @Override
  void mapToEntityTypeSpecific(
      PrincipalRoleObj o,
      HashMap<String, String> properties,
      HashMap<String, String> internalProperties,
      PolarisEntitySubType subType) {
    super.mapToEntityTypeSpecific(o, properties, internalProperties, subType);
    if (o.federated()) {
      internalProperties.put(FederatedEntities.FEDERATED_ENTITY, "true");
    }
  }
}
