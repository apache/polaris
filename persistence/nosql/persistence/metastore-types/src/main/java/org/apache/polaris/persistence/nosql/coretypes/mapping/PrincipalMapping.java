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

import static org.apache.polaris.core.entity.PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE;
import static org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalsObj.PRINCIPALS_REF_NAME;

import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalsObj;

final class PrincipalMapping extends BaseMapping<PrincipalObj, PrincipalObj.Builder> {
  PrincipalMapping() {
    super(PrincipalObj.TYPE, PrincipalsObj.TYPE, PRINCIPALS_REF_NAME, PolarisEntityType.PRINCIPAL);
  }

  @Override
  public PrincipalObj.Builder newObjBuilder(@Nonnull PolarisEntitySubType subType) {
    return PrincipalObj.builder();
  }

  @Override
  void mapToObjTypeSpecific(
      PrincipalObj.Builder baseBuilder,
      @Nonnull PolarisBaseEntity entity,
      Optional<PolarisPrincipalSecrets> principalSecrets,
      Map<String, String> properties,
      Map<String, String> internalProperties) {
    super.mapToObjTypeSpecific(
        baseBuilder, entity, principalSecrets, properties, internalProperties);
    var credentialRotationRequired =
        internalProperties.remove(PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE);
    internalProperties.remove(PolarisEntityConstants.getClientIdPropertyName());

    var principalObjBuilder =
        baseBuilder.credentialRotationRequired(credentialRotationRequired != null);
    principalSecrets.ifPresent(
        secrets ->
            principalObjBuilder
                .clientId(secrets.getPrincipalClientId())
                .mainSecretHash(secrets.getMainSecretHash())
                .secondarySecretHash(secrets.getSecondarySecretHash())
                .secretSalt(secrets.getSecretSalt()));
  }

  @Override
  void mapToEntityTypeSpecific(
      PrincipalObj o,
      HashMap<String, String> properties,
      HashMap<String, String> internalProperties,
      PolarisEntitySubType subType) {
    super.mapToEntityTypeSpecific(o, properties, internalProperties, subType);
    if (o.credentialRotationRequired()) {
      internalProperties.put(PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE, "true");
    }
    o.clientId()
        .ifPresent(
            v -> internalProperties.put(PolarisEntityConstants.getClientIdPropertyName(), v));
  }
}
