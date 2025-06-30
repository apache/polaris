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
package org.apache.polaris.core.entity;

import org.apache.polaris.core.admin.model.Principal;

/** Wrapper for translating between the REST Principal object and the base PolarisEntity type. */
public class PrincipalEntity extends PolarisEntity {
  public PrincipalEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
  }

  public static PrincipalEntity of(PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new PrincipalEntity(sourceEntity);
    }
    return null;
  }

  public static PrincipalEntity fromPrincipal(Principal principal) {
    return new Builder()
        .setName(principal.getName())
        .setProperties(principal.getProperties())
        .setClientId(principal.getClientId())
        .build();
  }

  public String getClientId() {
    return getInternalPropertiesAsMap().get(PolarisEntityConstants.getClientIdPropertyName());
  }

  public static class Builder extends PolarisEntity.BaseBuilder<PrincipalEntity, Builder> {
    public Builder() {
      super();
      setType(PolarisEntityType.PRINCIPAL);
      setCatalogId(PolarisEntityConstants.getNullId());
      setParentId(PolarisEntityConstants.getRootEntityId());
    }

    public Builder(PrincipalEntity original) {
      super(original);
    }

    public Builder setClientId(String clientId) {
      internalProperties.put(PolarisEntityConstants.getClientIdPropertyName(), clientId);
      return this;
    }

    public Builder setCredentialRotationRequiredState() {
      internalProperties.put(
          PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE, "true");
      return this;
    }

    @Override
    public PrincipalEntity build() {
      return new PrincipalEntity(buildBase());
    }
  }
}
