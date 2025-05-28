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

package org.apache.polaris.service.quarkus.identity.mutation;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.connection.AuthenticationType;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.identity.registry.ServiceIdentityRegistry;
import org.apache.polaris.service.identity.mutation.EntityMutator;

@RequestScoped
public class CatalogEntityConnectionConfigMutator implements EntityMutator {

  private static final String MUTATOR_ID = "catalog-connection-config";

  ServiceIdentityRegistry serviceIdentityRegistry;

  @Inject
  CatalogEntityConnectionConfigMutator(ServiceIdentityRegistry serviceIdentityRegistry) {
    this.serviceIdentityRegistry = serviceIdentityRegistry;
  }

  @Override
  public String id() {
    return MUTATOR_ID;
  }

  @Override
  public PolarisBaseEntity apply(PolarisBaseEntity entity) {
    if (!(entity instanceof CatalogEntity catalogEntity) || !catalogEntity.isPassthroughFacade()) {
      return entity;
    }

    ConnectionConfigInfoDpo connectionConfigInfoDpo = catalogEntity.getConnectionConfigInfoDpo();
    AuthenticationParametersDpo authenticationParameters =
        connectionConfigInfoDpo.getAuthenticationParameters();
    if (authenticationParameters.getAuthenticationType() == AuthenticationType.SIGV4) {
      CatalogEntity.Builder builder = new CatalogEntity.Builder(catalogEntity);
      ConnectionConfigInfoDpo injectedConnectionConfigInfoDpo =
          connectionConfigInfoDpo.withServiceIdentity(
              serviceIdentityRegistry.assignServiceIdentity(ServiceIdentityType.AWS_IAM));
      builder.setConnectionConfigInfoDpo(injectedConnectionConfigInfoDpo);
      builder.setEntityVersion(entity.getEntityVersion() + 1);
      return builder.build();
    }
    return entity;
  }
}
