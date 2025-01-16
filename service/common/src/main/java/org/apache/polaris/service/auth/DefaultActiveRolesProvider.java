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
package org.apache.polaris.service.auth;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisGrantManager;
import org.apache.polaris.core.context.RealmId;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the {@link ActiveRolesProvider} looks up the grant records for a
 * principal to determine roles that are available. {@link
 * AuthenticatedPolarisPrincipal#getActivatedPrincipalRoleNames()} is used to determine which of the
 * available roles are active for this request.
 */
@RequestScoped
public class DefaultActiveRolesProvider implements ActiveRolesProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultActiveRolesProvider.class);

  @Inject RealmId realmId;
  @Inject MetaStoreManagerFactory metaStoreManagerFactory;
  @Inject PolarisDiagnostics diagnostics;

  @Override
  public Set<String> getActiveRoles(AuthenticatedPolarisPrincipal principal) {
    List<PrincipalRoleEntity> activeRoles =
        loadActivePrincipalRoles(
            principal.getActivatedPrincipalRoleNames(),
            principal.getPrincipalEntity(),
            metaStoreManagerFactory.getOrCreateMetaStoreManager(realmId),
            metaStoreManagerFactory.getOrCreateSessionSupplier(realmId).get());
    return activeRoles.stream().map(PrincipalRoleEntity::getName).collect(Collectors.toSet());
  }

  protected List<PrincipalRoleEntity> loadActivePrincipalRoles(
      Set<String> tokenRoles,
      PolarisEntity principal,
      PolarisMetaStoreManager metaStoreManager,
      PolarisMetaStoreSession metaStoreSession) {
    PolarisGrantManager.LoadGrantsResult principalGrantResults =
        metaStoreManager.loadGrantsToGrantee(metaStoreSession, principal);
    diagnostics.check(
        principalGrantResults.isSuccess(),
        "Failed to resolve principal roles for principal name={} id={}",
        principal.getName(),
        principal.getId());
    if (!principalGrantResults.isSuccess()) {
      LOGGER.warn(
          "Failed to resolve principal roles for principal name={} id={}",
          principal.getName(),
          principal.getId());
      throw new NotAuthorizedException("Unable to authenticate");
    }
    boolean allRoles = tokenRoles.contains(BasePolarisAuthenticator.PRINCIPAL_ROLE_ALL);
    Predicate<PrincipalRoleEntity> includeRoleFilter =
        allRoles ? r -> true : r -> tokenRoles.contains(r.getName());
    List<PrincipalRoleEntity> activeRoles =
        principalGrantResults.getGrantRecords().stream()
            .map(
                gr ->
                    metaStoreManager.loadEntity(
                        metaStoreSession, gr.getSecurableCatalogId(), gr.getSecurableId()))
            .filter(PolarisMetaStoreManager.EntityResult::isSuccess)
            .map(PolarisMetaStoreManager.EntityResult::getEntity)
            .map(PrincipalRoleEntity::of)
            .filter(includeRoleFilter)
            .toList();
    if (activeRoles.size() != principalGrantResults.getGrantRecords().size()) {
      LOGGER
          .atWarn()
          .addKeyValue("principal", principal.getName())
          .addKeyValue("scopes", tokenRoles)
          .addKeyValue("roles", activeRoles)
          .log("Some principal roles were not found in the principal's grants");
    }
    return activeRoles;
  }
}
