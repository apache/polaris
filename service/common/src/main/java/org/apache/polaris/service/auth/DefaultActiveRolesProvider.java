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

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisGrantManager;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the {@link ActiveRolesProvider} looks up the grant records for a
 * principal to determine roles that are available. {@link
 * AuthenticatedPolarisPrincipal#getActivatedPrincipalRoleNames()} is used to determine which of the
 * available roles are active for this request.
 */
public class DefaultActiveRolesProvider implements ActiveRolesProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultActiveRolesProvider.class);
  @Inject Provider<RealmContext> realmContextProvider;
  @Inject MetaStoreManagerFactory metaStoreManagerFactory;
  @Inject Provider<PolarisGrantManager> polarisGrantManagerProvider;

  @Override
  public Set<String> getActiveRoles(AuthenticatedPolarisPrincipal principal) {
    return loadActivePrincipalRoles(
        principal.getActivatedPrincipalRoleNames(),
        principal.getPrincipalEntity(),
        metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContextProvider.get()));
  }

  private Set<String> loadActivePrincipalRoles(
      Set<String> tokenRoles, PolarisEntity principal, PolarisMetaStoreManager metaStoreManager) {
    PolarisCallContext polarisContext = CallContext.getCurrentContext().getPolarisCallContext();
    PolarisGrantManager.LoadGrantsResult principalGrantResults =
        polarisGrantManagerProvider.get().loadGrantsToGrantee(polarisContext, principal);
    polarisContext
        .getDiagServices()
        .check(
            principalGrantResults.isSuccess(),
            "Failed to resolve principal roles for principal name={} id={}",
            principal.getName(),
            principal.getId());
    if (!principalGrantResults.isSuccess()) {
      LOGGER.warn(
          "Failed to resolve principal roles for principal name={} id={}",
          principal.getName(),
          principal.getId());
      return Set.of();
    }
    boolean allRoles = tokenRoles.contains(BasePolarisAuthenticator.PRINCIPAL_ROLE_ALL);
    Predicate<String> includeRoleFilter = allRoles ? r -> true : r -> tokenRoles.contains(r);
    Set<String> activeRoles =
        principalGrantResults.getGrantRecords().stream()
            .map(
                gr ->
                    metaStoreManager.loadEntity(
                        polarisContext, gr.getSecurableCatalogId(), gr.getSecurableId()))
            .filter(PolarisMetaStoreManager.EntityResult::isSuccess)
            .map(PolarisMetaStoreManager.EntityResult::getEntity)
            .map(PolarisBaseEntity::getName)
            .filter(includeRoleFilter)
            .collect(Collectors.toSet());
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
