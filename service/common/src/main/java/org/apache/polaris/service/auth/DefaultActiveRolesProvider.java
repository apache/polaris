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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.LoadGrantsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the {@link ActiveRolesProvider} looks up the grant records for a
 * principal to determine roles that are available. {@link
 * AuthenticatedPolarisPrincipal#getActivatedPrincipalRoleNames()} is used to determine which of the
 * available roles are active for this request.
 */
@RequestScoped
@Identifier("default")
public class DefaultActiveRolesProvider implements ActiveRolesProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultActiveRolesProvider.class);

  @Inject CallContext callContext;
  @Inject MetaStoreManagerFactory metaStoreManagerFactory;

  @Override
  public Set<String> getActiveRoles(AuthenticatedPolarisPrincipal principal) {
    List<PrincipalRoleEntity> activeRoles =
        loadActivePrincipalRoles(
            principal,
            metaStoreManagerFactory.getOrCreateMetaStoreManager(callContext.getRealmContext()));
    return activeRoles.stream().map(PrincipalRoleEntity::getName).collect(Collectors.toSet());
  }

  protected List<PrincipalRoleEntity> loadActivePrincipalRoles(
      AuthenticatedPolarisPrincipal principal, PolarisMetaStoreManager metaStoreManager) {
    PolarisCallContext polarisContext = callContext.getPolarisCallContext();
    LoadGrantsResult principalGrantResults =
        metaStoreManager.loadGrantsToGrantee(polarisContext, principal.getPrincipalEntity());
    polarisContext
        .getDiagServices()
        .check(
            principalGrantResults.isSuccess(),
            "Failed to resolve principal roles for principal name={} id={}",
            principal.getName(),
            principal.getPrincipalEntity().getId());
    if (!principalGrantResults.isSuccess()) {
      LOGGER.warn(
          "Failed to resolve principal roles for principal name={} id={}",
          principal.getName(),
          principal.getPrincipalEntity().getId());
      throw new NotAuthorizedException("Unable to authenticate");
    }

    // By convention, an empty set means all roles are activated
    Set<String> tokenRoles = principal.getActivatedPrincipalRoleNames();
    Predicate<PrincipalRoleEntity> includeRoleFilter =
        principal.allRoles() ? r -> true : r -> tokenRoles.contains(r.getName());
    List<PrincipalRoleEntity> activeRoles =
        principalGrantResults.getGrantRecords().stream()
            .map(
                gr ->
                    metaStoreManager.loadEntity(
                        polarisContext,
                        gr.getSecurableCatalogId(),
                        gr.getSecurableId(),
                        PolarisEntityType.PRINCIPAL_ROLE))
            .filter(EntityResult::isSuccess)
            .map(EntityResult::getEntity)
            .map(PrincipalRoleEntity::of)
            .filter(includeRoleFilter)
            .toList();
    if (activeRoles.size() != principalGrantResults.getGrantRecords().size()) {
      LOGGER
          .atWarn()
          .addKeyValue("principal", principal.getName())
          .addKeyValue("tokenRoles", principal.allRoles() ? "ALL" : tokenRoles)
          .addKeyValue("roles", activeRoles)
          .log("Some principal roles were not found in the principal's grants");
    }
    return activeRoles;
  }
}
