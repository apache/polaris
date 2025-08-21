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

import com.google.common.base.Throwables;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntity;
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
 * principal to determine roles that are available. {@link PolarisPrincipal#getRoles()} is used to
 * determine which of the available roles are active for this request.
 */
@RequestScoped
@Identifier("default")
@Deprecated
public class DefaultActiveRolesProvider implements ActiveRolesProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultActiveRolesProvider.class);

  @Inject PolarisDiagnostics diagnostics;
  @Inject CallContext callContext;
  @Inject MetaStoreManagerFactory metaStoreManagerFactory;

  @Override
  public Set<String> getActiveRoles(PolarisPrincipal principal) {
    if (!(principal instanceof PersistedPolarisPrincipal persistedPolarisPrincipal)) {
      LOGGER.error(
          "Expected an PersistedPolarisPrincipal, but got {}: {}",
          principal.getClass().getName(),
          Throwables.getStackTraceAsString(new ServiceFailureException("Invalid principal type")));
      throw new NotAuthorizedException("Unable to authenticate");
    }
    List<PrincipalRoleEntity> activeRoles =
        loadActivePrincipalRoles(
            principal.getRoles(),
            persistedPolarisPrincipal.getEntity(),
            metaStoreManagerFactory.getOrCreateMetaStoreManager(callContext.getRealmContext()));
    return activeRoles.stream().map(PrincipalRoleEntity::getName).collect(Collectors.toSet());
  }

  protected List<PrincipalRoleEntity> loadActivePrincipalRoles(
      Set<String> tokenRoles, PolarisEntity principal, PolarisMetaStoreManager metaStoreManager) {
    PolarisCallContext polarisContext = callContext.getPolarisCallContext();
    LoadGrantsResult principalGrantResults =
        metaStoreManager.loadGrantsToGrantee(polarisContext, principal);
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

    // FIXME how to distinguish allRoles from no roles at all?
    boolean allRoles = tokenRoles.isEmpty();
    Predicate<PrincipalRoleEntity> includeRoleFilter =
        allRoles ? r -> true : r -> tokenRoles.contains(r.getName());
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
          .addKeyValue("scopes", tokenRoles)
          .addKeyValue("roles", activeRoles)
          .log("Some principal roles were not found in the principal's grants");
    }
    return activeRoles;
  }
}
