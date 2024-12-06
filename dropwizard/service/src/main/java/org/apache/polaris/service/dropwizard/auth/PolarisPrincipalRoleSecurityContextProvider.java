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
package org.apache.polaris.service.dropwizard.auth;

import jakarta.annotation.Priority;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.security.Principal;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisGrantManager;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.auth.BasePolarisAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Priority(Priorities.AUTHENTICATION + 1)
public class PolarisPrincipalRoleSecurityContextProvider implements ContainerRequestFilter {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PolarisPrincipalRoleSecurityContextProvider.class);
  @Inject Provider<SecurityContext> securityContextProvider;
  @Inject MetaStoreManagerFactory metaStoreManager;
  @Inject Provider<PolarisGrantManager> polarisGrantManagerProvider;

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    AuthenticatedPolarisPrincipal polarisPrincipal =
        (AuthenticatedPolarisPrincipal) securityContextProvider.get().getUserPrincipal();
    if (polarisPrincipal == null) {
      return;
    }
    SecurityContext securityContext =
        createSecurityContext(requestContext.getSecurityContext(), polarisPrincipal);
    requestContext.setSecurityContext(securityContext);
  }

  public SecurityContext createSecurityContext(
      SecurityContext ctx, AuthenticatedPolarisPrincipal principal) {
    RealmContext realmContext = CallContext.getCurrentContext().getRealmContext();
    List<PrincipalRoleEntity> activeRoles =
        loadActivePrincipalRoles(
            principal.getActivatedPrincipalRoleNames(),
            principal.getPrincipalEntity(),
            metaStoreManager.getOrCreateMetaStoreManager(realmContext));
    Set<String> validRoleNames =
        activeRoles.stream().map(PrincipalRoleEntity::getName).collect(Collectors.toSet());
    return new SecurityContext() {
      @Override
      public Principal getUserPrincipal() {
        return principal;
      }

      @Override
      public boolean isUserInRole(String role) {
        return validRoleNames.contains(role);
      }

      @Override
      public boolean isSecure() {
        return ctx.isSecure();
      }

      @Override
      public String getAuthenticationScheme() {
        return ctx.getAuthenticationScheme();
      }
    };
  }

  protected List<PrincipalRoleEntity> loadActivePrincipalRoles(
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
                        polarisContext, gr.getSecurableCatalogId(), gr.getSecurableId()))
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
