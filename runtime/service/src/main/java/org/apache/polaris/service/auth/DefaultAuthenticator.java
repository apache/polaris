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
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.LoadGrantsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default {@link Authenticator}.
 *
 * <p>This implementation resolves the principal entity based on the provided credentials, extracts
 * the requested roles, and loads the principal's grants to determine which roles are currently
 * active for the principal.
 *
 * <p>It supports a pseudo-role {@value #PRINCIPAL_ROLE_ALL} that allows requesting all roles the
 * principal has been granted in the system.
 *
 * <p><strong>This authenticator is used in both internal and external authentication scenarios. For
 * now, it does not support federated principals that are not managed by Polaris.</strong>
 */
@RequestScoped
@Identifier("default")
public class DefaultAuthenticator implements Authenticator {

  /**
   * The pseudo-role that resolves to all roles the principal has been granted in the system.
   *
   * <p>This role is not a valid role and is not stored in the database; it may be used in incoming
   * credentials to explicitly indicate that the principal is requesting that all its roles be
   * activated upon authentication, without needing to specify each role individually.
   */
  public static final String PRINCIPAL_ROLE_ALL = "PRINCIPAL_ROLE:ALL";

  /**
   * The prefix for the roles in incoming credentials that are used to indicate that the principal
   * is requesting that specific roles be activated upon authentication.
   *
   * <p>If the incoming credentials contain roles prefixed with this string, the authenticator will
   * attempt to resolve those roles from the principal's grants. Only those roles will be activated.
   *
   * <p>If the incoming credentials contain roles that do not match this prefix, they will be
   * ignored during the authentication process. If necessary, use {@code PrincipalRolesMapper} to
   * convert roles from the identity to Polaris-specific roles.
   */
  public static final String PRINCIPAL_ROLE_PREFIX = "PRINCIPAL_ROLE:";

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultAuthenticator.class);

  private static final Set<String> ALL_ROLES_REQUESTED = Set.of();

  @Inject PolarisMetaStoreManager metaStoreManager;
  @Inject CallContext callContext;
  @Inject PolarisDiagnostics diagnostics;

  @Override
  public PolarisPrincipal authenticate(PolarisCredential credentials) {

    LOGGER.debug("Resolving principal for credentials: {}", credentials);

    PrincipalEntity principalEntity = resolvePrincipalEntity(credentials);
    Set<String> principalRoles = resolvePrincipalRoles(credentials, principalEntity);
    PolarisPrincipal polarisPrincipal = PolarisPrincipal.of(principalEntity, principalRoles);

    LOGGER.debug("Resolved principal: {}", polarisPrincipal);
    return polarisPrincipal;
  }

  /**
   * Resolves the principal entity based on the provided credentials.
   *
   * <p>This method attempts to load the principal entity using either the principal ID or the
   * principal name from the credentials. If neither is available, nor if the principal entity can
   * be found, it throws a {@link NotAuthorizedException}.
   */
  protected PrincipalEntity resolvePrincipalEntity(PolarisCredential credentials) {

    PrincipalEntity principal = null;
    try {
      // If the principal id is present, prefer to use it to load the principal entity,
      // otherwise, use the principal name to load the entity.
      if (credentials.getPrincipalId() != null && credentials.getPrincipalId() > 0) {
        principal =
            PrincipalEntity.of(
                metaStoreManager
                    .loadEntity(
                        callContext.getPolarisCallContext(),
                        0L,
                        credentials.getPrincipalId(),
                        PolarisEntityType.PRINCIPAL)
                    .getEntity());
      } else if (credentials.getPrincipalName() != null) {
        principal =
            metaStoreManager
                .findPrincipalByName(
                    callContext.getPolarisCallContext(), credentials.getPrincipalName())
                .orElse(null);
      }
    } catch (Exception e) {
      LOGGER
          .atError()
          .addKeyValue("errMsg", e.getMessage())
          .addKeyValue("stackTrace", Throwables.getStackTraceAsString(e))
          .log("Unable to authenticate user with token");
      throw new ServiceFailureException("Unable to fetch principal entity");
    }

    if (principal == null || principal.getType() != PolarisEntityType.PRINCIPAL) {
      LOGGER.warn("Failed to resolve principal from credentials={}", credentials);
      throw new NotAuthorizedException("Unable to authenticate");
    }

    return principal;
  }

  /**
   * Resolves the roles for the given principal based on the provided credentials.
   *
   * <p>This method checks the credentials for requested roles and loads the principal's grants to
   * determine which roles are currently active for the principal.
   *
   * <p>The returned set of roles will include only those roles that the principal has been granted
   * and that match the requested roles from the credentials. If the credentials contain the
   * pseudo-role {@link #PRINCIPAL_ROLE_ALL}, it indicates that the principal is requesting all
   * roles they have been granted in the system, and all such roles will be included in the returned
   * set.
   */
  protected Set<String> resolvePrincipalRoles(
      PolarisCredential credentials, PrincipalEntity principal) {

    Set<String> requestedRoles = extractRequestedRoles(credentials);
    LoadGrantsResult loadGrantsResult = loadPrincipalGrants(principal);

    Predicate<String> includeRoleFilter =
        requestedRoles == ALL_ROLES_REQUESTED ? role -> true : requestedRoles::contains;

    Set<String> activeRoles =
        loadGrantsResult.getGrantRecords().stream()
            .map(
                gr ->
                    metaStoreManager.loadEntity(
                        callContext.getPolarisCallContext(),
                        gr.getSecurableCatalogId(),
                        gr.getSecurableId(),
                        PolarisEntityType.PRINCIPAL_ROLE))
            .filter(EntityResult::isSuccess)
            .map(EntityResult::getEntity)
            .map(PrincipalRoleEntity::of)
            .map(PrincipalRoleEntity::getName)
            .filter(includeRoleFilter)
            .collect(Collectors.toSet());

    if (requestedRoles != ALL_ROLES_REQUESTED && !activeRoles.containsAll(requestedRoles)) {
      LOGGER
          .atWarn()
          .addKeyValue("principal", principal.getName())
          .addKeyValue("credentials", credentials)
          .addKeyValue("roles", activeRoles)
          .log("Some principal roles were not found in the principal's grants");
    }

    return activeRoles;
  }

  /**
   * Extracts the requested roles from the credentials.
   *
   * <p>If the credentials contain the pseudo-role {@link #PRINCIPAL_ROLE_ALL}, it indicates that
   * the principal is requesting all roles they have been granted in the system.
   *
   * <p>Otherwise, it filters the roles that start with the {@link #PRINCIPAL_ROLE_PREFIX} and
   * returns the set of roles without the prefix.
   */
  protected Set<String> extractRequestedRoles(PolarisCredential credentials) {
    Set<String> credentialsRoles = credentials.getPrincipalRoles();
    if (credentialsRoles.contains(PRINCIPAL_ROLE_ALL)) {
      return ALL_ROLES_REQUESTED;
    }
    if (credentialsRoles.stream().anyMatch(s -> !s.startsWith(PRINCIPAL_ROLE_PREFIX))) {
      LOGGER
          .atWarn()
          .addKeyValue("credentials", credentials)
          .addKeyValue("roles", credentialsRoles)
          .log(
              "Credentials contain roles that do not start with expected prefix '{}'. "
                  + "These roles will be ignored during authentication.",
              PRINCIPAL_ROLE_PREFIX);
    }
    return credentialsRoles.stream()
        .filter(s -> s.startsWith(PRINCIPAL_ROLE_PREFIX))
        .map(s -> s.substring(PRINCIPAL_ROLE_PREFIX.length()))
        .collect(Collectors.toSet());
  }

  /**
   * Loads the grants for the given principal.
   *
   * <p>This method retrieves the grants that the principal has been granted in the system, which
   * will be used to determine the active roles for the principal.
   */
  protected LoadGrantsResult loadPrincipalGrants(PrincipalEntity principal) {
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
    return principalGrantResults;
  }
}
