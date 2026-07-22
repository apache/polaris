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
import com.google.common.collect.ImmutableMap;
import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.ServiceUnavailableException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.LoadGrantsResult;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.jspecify.annotations.Nullable;
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

  @Inject PolarisMetaStoreManager metaStoreManager;
  @Inject CallContext callContext;
  @Inject PolarisDiagnostics diagnostics;

  @Override
  public PolarisPrincipal authenticate(SecurityIdentity identity) {

    PolarisCredential credentials = extractPolarisCredential(identity);
    LOGGER.debug("Resolving principal for credentials: {}", credentials);

    PrincipalEntity principalEntity = resolvePrincipalEntity(credentials);
    PrincipalRoleSelection principalRoles = resolvePrincipalRoles(credentials, principalEntity);
    Map<String, Object> principalAttributes =
        resolvePrincipalAttributes(identity, principalEntity, principalRoles.allRolesRequested());
    PolarisPrincipal polarisPrincipal =
        PolarisPrincipal.of(principalEntity.getName(), principalAttributes, principalRoles.roles());

    LOGGER.debug("Resolved principal: {}", polarisPrincipal);
    return polarisPrincipal;
  }

  private static PolarisCredential extractPolarisCredential(SecurityIdentity identity) {
    PolarisCredential credential = identity.getCredential(PolarisCredential.class);
    if (credential == null) {
      throw new AuthenticationFailedException("No Polaris credential available");
    }
    return credential;
  }

  /**
   * Resolves the principal entity based on the provided credentials.
   *
   * <p>This method attempts to load the principal entity using either the principal ID or the
   * principal name from the credentials. If neither is available, nor if the principal entity can
   * be found, it throws a {@link AuthenticationFailedException}.
   */
  protected PrincipalEntity resolvePrincipalEntity(PolarisCredential credentials) {

    PrincipalEntity principal = null;
    try {
      // If the principal id is present, prefer to use it to load the principal entity,
      // otherwise, use the principal name to load the entity.
      if (credentials.getPrincipalId() != null && credentials.getPrincipalId() > 0) {
        principal =
            metaStoreManager
                .findPrincipalById(
                    callContext.getPolarisCallContext(), credentials.getPrincipalId())
                .orElse(null);
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
          .log("Unable to resolve principal entity from credentials");
      throw new ServiceUnavailableException("Unable to fetch principal entity");
    }

    if (principal == null || principal.getType() != PolarisEntityType.PRINCIPAL) {
      LOGGER.warn("Failed to resolve principal from credentials={}", credentials);
      throw new AuthenticationFailedException("Unable to authenticate");
    }

    return principal;
  }

  protected Map<String, Object> resolvePrincipalAttributes(
      SecurityIdentity identity, PrincipalEntity principalEntity, boolean allRolesRequested) {
    ImmutableMap.Builder<String, Object> principalAttributes =
        ImmutableMap.<String, Object>builder()
            .putAll(identity.getAttributes())
            .put(PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, principalEntity)
            .put(PolarisPrincipal.PRINCIPAL_ROLE_ALL_ATTRIBUTE_KEY, allRolesRequested);
    if (identity.getPrincipal() instanceof JsonWebToken jwt) {
      principalAttributes.put(PolarisPrincipal.JWT_ATTRIBUTE_KEY, jwt.getRawToken());
    }
    return principalAttributes.buildKeepingLast();
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
  protected PrincipalRoleSelection resolvePrincipalRoles(
      PolarisCredential credentials, PrincipalEntity principal) {

    PrincipalRoleSelection requestedRoles = extractRequestedRoles(credentials);
    LoadGrantsResult loadGrantsResult = loadPrincipalGrants(principal);

    Map<Long, PolarisBaseEntity> entitiesById = loadGrantsResult.getEntitiesAsMap();

    Set<String> activeRoles =
        loadGrantsResult.getGrantRecords().stream()
            .map(gr -> loadSecurableEntity(gr, entitiesById))
            .filter(Objects::nonNull)
            .filter(entity -> entity.getType() == PolarisEntityType.PRINCIPAL_ROLE)
            .map(PrincipalRoleEntity::of)
            .map(PrincipalRoleEntity::getName)
            .filter(
                role -> requestedRoles.allRolesRequested() || requestedRoles.roles().contains(role))
            .collect(Collectors.toSet());

    if (!requestedRoles.allRolesRequested() && !activeRoles.containsAll(requestedRoles.roles())) {
      LOGGER
          .atWarn()
          .addKeyValue("principal", principal.getName())
          .addKeyValue("credentials", credentials)
          .addKeyValue("roles", activeRoles)
          .log("Some principal roles were not found in the principal's grants");
      throw new AuthenticationFailedException("Unable to authenticate");
    }

    return new PrincipalRoleSelection(activeRoles, requestedRoles.allRolesRequested());
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
  protected PrincipalRoleSelection extractRequestedRoles(PolarisCredential credentials) {
    Set<String> credentialsRoles = credentials.getPrincipalRoles();
    if (credentialsRoles.contains(PRINCIPAL_ROLE_ALL)) {
      return new PrincipalRoleSelection(Set.of(), true);
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
    return new PrincipalRoleSelection(
        credentialsRoles.stream()
            .filter(s -> s.startsWith(PRINCIPAL_ROLE_PREFIX))
            .map(s -> s.substring(PRINCIPAL_ROLE_PREFIX.length()))
            .collect(Collectors.toSet()),
        false);
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
      throw new AuthenticationFailedException("Unable to authenticate");
    }
    return principalGrantResults;
  }

  /**
   * Resolves the securable entity for a grant record, using preloaded entities when available and
   * falling back to {@link PolarisMetaStoreManager#loadEntity} only when the metastore did not
   * populate {@link LoadGrantsResult#getEntities()}.
   */
  private @Nullable PolarisBaseEntity loadSecurableEntity(
      PolarisGrantRecord grant, @Nullable Map<Long, PolarisBaseEntity> entitiesById) {
    if (entitiesById != null) {
      return entitiesById.get(grant.getSecurableId());
    }
    return metaStoreManager
        .loadEntity(
            callContext.getPolarisCallContext(),
            grant.getSecurableCatalogId(),
            grant.getSecurableId(),
            PolarisEntityType.PRINCIPAL_ROLE)
        .getEntity();
  }

  protected record PrincipalRoleSelection(Set<String> roles, boolean allRolesRequested) {}
}
