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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.StructuredLogKeys;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.exceptions.PolarisServiceUnavailableException;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.LoadGrantsResult;
import org.apache.polaris.service.auth.internal.broker.InternalPolarisToken;
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
  @Inject AuthenticationRealmConfiguration authConfig;

  @Override
  public PolarisPrincipal authenticate(SecurityIdentity identity) {

    PolarisCredential credentials = extractPolarisCredential(identity);
    LOGGER.debug("Resolving principal for credentials: {}", credentials);

    var entity = resolvePrincipalEntity(credentials);
    var roleSelection = resolvePrincipalRoles(credentials, entity);
    var attributes =
        resolvePrincipalAttributes(identity, entity, roleSelection.allRolesRequested());

    var principalName = entity != null ? entity.getName() : credentials.getPrincipalName();
    var polarisPrincipal = PolarisPrincipal.of(principalName, attributes, roleSelection.roles());

    LOGGER.debug("Resolved internal principal: {}", polarisPrincipal);
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
   * <p>When the credentials were issued by Polaris itself ({@link InternalPolarisToken}) or when
   * the principal mode is {@link PrincipalMode#INTERNAL}, this method attempts to load the
   * principal entity using either the principal ID or the principal name from the credentials. If
   * neither is available, nor if the principal entity can be found, it throws a {@link
   * AuthenticationFailedException}.
   *
   * <p>When the credentials were issued by an external IDP and the principal mode is {@link
   * PrincipalMode#EXTERNAL}, this method does not attempt to load the principal entity from the
   * database and returns {@code null} instead. It throws {@link AuthenticationFailedException} if
   * the principal name is not available in the credentials.
   */
  @Nullable
  protected PrincipalEntity resolvePrincipalEntity(PolarisCredential credentials) {

    PrincipalEntity entity = null;

    boolean shouldHaveBackingEntity =
        credentials instanceof InternalPolarisToken
            || authConfig.principalMode() == PrincipalMode.INTERNAL;

    if (shouldHaveBackingEntity) {

      try {
        // If the principal id is present, prefer to use it to load the principal entity,
        // otherwise, use the principal name to load the entity.
        if (credentials.getPrincipalId() != null && credentials.getPrincipalId() > 0) {
          entity =
              metaStoreManager
                  .findPrincipalById(
                      callContext.getPolarisCallContext(), credentials.getPrincipalId())
                  .orElse(null);
        } else if (credentials.getPrincipalName() != null) {
          entity =
              metaStoreManager
                  .findPrincipalByName(
                      callContext.getPolarisCallContext(), credentials.getPrincipalName())
                  .orElse(null);
        }
      } catch (Exception e) {
        throw metaStoreUnavailable(
            e,
            "Unable to resolve principal entity from credentials, principalName={} principalId={}",
        credentials.getPrincipalName(),
          credentials.getPrincipalId());
      }

      if (entity == null || entity.getType() != PolarisEntityType.PRINCIPAL) {
        LOGGER.warn("Failed to resolve principal from credentials={}", credentials);
        throw new AuthenticationFailedException("Unable to authenticate");
      }

    } else {

      String principalName = credentials.getPrincipalName();
      if (principalName == null) {
        throw new AuthenticationFailedException("Invalid credential");
      }
    }

    return entity;
  }

  protected Map<String, Object> resolvePrincipalAttributes(
      SecurityIdentity identity,
      @Nullable PrincipalEntity principalEntity,
      boolean allRolesRequested) {
    // Do not merge the security identity's attributes into the principal attributes:
    // these must stay separate.
    ImmutableMap.Builder<String, Object> principalAttributes = ImmutableMap.builder();
    if (identity.getPrincipal() instanceof JsonWebToken jwt) {
      principalAttributes.put(PolarisPrincipal.JWT_ATTRIBUTE_KEY, jwt.getRawToken());
    }
    if (principalEntity != null) {
      principalAttributes
          .put(PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, principalEntity)
          .put(PolarisPrincipal.PRINCIPAL_ROLE_ALL_ATTRIBUTE_KEY, allRolesRequested);
    }
    return principalAttributes.build();
  }

  /**
   * Resolves the roles for the given principal based on the provided credentials and entity.
   *
   * <p>When no entity is available, the method assumes that the principal is not backed by an
   * entity in the Polaris metastore and resolves the roles from the credentials directly. In this
   * case, no special treatment is applied to role names, and the pseudo-role {@link
   * #PRINCIPAL_ROLE_ALL} is ignored.
   *
   * <p>When an entity is available, this method checks the credentials for requested roles and
   * loads the principal's grants to determine which roles are currently active for the principal.
   * The returned set of roles will include only those roles that the principal has been granted and
   * that match the requested roles from the credentials. If the credentials contain the pseudo-role
   * {@link #PRINCIPAL_ROLE_ALL}, it indicates that the principal is requesting all roles they have
   * been granted in the system, and all such roles will be included in the returned set.
   */
  protected PrincipalRoleSelection resolvePrincipalRoles(
      PolarisCredential credentials, @Nullable PrincipalEntity principal) {

    if (principal == null) {
      return new PrincipalRoleSelection(credentials.getPrincipalRoles(), false);
    }

    PrincipalRoleSelection requestedRoles = extractRequestedRoles(credentials);
    LoadGrantsResult loadGrantsResult = loadPrincipalGrants(principal);

    Map<Long, PolarisBaseEntity> entitiesById = loadGrantsResult.getEntitiesAsMap();

    Set<String> activeRoles =
        loadGrantsResult.getGrantRecords().stream()
            .map(gr -> loadSecurableEntity(gr, entitiesById, principal))
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
          .addKeyValue(StructuredLogKeys.PRINCIPAL, principal.getName())
          .addKeyValue(StructuredLogKeys.CREDENTIALS, credentials)
          .addKeyValue(StructuredLogKeys.ROLES, activeRoles)
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
          .addKeyValue(StructuredLogKeys.CREDENTIALS, credentials)
          .addKeyValue(StructuredLogKeys.ROLES, credentialsRoles)
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
    LoadGrantsResult principalGrantResults;
    try {
      principalGrantResults = metaStoreManager.loadGrantsToGrantee(polarisContext, principal);
    } catch (Exception e) {
      throw metaStoreUnavailable(
          e,
          "Unable to load grants, principalName={} principalId={}",
          principal.getName(),
          principal.getId());
    }
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
   * populate {@link LoadGrantsResult#getEntities()}. The principal identifies the failing request
   * if that fallback hits a metastore failure.
   */
  private @Nullable PolarisBaseEntity loadSecurableEntity(
      PolarisGrantRecord grant,
      @Nullable Map<Long, PolarisBaseEntity> entitiesById,
      PrincipalEntity principal) {
    if (entitiesById != null) {
      return entitiesById.get(grant.getSecurableId());
    }
    PolarisCallContext polarisContext = callContext.getPolarisCallContext();
    try {
      return metaStoreManager
          .loadEntity(
              polarisContext,
              grant.getSecurableCatalogId(),
              grant.getSecurableId(),
              PolarisEntityType.PRINCIPAL_ROLE)
          .getEntity();
    } catch (Exception e) {
      throw metaStoreUnavailable(
          e,
          "Unable to load securable entity for grant, principalName={} principalId={} "
              + "securableCatalogId={} securableId={}",
          principal.getName(),
          principal.getId(),
          grant.getSecurableCatalogId(),
          grant.getSecurableId());
    }
  }

  /**
   * Logs a metastore failure raised during authentication and returns the exception to throw, so
   * that a failing backend is reported as a transient condition instead of an internal error.
   *
   * <p>The caller is not authenticated yet at this point, so the response carries a fixed message
   * and only the log names the lookup that failed. The two are matched through the request id,
   * returned by default as {@code X-Request-ID} and printed by the default log format as {@code
   * requestId}.
   *
   * @param cause the metastore failure
   * @param logMessage the log message, with SLF4J placeholders for {@code logArgs}
   * @param logArgs the values for the placeholders in {@code logMessage}
   */
  private static PolarisServiceUnavailableException metaStoreUnavailable(
      Exception cause, String logMessage, Object... logArgs) {
    LOGGER
        .atError()
        .addKeyValue(StructuredLogKeys.ERR_MSG, cause.getMessage())
        .addKeyValue(StructuredLogKeys.STACK_TRACE, Throwables.getStackTraceAsString(cause))
        .log(logMessage, logArgs);
    return new PolarisServiceUnavailableException(0, "Service unavailable");
  }

  protected record PrincipalRoleSelection(Set<String> roles, boolean allRolesRequested) {}
}
