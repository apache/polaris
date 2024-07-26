package io.polaris.service.auth;

import io.polaris.core.PolarisCallContext;
import io.polaris.core.auth.AuthenticatedPolarisPrincipal;
import io.polaris.core.context.CallContext;
import io.polaris.core.context.RealmContext;
import io.polaris.core.entity.PolarisEntity;
import io.polaris.core.entity.PolarisEntitySubType;
import io.polaris.core.entity.PolarisEntityType;
import io.polaris.core.entity.PrincipalEntity;
import io.polaris.core.persistence.PolarisMetaStoreManager;
import io.polaris.service.config.RealmEntityManagerFactory;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base implementation of {@link DiscoverableAuthenticator} constructs a {@link
 * AuthenticatedPolarisPrincipal} from the token parsed by subclasses. The {@link
 * AuthenticatedPolarisPrincipal} is read from the {@link PolarisMetaStoreManager} for the current
 * {@link RealmContext}. If the token defines a non-empty set of scopes, only the principal roles
 * specified in the scopes will be active for the current principal. Only the grants assigned to
 * these roles will be active in the current request.
 */
public abstract class BasePolarisAuthenticator
    implements DiscoverableAuthenticator<String, AuthenticatedPolarisPrincipal> {
  public static final String PRINCIPAL_ROLE_ALL = "PRINCIPAL_ROLE:ALL";
  public static final String PRINCIPAL_ROLE_PREFIX = "PRINCIPAL_ROLE:";
  private static final Logger LOGGER = LoggerFactory.getLogger(BasePolarisAuthenticator.class);

  protected RealmEntityManagerFactory entityManagerFactory;

  public void setEntityManagerFactory(RealmEntityManagerFactory entityManagerFactory) {
    this.entityManagerFactory = entityManagerFactory;
  }

  public PolarisCallContext getCurrentPolarisContext() {
    return CallContext.getCurrentContext().getPolarisCallContext();
  }

  protected Optional<AuthenticatedPolarisPrincipal> getPrincipal(DecodedToken tokenInfo) {
    LOGGER.debug("Resolving principal for tokenInfo client_id={}", tokenInfo.getClientId());
    RealmContext realmContext = CallContext.getCurrentContext().getRealmContext();
    PolarisMetaStoreManager metaStoreManager =
        entityManagerFactory.getOrCreateEntityManager(realmContext).getMetaStoreManager();
    PolarisEntity principal;
    try {
      principal =
          tokenInfo.getPrincipalId() > 0
              ? PolarisEntity.of(
                  metaStoreManager.loadEntity(
                      getCurrentPolarisContext(), 0L, tokenInfo.getPrincipalId()))
              : PolarisEntity.of(
                  metaStoreManager.readEntityByName(
                      getCurrentPolarisContext(),
                      null,
                      PolarisEntityType.PRINCIPAL,
                      PolarisEntitySubType.NULL_SUBTYPE,
                      tokenInfo.getSub()));
    } catch (Exception e) {
      LoggerFactory.getLogger(BasePolarisAuthenticator.class)
          .atError()
          .addKeyValue("errMsg", e.getMessage())
          .addKeyValue("stackTrace", ExceptionUtils.getStackTrace(e))
          .log("Unable to authenticate user with token");
      throw new NotAuthorizedException("Unable to authenticate");
    }
    if (principal == null) {
      LOGGER.warn(
          "Failed to resolve principal from tokenInfo client_id={}", tokenInfo.getClientId());
      throw new NotAuthorizedException("Unable to authenticate");
    }

    Set<String> activatedPrincipalRoles = new HashSet<>();
    // TODO: Consolidate the divergent "scopes" logic between test-bearer-token and token-exchange.
    if (tokenInfo.getScope() != null && !tokenInfo.getScope().equals(PRINCIPAL_ROLE_ALL)) {
      activatedPrincipalRoles.addAll(
          Arrays.stream(tokenInfo.getScope().split(" "))
              .map(
                  s -> // strip the principal_role prefix, if present
                  s.startsWith(PRINCIPAL_ROLE_PREFIX)
                          ? s.substring(PRINCIPAL_ROLE_PREFIX.length())
                          : s)
              .toList());
    }

    LOGGER.debug("Resolved principal: {}", principal);

    AuthenticatedPolarisPrincipal authenticatedPrincipal =
        new AuthenticatedPolarisPrincipal(new PrincipalEntity(principal), activatedPrincipalRoles);
    LOGGER.debug("Populating authenticatedPrincipal into CallContext: {}", authenticatedPrincipal);
    CallContext.getCurrentContext()
        .contextVariables()
        .put(CallContext.AUTHENTICATED_PRINCIPAL, authenticatedPrincipal);
    return Optional.of(authenticatedPrincipal);
  }
}
