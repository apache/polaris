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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base implementation of {@link Authenticator} constructs a {@link AuthenticatedPolarisPrincipal}
 * from the token parsed by subclasses. The {@link AuthenticatedPolarisPrincipal} is read from the
 * {@link PolarisMetaStoreManager} for the current {@link RealmContext}. If the token defines a
 * non-empty set of scopes, only the principal roles specified in the scopes will be active for the
 * current principal. Only the grants assigned to these roles will be active in the current request.
 */
public abstract class BasePolarisAuthenticator
    implements Authenticator<String, AuthenticatedPolarisPrincipal> {
  public static final String PRINCIPAL_ROLE_ALL = "PRINCIPAL_ROLE:ALL";
  public static final String PRINCIPAL_ROLE_PREFIX = "PRINCIPAL_ROLE:";
  private static final Logger LOGGER = LoggerFactory.getLogger(BasePolarisAuthenticator.class);

  protected final MetaStoreManagerFactory metaStoreManagerFactory;
  protected final CallContext callContext;

  protected BasePolarisAuthenticator(
      MetaStoreManagerFactory metaStoreManagerFactory, CallContext callContext) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.callContext = callContext;
  }

  protected Optional<AuthenticatedPolarisPrincipal> getPrincipal(DecodedToken tokenInfo) {
    LOGGER.debug("Resolving principal for tokenInfo client_id={}", tokenInfo.getClientId());
    PolarisMetaStoreManager metaStoreManager =
        metaStoreManagerFactory.getOrCreateMetaStoreManager(callContext.getRealmContext());
    PolarisEntity principal;
    try {
      principal =
          tokenInfo.getPrincipalId() > 0
              ? PolarisEntity.of(
                  metaStoreManager.loadEntity(
                      callContext.getPolarisCallContext(),
                      0L,
                      tokenInfo.getPrincipalId(),
                      PolarisEntityType.PRINCIPAL))
              : PolarisEntity.of(
                  metaStoreManager.readEntityByName(
                      callContext.getPolarisCallContext(),
                      null,
                      PolarisEntityType.PRINCIPAL,
                      PolarisEntitySubType.NULL_SUBTYPE,
                      tokenInfo.getSub()));
    } catch (Exception e) {
      LOGGER
          .atError()
          .addKeyValue("errMsg", e.getMessage())
          .addKeyValue("stackTrace", ExceptionUtils.getStackTrace(e))
          .log("Unable to authenticate user with token");
      throw new ServiceFailureException("Unable to fetch principal entity");
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
    callContext.contextVariables().put(CallContext.AUTHENTICATED_PRINCIPAL, authenticatedPrincipal);
    return Optional.of(authenticatedPrincipal);
  }
}
