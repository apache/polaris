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
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default authenticator that resolves a {@link PrincipalAuthInfo} to an {@link
 * AuthenticatedPolarisPrincipal}.
 *
 * <p>This authenticator is used in both internal and external authentication scenarios.
 */
@RequestScoped
@Identifier("default")
public class DefaultAuthenticator
    implements Authenticator<PrincipalAuthInfo, AuthenticatedPolarisPrincipal> {

  public static final String PRINCIPAL_ROLE_ALL = "PRINCIPAL_ROLE:ALL";
  public static final String PRINCIPAL_ROLE_PREFIX = "PRINCIPAL_ROLE:";

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultAuthenticator.class);

  @Inject MetaStoreManagerFactory metaStoreManagerFactory;
  @Inject CallContext callContext;

  @Override
  public Optional<AuthenticatedPolarisPrincipal> authenticate(PrincipalAuthInfo credentials) {
    LOGGER.debug("Resolving principal for credentials={}", credentials);
    PolarisMetaStoreManager metaStoreManager =
        metaStoreManagerFactory.getOrCreateMetaStoreManager(callContext.getRealmContext());
    PolarisEntity principal = null;
    try {
      // If the principal id is present, prefer to use it to load the principal entity,
      // otherwise, use the principal name to load the entity.
      if (credentials.getPrincipalId() != null && credentials.getPrincipalId() > 0) {
        principal =
            PolarisEntity.of(
                metaStoreManager.loadEntity(
                    callContext.getPolarisCallContext(),
                    0L,
                    credentials.getPrincipalId(),
                    PolarisEntityType.PRINCIPAL));
      } else if (credentials.getPrincipalName() != null) {
        principal =
            PolarisEntity.of(
                metaStoreManager.readEntityByName(
                    callContext.getPolarisCallContext(),
                    null,
                    PolarisEntityType.PRINCIPAL,
                    PolarisEntitySubType.NULL_SUBTYPE,
                    credentials.getPrincipalName()));
      }
    } catch (Exception e) {
      LOGGER
          .atError()
          .addKeyValue("errMsg", e.getMessage())
          .addKeyValue("stackTrace", ExceptionUtils.getStackTrace(e))
          .log("Unable to authenticate user with token");
      throw new ServiceFailureException("Unable to fetch principal entity");
    }
    if (principal == null || principal.getType() != PolarisEntityType.PRINCIPAL) {
      LOGGER.warn("Failed to resolve principal from credentials={}", credentials);
      throw new NotAuthorizedException("Unable to authenticate");
    }

    LOGGER.debug("Resolved principal: {}", principal);

    boolean allRoles = credentials.getPrincipalRoles().contains(PRINCIPAL_ROLE_ALL);

    Set<String> activatedPrincipalRoles = new HashSet<>();
    if (!allRoles) {
      activatedPrincipalRoles.addAll(
          credentials.getPrincipalRoles().stream()
              .filter(s -> s.startsWith(PRINCIPAL_ROLE_PREFIX))
              .map(s -> s.substring(PRINCIPAL_ROLE_PREFIX.length()))
              .toList());
    }

    LOGGER.debug("Resolved principal: {}", principal);

    AuthenticatedPolarisPrincipal authenticatedPrincipal =
        new AuthenticatedPolarisPrincipal(new PrincipalEntity(principal), activatedPrincipalRoles);
    return Optional.of(authenticatedPrincipal);
  }
}
