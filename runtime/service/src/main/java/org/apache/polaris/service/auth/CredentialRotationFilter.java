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
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ResourceInfo;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipalAttributes;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.service.admin.api.PolarisPrincipalsApi;
import org.apache.polaris.service.config.FilterPriorities;
import org.jboss.resteasy.reactive.server.ServerRequestFilter;

/**
 * Rejects every request made by a principal that must rotate its credentials first, except the
 * request that rotates them. The check is independent of the configured authorizer.
 *
 * <p>The check is only active when {@link
 * FeatureConfiguration#ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING} is enabled.
 */
public class CredentialRotationFilter {

  private static final String ROTATE_CREDENTIALS_METHOD = "rotateCredentials";

  @Inject RealmConfig realmConfig;

  @ServerRequestFilter(priority = FilterPriorities.CREDENTIAL_ROTATION_FILTER)
  public void checkCredentialRotationRequired(
      ContainerRequestContext requestContext, ResourceInfo resourceInfo) {
    if (requestContext.getSecurityContext().getUserPrincipal() instanceof PolarisPrincipal principal
        && mustRotateCredentials(principal)
        && !isRotateCredentials(resourceInfo)
        && realmConfig.getConfig(
            FeatureConfiguration.ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING)) {
      throw new ForbiddenException(
          "Principal '%s' is not authorized because it must rotate credentials first",
          principal.getName());
    }
  }

  private static boolean isRotateCredentials(ResourceInfo resourceInfo) {
    return PolarisPrincipalsApi.class.isAssignableFrom(resourceInfo.getResourceClass())
        && resourceInfo.getResourceMethod().getName().equals(ROTATE_CREDENTIALS_METHOD);
  }

  private static boolean mustRotateCredentials(PolarisPrincipal principal) {
    return principal
        .getAttributes()
        .getOptional(PolarisPrincipalAttributes.PRINCIPAL_ENTITY_ATTRIBUTE_KEY)
        .map(PrincipalEntity::getInternalPropertiesAsMap)
        .map(
            map ->
                map.containsKey(
                    PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE))
        .orElse(false);
  }
}
