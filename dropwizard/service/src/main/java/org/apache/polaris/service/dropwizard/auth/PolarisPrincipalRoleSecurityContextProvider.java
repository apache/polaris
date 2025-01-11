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
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.security.Principal;
import java.util.Set;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.service.auth.ActiveRolesProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Priority(Priorities.AUTHENTICATION + 1)
public class PolarisPrincipalRoleSecurityContextProvider implements ContainerRequestFilter {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PolarisPrincipalRoleSecurityContextProvider.class);
  @Inject ActiveRolesProvider activeRolesProvider;

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    AuthenticatedPolarisPrincipal polarisPrincipal =
        (AuthenticatedPolarisPrincipal) requestContext.getSecurityContext().getUserPrincipal();
    if (polarisPrincipal == null) {
      return;
    }
    SecurityContext securityContext =
        createSecurityContext(requestContext.getSecurityContext(), polarisPrincipal);
    requestContext.setSecurityContext(securityContext);
  }

  public SecurityContext createSecurityContext(
      SecurityContext ctx, AuthenticatedPolarisPrincipal principal) {
    Set<String> validRoleNames = activeRolesProvider.getActiveRoles(principal);
    if (validRoleNames.isEmpty()) {
      LOGGER.warn("Principal {} has no active roles. Request will be denied.", principal.getName());
      throw new ForbiddenException("Principal has no active roles");
    }
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
}
