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

import jakarta.annotation.Priority;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.ext.Provider;
import java.security.Principal;
import java.util.Set;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

@PreMatching
@Priority(Priorities.AUTHENTICATION + 1)
@RequestScoped
@Provider
public class PolarisPrincipalRolesProviderFilter implements ContainerRequestFilter {

  @Inject ActiveRolesProvider activeRolesProvider;

  @Override
  public void filter(ContainerRequestContext requestContext) {
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
