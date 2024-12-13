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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.ext.Provider;
import java.security.Principal;
import java.util.Optional;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;

@PreMatching
@Priority(Priorities.AUTHENTICATION)
@ApplicationScoped
@Provider
public class PolarisPrincipalAuthenticatorFilter implements ContainerRequestFilter {

  @Inject Authenticator<String, AuthenticatedPolarisPrincipal> authenticator;

  @Override
  public void filter(ContainerRequestContext requestContext) {

    // anonymous paths
    if (requestContext.getUriInfo().getPath().equals("/api/catalog/v1/oauth/tokens")) {
      return;
    }

    String authHeader = requestContext.getHeaderString("Authorization");

    if (authHeader == null) {
      throw new NotAuthorizedException("Authorization header is missing");
    }
    int spaceIdx = authHeader.indexOf(' ');
    if (spaceIdx <= 0 || !authHeader.substring(0, spaceIdx).equalsIgnoreCase("Bearer")) {
      throw new NotAuthorizedException("Authorization header is not a Bearer token");
    }
    String credential = authHeader.substring(spaceIdx + 1);
    Optional<AuthenticatedPolarisPrincipal> principal = authenticator.authenticate(credential);
    if (principal.isEmpty()) {
      throw new NotAuthorizedException("Unable to authenticate");
    }
    SecurityContext securityContext = requestContext.getSecurityContext();
    requestContext.setSecurityContext(
        new SecurityContext() {
          @Override
          public Principal getUserPrincipal() {
            return principal.get();
          }

          @Override
          public boolean isUserInRole(String role) {
            return securityContext.isUserInRole(role);
          }

          @Override
          public boolean isSecure() {
            return securityContext.isSecure();
          }

          @Override
          public String getAuthenticationScheme() {
            return securityContext.getAuthenticationScheme();
          }
        });
  }
}
