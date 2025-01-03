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
package org.apache.polaris.service.quarkus.auth;

import jakarta.inject.Inject;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.SecurityContext;
import java.security.Principal;
import java.util.Optional;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.auth.Authenticator;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jboss.resteasy.reactive.server.ServerRequestFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO replace with all authN with Quarkus native authN
public class QuarkusOAuthFilter {

  private static final Logger LOGGER = LoggerFactory.getLogger(QuarkusOAuthFilter.class);

  private static final String CHALLENGE_FORMAT = "Bearer realm=\"%s\"";

  /**
   * Query parameter used to pass Bearer token
   *
   * @see <a href="https://tools.ietf.org/html/rfc6750#section-2.3">The OAuth 2.0 Authorization
   *     Framework: Bearer Token Usage</a>
   */
  public static final String OAUTH_ACCESS_TOKEN_PARAM = "access_token";

  @Inject Authenticator<String, AuthenticatedPolarisPrincipal> authenticator;
  @Inject RealmContext realmContext;

  @ServerRequestFilter(priority = Priorities.AUTHENTICATION)
  public void authenticate(ContainerRequestContext requestContext) {

    if (requestContext.getUriInfo().getPath().equals("/api/catalog/v1/oauth/tokens")) {
      return;
    }

    String credentials =
        getCredentials(requestContext.getHeaders().getFirst(HttpHeaders.AUTHORIZATION));

    // If Authorization header is not used, check query parameter where token can be passed as well
    if (credentials == null) {
      credentials =
          requestContext.getUriInfo().getQueryParameters().getFirst(OAUTH_ACCESS_TOKEN_PARAM);
    }

    if (!authenticate(requestContext, credentials, SecurityContext.BASIC_AUTH)) {
      throw new NotAuthorizedException(
          "Credentials are required to access this resource.",
          String.format(CHALLENGE_FORMAT, realmContext.getRealmIdentifier()));
    }
  }

  /**
   * Parses a value of the `Authorization` header in the form of `Bearer a892bf3e284da9bb40648ab10`.
   *
   * @param header the value of the `Authorization` header
   * @return a token
   */
  @Nullable
  private String getCredentials(String header) {
    if (header == null) {
      return null;
    }

    final int space = header.indexOf(' ');
    if (space <= 0) {
      return null;
    }

    final String method = header.substring(0, space);
    if (!"Bearer".equalsIgnoreCase(method)) {
      return null;
    }

    return header.substring(space + 1);
  }

  /**
   * Authenticates a request with user credentials and setup the security context.
   *
   * @param requestContext the context of the request
   * @param credentials the user credentials
   * @param scheme the authentication scheme; one of {@code BASIC_AUTH, FORM_AUTH, CLIENT_CERT_AUTH,
   *     DIGEST_AUTH}. See {@link SecurityContext}
   * @return {@code true}, if the request is authenticated, otherwise {@code false}
   */
  protected boolean authenticate(
      ContainerRequestContext requestContext, @Nullable String credentials, String scheme) {
    if (credentials == null) {
      return false;
    }

    Optional<AuthenticatedPolarisPrincipal> principal = authenticator.authenticate(credentials);
    if (principal.isEmpty()) {
      return false;
    }

    LOGGER.debug("Authenticated user: {}", principal.get().getName());

    AuthenticatedPolarisPrincipal prince = principal.get();
    SecurityContext securityContext = augmentSecurityContext(requestContext, scheme, prince);
    requestContext.setSecurityContext(securityContext);
    return true;
  }

  private static SecurityContext augmentSecurityContext(
      ContainerRequestContext requestContext, String scheme, AuthenticatedPolarisPrincipal prince) {
    SecurityContext securityContext = requestContext.getSecurityContext();
    boolean secure = securityContext != null && securityContext.isSecure();
    return new SecurityContext() {
      @Override
      public Principal getUserPrincipal() {
        return prince;
      }

      @Override
      public boolean isUserInRole(String role) {
        return true; // TODO: implement role-based access control
      }

      @Override
      public boolean isSecure() {
        return secure;
      }

      @Override
      public String getAuthenticationScheme() {
        return scheme;
      }
    };
  }
}
