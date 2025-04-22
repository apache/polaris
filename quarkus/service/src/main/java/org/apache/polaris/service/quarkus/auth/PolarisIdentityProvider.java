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

import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.IdentityProvider;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.TokenAuthenticationRequest;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.quarkus.vertx.http.runtime.security.HttpSecurityUtils;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.NotAuthorizedException;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.service.auth.Authenticator;
import org.apache.polaris.service.quarkus.auth.PolarisAuthenticationMechanism.PolarisTokenCredential;

/** A custom {@link IdentityProvider} that handles Polaris token authentication requests. */
@ApplicationScoped
public class PolarisIdentityProvider implements IdentityProvider<TokenAuthenticationRequest> {

  @Inject Authenticator<String, AuthenticatedPolarisPrincipal> authenticator;

  @Override
  public Class<TokenAuthenticationRequest> getRequestType() {
    return TokenAuthenticationRequest.class;
  }

  @Override
  public Uni<SecurityIdentity> authenticate(
      TokenAuthenticationRequest request, AuthenticationRequestContext context) {
    if (!(request.getToken() instanceof PolarisTokenCredential)) {
      return Uni.createFrom().nullItem();
    }
    return context.runBlocking(() -> createSecurityIdentity(request, authenticator));
  }

  public SecurityIdentity createSecurityIdentity(
      TokenAuthenticationRequest request,
      Authenticator<String, AuthenticatedPolarisPrincipal> authenticator) {
    try {
      AuthenticatedPolarisPrincipal principal =
          authenticator
              .authenticate(request.getToken().getToken())
              .orElseThrow(() -> new NotAuthorizedException("Authentication failed"));
      QuarkusSecurityIdentity.Builder builder =
          QuarkusSecurityIdentity.builder()
              .setPrincipal(principal)
              .addCredential(request.getToken())
              .addRoles(principal.getActivatedPrincipalRoleNames())
              .addAttribute(SecurityIdentity.USER_ATTRIBUTE, principal);
      RoutingContext routingContext = HttpSecurityUtils.getRoutingContextAttribute(request);
      if (routingContext != null) {
        builder.addAttribute(RoutingContext.class.getName(), routingContext);
      }
      return builder.build();
    } catch (RuntimeException e) {
      throw new AuthenticationFailedException(e);
    }
  }
}
