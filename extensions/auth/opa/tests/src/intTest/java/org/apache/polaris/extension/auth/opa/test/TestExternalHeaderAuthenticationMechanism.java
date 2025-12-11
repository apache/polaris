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
package org.apache.polaris.extension.auth.opa.test;

import io.quarkus.security.identity.IdentityProviderManager;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.quarkus.vertx.http.runtime.security.ChallengeData;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticationMechanism;
import io.quarkus.vertx.http.runtime.security.HttpCredentialTransport;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.RoutingContext;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.auth.Authenticator;
import org.apache.polaris.service.auth.PolarisCredential;
import org.jboss.logging.Logger;

/**
 * Test-only authentication mechanism that turns test headers into external {@link
 * PolarisCredential} instances. This avoids any DB lookups and exercises the external authenticator
 * flow.
 */
@ApplicationScoped
@Priority(HttpAuthenticationMechanism.DEFAULT_PRIORITY + 200)
public class TestExternalHeaderAuthenticationMechanism implements HttpAuthenticationMechanism {

  private static final Logger LOG =
      Logger.getLogger(TestExternalHeaderAuthenticationMechanism.class);

  static final String PRINCIPAL_HEADER = "X-External-Principal";
  static final String ROLES_HEADER = "X-External-Roles";

  @Inject Authenticator authenticator;

  @Override
  public Uni<SecurityIdentity> authenticate(
      RoutingContext context, IdentityProviderManager identityProviderManager) {
    String principal = context.request().getHeader(PRINCIPAL_HEADER);
    if (principal == null || principal.isBlank()) {
      return Uni.createFrom().nullItem();
    }
    Set<String> roles = parseRoles(context.request().getHeader(ROLES_HEADER));
    PolarisCredential credential = PolarisCredential.of(null, principal, roles, true);
    PolarisPrincipal polarisPrincipal = authenticator.authenticate(credential);
    QuarkusSecurityIdentity identity =
        QuarkusSecurityIdentity.builder()
            .setPrincipal(polarisPrincipal)
            .addCredential(credential)
            .addRoles(polarisPrincipal.getRoles())
            .setAnonymous(false)
            .build();
    LOG.debugf(
        "Authenticated external principal from headers principal=%s roles=%s", principal, roles);
    return Uni.createFrom().item(identity);
  }

  @Override
  public Uni<ChallengeData> getChallenge(RoutingContext context) {
    return Uni.createFrom().nullItem();
  }

  @Override
  public Set<Class<? extends io.quarkus.security.identity.request.AuthenticationRequest>>
      getCredentialTypes() {
    return Collections.emptySet();
  }

  @Override
  public Uni<HttpCredentialTransport> getCredentialTransport(RoutingContext context) {
    return Uni.createFrom().nullItem();
  }

  private Set<String> parseRoles(String header) {
    if (header == null || header.isBlank()) {
      return Set.of();
    }
    return Arrays.stream(header.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toSet());
  }
}
