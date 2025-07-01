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
package org.apache.polaris.service.quarkus.auth.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.security.credential.TokenCredential;
import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.TokenAuthenticationRequest;
import io.quarkus.vertx.http.runtime.security.HttpSecurityUtils;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.RoutingContext;
import java.security.Principal;
import org.apache.polaris.service.quarkus.auth.internal.InternalAuthenticationMechanism.InternalPrincipalAuthInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class InternalIdentityProviderTest {

  private InternalIdentityProvider provider;
  private AuthenticationRequestContext context;

  @BeforeEach
  public void setup() {
    provider = new InternalIdentityProvider();
    context = mock(AuthenticationRequestContext.class);
  }

  @Test
  public void testAuthenticateWithWrongCredential() {
    TokenCredential nonInternalCredential = mock(TokenCredential.class);
    TokenAuthenticationRequest request = new TokenAuthenticationRequest(nonInternalCredential);

    Uni<SecurityIdentity> result = provider.authenticate(request, context);

    assertThat(result.await().indefinitely()).isNull();
  }

  @Test
  public void testAuthenticateWithValidCredential() {
    // Create a mock InternalPrincipalAuthInfo
    InternalPrincipalAuthInfo credential = mock(InternalPrincipalAuthInfo.class);
    when(credential.getPrincipalName()).thenReturn("testUser");

    // Create a request with the credential and a routing context attribute
    RoutingContext routingContext = mock(RoutingContext.class);
    TokenAuthenticationRequest request = new TokenAuthenticationRequest(credential);
    HttpSecurityUtils.setRoutingContextAttribute(request, routingContext);

    // Authenticate the request
    Uni<SecurityIdentity> result = provider.authenticate(request, context);

    // Verify the result
    SecurityIdentity identity = result.await().indefinitely();
    assertThat(identity).isNotNull();

    // Verify the principal
    Principal principal = identity.getPrincipal();
    assertThat(principal).isNotNull();
    assertThat(principal.getName()).isEqualTo("testUser");

    // Verify the credential is set
    assertThat(identity.getCredential(InternalPrincipalAuthInfo.class)).isSameAs(credential);

    // Verify the routing context attribute is set
    assertThat((RoutingContext) identity.getAttribute(RoutingContext.class.getName()))
        .isSameAs(routingContext);
  }
}
