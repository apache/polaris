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

import com.google.common.annotations.VisibleForTesting;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.credential.TokenCredential;
import io.quarkus.security.identity.IdentityProviderManager;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.AuthenticationRequest;
import io.quarkus.security.identity.request.TokenAuthenticationRequest;
import io.quarkus.vertx.http.runtime.security.ChallengeData;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticationMechanism;
import io.quarkus.vertx.http.runtime.security.HttpCredentialTransport;
import io.quarkus.vertx.http.runtime.security.HttpSecurityUtils;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.RoutingContext;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.Set;
import org.apache.polaris.service.auth.AuthenticationRealmConfiguration;
import org.apache.polaris.service.auth.AuthenticationType;
import org.apache.polaris.service.auth.DecodedToken;
import org.apache.polaris.service.auth.TokenBroker;
import org.apache.polaris.service.quarkus.auth.QuarkusPrincipalAuthInfo;

/**
 * A custom {@link HttpAuthenticationMechanism} that handles internal token authentication, that is,
 * authentication using tokens provided by Polaris itself.
 */
@ApplicationScoped
class InternalAuthenticationMechanism implements HttpAuthenticationMechanism {

  // Must be higher than the OIDC authentication mechanism priority, which is
  // HttpAuthenticationMechanism.DEFAULT_PRIORITY + 1, since this mechanism must be tried first.
  // See io.quarkus.oidc.runtime.OidcAuthenticationMechanism
  public static final int PRIORITY = HttpAuthenticationMechanism.DEFAULT_PRIORITY + 100;

  private static final String BEARER = "Bearer";

  @VisibleForTesting final AuthenticationRealmConfiguration configuration;
  private final TokenBroker tokenBroker;

  @Inject
  public InternalAuthenticationMechanism(
      AuthenticationRealmConfiguration configuration, TokenBroker tokenBroker) {
    this.configuration = configuration;
    this.tokenBroker = tokenBroker;
  }

  @Override
  public int getPriority() {
    return PRIORITY;
  }

  @Override
  public Uni<SecurityIdentity> authenticate(
      RoutingContext context, IdentityProviderManager identityProviderManager) {

    if (configuration.type() == AuthenticationType.EXTERNAL) {
      return Uni.createFrom().nullItem();
    }

    String authHeader = context.request().getHeader("Authorization");
    if (authHeader == null) {
      return Uni.createFrom().nullItem();
    }

    int spaceIdx = authHeader.indexOf(' ');
    if (spaceIdx <= 0 || !authHeader.substring(0, spaceIdx).equalsIgnoreCase(BEARER)) {
      return Uni.createFrom().nullItem();
    }

    String credential = authHeader.substring(spaceIdx + 1);

    DecodedToken token;
    try {
      token = tokenBroker.verify(credential);
    } catch (Exception e) {
      return configuration.type() == AuthenticationType.MIXED
          ? Uni.createFrom().nullItem() // let other auth mechanisms handle it
          : Uni.createFrom().failure(new AuthenticationFailedException(e)); // stop here
    }

    if (token == null) {
      return Uni.createFrom().nullItem();
    }

    return identityProviderManager.authenticate(
        HttpSecurityUtils.setRoutingContextAttribute(
            new TokenAuthenticationRequest(new InternalPrincipalAuthInfo(credential, token)),
            context));
  }

  @Override
  public Uni<ChallengeData> getChallenge(RoutingContext context) {
    ChallengeData result =
        new ChallengeData(
            HttpResponseStatus.UNAUTHORIZED.code(), HttpHeaderNames.WWW_AUTHENTICATE, BEARER);
    return Uni.createFrom().item(result);
  }

  @Override
  public Set<Class<? extends AuthenticationRequest>> getCredentialTypes() {
    return Collections.singleton(TokenAuthenticationRequest.class);
  }

  @Override
  public Uni<HttpCredentialTransport> getCredentialTransport(RoutingContext context) {
    return Uni.createFrom()
        .item(new HttpCredentialTransport(HttpCredentialTransport.Type.AUTHORIZATION, BEARER));
  }

  static class InternalPrincipalAuthInfo extends TokenCredential
      implements QuarkusPrincipalAuthInfo {

    private final DecodedToken token;

    InternalPrincipalAuthInfo(String credential, DecodedToken token) {
      super(credential, "bearer");
      this.token = token;
    }

    @Nullable
    @Override
    public Long getPrincipalId() {
      return token.getPrincipalId();
    }

    @Nullable
    @Override
    public String getPrincipalName() {
      return token.getPrincipalName();
    }

    @Override
    public Set<String> getPrincipalRoles() {
      return token.getPrincipalRoles();
    }
  }
}
