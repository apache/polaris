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

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.IdentityProviderManager;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.quarkus.vertx.http.runtime.security.ChallengeData;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticationMechanism;
import io.quarkus.vertx.http.runtime.security.HttpCredentialTransport;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.auth.PolarisCredential;

@ApplicationScoped
public class NoVerifyAuthenticationMechanism implements HttpAuthenticationMechanism {

  private static final String BEARER = "Bearer";
  private static final String EXTERNAL_ISSUER = "external-idp";

  @Override
  public int getPriority() {
    return HttpAuthenticationMechanism.DEFAULT_PRIORITY + 200;
  }

  @Override
  public Uni<SecurityIdentity> authenticate(
      RoutingContext context, IdentityProviderManager identityProviderManager) {
    String authHeader = context.request().getHeader("Authorization");
    if (authHeader == null) {
      return Uni.createFrom().nullItem();
    }

    int spaceIdx = authHeader.indexOf(' ');
    if (spaceIdx <= 0 || !authHeader.substring(0, spaceIdx).equalsIgnoreCase(BEARER)) {
      return Uni.createFrom().nullItem();
    }

    String token = authHeader.substring(spaceIdx + 1);
    DecodedJWT decoded;
    try {
      decoded = JWT.decode(token);
    } catch (Exception e) {
      return Uni.createFrom().nullItem();
    }

    if (!EXTERNAL_ISSUER.equals(decoded.getIssuer())) {
      return Uni.createFrom().nullItem();
    }

    String principalName = decoded.getSubject();
    if (principalName == null || principalName.isBlank()) {
      return Uni.createFrom().failure(new AuthenticationFailedException("Missing subject"));
    }

    PolarisCredential credential = PolarisCredential.of(null, principalName, Set.of(), token);
    PolarisPrincipal principal = PolarisPrincipal.of(principalName, Map.of(), Set.of());
    SecurityIdentity identity =
        QuarkusSecurityIdentity.builder()
            .setAnonymous(false)
            .setPrincipal(principal)
            .addCredential(credential)
            .build();
    return Uni.createFrom().item(identity);
  }

  @Override
  public Uni<ChallengeData> getChallenge(RoutingContext context) {
    ChallengeData result =
        new ChallengeData(
            HttpResponseStatus.UNAUTHORIZED.code(), HttpHeaderNames.WWW_AUTHENTICATE, BEARER);
    return Uni.createFrom().item(result);
  }

  @Override
  public Set<Class<? extends io.quarkus.security.identity.request.AuthenticationRequest>>
      getCredentialTypes() {
    return Collections.emptySet();
  }

  @Override
  public Uni<HttpCredentialTransport> getCredentialTransport(RoutingContext context) {
    return Uni.createFrom()
        .item(new HttpCredentialTransport(HttpCredentialTransport.Type.AUTHORIZATION, BEARER));
  }
}
