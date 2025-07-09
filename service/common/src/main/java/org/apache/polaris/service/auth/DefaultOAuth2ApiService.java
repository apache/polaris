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

import static java.nio.charset.StandardCharsets.UTF_8;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.commons.codec.binary.Base64;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.catalog.api.IcebergRestOAuth2ApiService;
import org.apache.polaris.service.types.TokenType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the {@link IcebergRestOAuth2ApiService} that generates a JWT token for
 * the client if the client secret matches.
 */
@RequestScoped
@Identifier("default")
public class DefaultOAuth2ApiService implements IcebergRestOAuth2ApiService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultOAuth2ApiService.class);

  private static final String BEARER = "bearer";

  private final TokenBroker tokenBroker;
  private final CallContext callContext;

  @Inject
  public DefaultOAuth2ApiService(TokenBroker tokenBroker, CallContext callContext) {
    this.tokenBroker = tokenBroker;
    this.callContext = callContext;
  }

  @Override
  public Response getToken(
      String authHeader,
      String grantType,
      String scope,
      String clientId,
      String clientSecret,
      TokenType requestedTokenType,
      String subjectToken,
      TokenType subjectTokenType,
      String actorToken,
      TokenType actorTokenType,
      RealmContext realmContext,
      SecurityContext securityContext) {

    if (!tokenBroker.supportsGrantType(grantType)) {
      return OAuthUtils.getResponseFromError(OAuthTokenErrorResponse.Error.unsupported_grant_type);
    }
    if (!tokenBroker.supportsRequestedTokenType(requestedTokenType)) {
      return OAuthUtils.getResponseFromError(OAuthTokenErrorResponse.Error.invalid_request);
    }
    if (authHeader == null && clientSecret == null) {
      return OAuthUtils.getResponseFromError(OAuthTokenErrorResponse.Error.invalid_client);
    }
    // token exchange with client id and client secret in the authorization header means the client
    // has previously attempted to refresh an access token, but refreshing was not supported by the
    // token broker. Accept the client id and secret and treat it as a new token request
    if (authHeader != null && clientSecret == null && authHeader.startsWith("Basic ")) {
      String credentials = new String(Base64.decodeBase64(authHeader.substring(6)), UTF_8);
      if (!credentials.contains(":")) {
        return OAuthUtils.getResponseFromError(OAuthTokenErrorResponse.Error.invalid_request);
      }
      LOGGER.debug("Found credentials in auth header - treating as client_credentials");
      String[] parts = credentials.split(":", 2);
      if (parts.length == 2) {
        clientId = parts[0];
        clientSecret = parts[1];
      } else {
        LOGGER.debug("Don't know how to parse Basic auth header");
        return OAuthUtils.getResponseFromError(OAuthTokenErrorResponse.Error.invalid_request);
      }
    }
    TokenResponse tokenResponse;
    if (clientSecret != null) {
      tokenResponse =
          tokenBroker.generateFromClientSecrets(
              clientId,
              clientSecret,
              grantType,
              scope,
              callContext.getPolarisCallContext(),
              requestedTokenType);
    } else if (subjectToken != null) {
      tokenResponse =
          tokenBroker.generateFromToken(
              subjectTokenType, subjectToken, grantType, scope, requestedTokenType);
    } else {
      return OAuthUtils.getResponseFromError(OAuthTokenErrorResponse.Error.invalid_request);
    }
    if (tokenResponse == null) {
      return OAuthUtils.getResponseFromError(OAuthTokenErrorResponse.Error.unsupported_grant_type);
    }
    if (!tokenResponse.isValid()) {
      return OAuthUtils.getResponseFromError(tokenResponse.getError());
    }
    return Response.ok(
            OAuthTokenResponse.builder()
                .withToken(tokenResponse.getAccessToken())
                .withTokenType(BEARER)
                .withIssuedTokenType(tokenResponse.getTokenType())
                .setExpirationInSeconds(tokenResponse.getExpiresIn())
                .build())
        .build();
  }
}
