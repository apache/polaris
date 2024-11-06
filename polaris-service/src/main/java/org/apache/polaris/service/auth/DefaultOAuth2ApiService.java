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

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.service.config.HasMetaStoreManagerFactory;
import org.apache.polaris.service.config.OAuth2ApiService;
import org.apache.polaris.service.types.TokenType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the {@link OAuth2ApiService} that generates a JWT token for the client
 * if the client secret matches.
 */
@Named("default")
public class DefaultOAuth2ApiService implements OAuth2ApiService {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultOAuth2ApiService.class);
  private TokenBrokerFactory tokenBrokerFactory;

  public DefaultOAuth2ApiService() {}

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
      SecurityContext securityContext) {

    TokenBroker tokenBroker =
        tokenBrokerFactory.apply(CallContext.getCurrentContext().getRealmContext());
    if (!tokenBroker.supportsGrantType(grantType)) {
      return OAuthUtils.getResponseFromError(OAuthTokenErrorResponse.Error.unsupported_grant_type);
    }
    if (!tokenBroker.supportsRequestedTokenType(requestedTokenType)) {
      return OAuthUtils.getResponseFromError(OAuthTokenErrorResponse.Error.invalid_request);
    }
    if (authHeader == null && clientId == null) {
      return OAuthUtils.getResponseFromError(OAuthTokenErrorResponse.Error.invalid_client);
    }
    if (authHeader != null && clientId == null && authHeader.startsWith("Basic ")) {
      String credentials = new String(Base64.decodeBase64(authHeader.substring(6)), UTF_8);
      if (!credentials.contains(":")) {
        return OAuthUtils.getResponseFromError(OAuthTokenErrorResponse.Error.invalid_client);
      }
      LOGGER.debug("Found credentials in auth header - treating as client_credentials");
      String[] parts = credentials.split(":", 2);
      clientId = parts[0];
      clientSecret = parts[1];
    }
    TokenResponse tokenResponse =
        switch (subjectTokenType) {
          case TokenType.ID_TOKEN,
                  TokenType.REFRESH_TOKEN,
                  TokenType.JWT,
                  TokenType.SAML1,
                  TokenType.SAML2 ->
              new TokenResponse(OAuthTokenErrorResponse.Error.invalid_request);
          case TokenType.ACCESS_TOKEN -> {
            // token exchange with client id and client secret means the client has previously
            // attempted to refresh
            // an access token, but refreshing was not supported by the token broker. Accept the
            // client id and
            // secret and treat it as a new token request
            if (clientId != null && clientSecret != null) {
              yield tokenBroker.generateFromClientSecrets(
                  clientId, clientSecret, OAuth2Constants.CLIENT_CREDENTIALS, scope);
            } else {
              yield tokenBroker.generateFromToken(subjectTokenType, subjectToken, grantType, scope);
            }
          }
          case null ->
              tokenBroker.generateFromClientSecrets(clientId, clientSecret, grantType, scope);
        };
    if (tokenResponse == null) {
      return OAuthUtils.getResponseFromError(OAuthTokenErrorResponse.Error.unsupported_grant_type);
    }
    if (!tokenResponse.isValid()) {
      return OAuthUtils.getResponseFromError(tokenResponse.getError());
    }
    return Response.ok(
            OAuthTokenResponse.builder()
                .withToken(tokenResponse.getAccessToken())
                .withTokenType(OAuth2Constants.BEARER)
                .withIssuedTokenType(OAuth2Properties.ACCESS_TOKEN_TYPE)
                .setExpirationInSeconds(tokenResponse.getExpiresIn())
                .build())
        .build();
  }

  @Inject
  public void setMetaStoreManagerFactory(MetaStoreManagerFactory metaStoreManagerFactory) {
    if (tokenBrokerFactory instanceof HasMetaStoreManagerFactory hemf) {
      hemf.setMetaStoreManagerFactory(metaStoreManagerFactory);
    }
  }

  @Inject
  public void setTokenBroker(TokenBrokerFactory tokenBrokerFactory) {
    this.tokenBrokerFactory = tokenBrokerFactory;
  }
}
