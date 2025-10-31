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
package org.apache.polaris.service.auth.internal.service;

import static org.mockito.Mockito.when;

import jakarta.ws.rs.core.Response;
import java.nio.charset.Charset;
import java.util.Base64;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.auth.internal.broker.TokenBroker;
import org.apache.polaris.service.auth.internal.broker.TokenResponse;
import org.apache.polaris.service.types.TokenType;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@SuppressWarnings("resource")
class DefaultOAuth2ApiServiceTest {
  private static final String CLIENT_CREDENTIALS = "client_credentials";
  private static final String TOKEN_EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange";

  private final RealmContext realmContext = () -> "realm";
  private PolarisMetaStoreManager metaStoreManager;

  @BeforeEach
  void setUp() {
    metaStoreManager = Mockito.mock(PolarisMetaStoreManager.class);
  }

  @Test
  public void testNoSupportGrantType() {
    TokenBroker tokenBroker = Mockito.mock();
    when(tokenBroker.supportsGrantType(CLIENT_CREDENTIALS)).thenReturn(false);
    when(tokenBroker.supportsRequestedTokenType(TokenType.ACCESS_TOKEN)).thenReturn(true);
    when(tokenBroker.generateFromClientSecrets(
            "client",
            "secret",
            CLIENT_CREDENTIALS,
            "scope",
            metaStoreManager,
            TokenType.ACCESS_TOKEN))
        .thenReturn(TokenResponse.of("token", TokenType.ACCESS_TOKEN.getValue(), 3600));
    Response response =
        new InvocationBuilder()
            .scope("scope")
            .clientId("client")
            .clientSecret("secret")
            .grantType(CLIENT_CREDENTIALS)
            .requestedTokenType(TokenType.ACCESS_TOKEN)
            .realmContext(realmContext)
            .invoke(new DefaultOAuth2ApiService(tokenBroker, metaStoreManager));
    Assertions.assertThat(response.getEntity())
        .isInstanceOf(OAuthTokenErrorResponse.class)
        .asInstanceOf(InstanceOfAssertFactories.type(OAuthTokenErrorResponse.class))
        .returns(OAuthError.unsupported_grant_type.name(), OAuthTokenErrorResponse::getError);
  }

  @Test
  public void testNoSupportRequestedTokenType() {
    TokenBroker tokenBroker = Mockito.mock();
    when(tokenBroker.supportsGrantType(CLIENT_CREDENTIALS)).thenReturn(true);
    when(tokenBroker.supportsRequestedTokenType(TokenType.ACCESS_TOKEN)).thenReturn(false);
    when(tokenBroker.generateFromClientSecrets(
            "client",
            "secret",
            CLIENT_CREDENTIALS,
            "scope",
            metaStoreManager,
            TokenType.ACCESS_TOKEN))
        .thenReturn(TokenResponse.of("token", TokenType.ACCESS_TOKEN.getValue(), 3600));
    Response response =
        new InvocationBuilder()
            .scope("scope")
            .clientId("client")
            .clientSecret("secret")
            .grantType(CLIENT_CREDENTIALS)
            .requestedTokenType(TokenType.ACCESS_TOKEN)
            .realmContext(realmContext)
            .invoke(new DefaultOAuth2ApiService(tokenBroker, metaStoreManager));
    Assertions.assertThat(response.getEntity())
        .isInstanceOf(OAuthTokenErrorResponse.class)
        .asInstanceOf(InstanceOfAssertFactories.type(OAuthTokenErrorResponse.class))
        .returns(OAuthError.invalid_request.name(), OAuthTokenErrorResponse::getError);
  }

  @Test
  public void testSupportClientIdNoSecret() {
    TokenBroker tokenBroker = Mockito.mock();
    when(tokenBroker.supportsGrantType(CLIENT_CREDENTIALS)).thenReturn(true);
    when(tokenBroker.supportsRequestedTokenType(TokenType.ACCESS_TOKEN)).thenReturn(true);
    when(tokenBroker.generateFromClientSecrets(
            null, "secret", CLIENT_CREDENTIALS, "scope", metaStoreManager, TokenType.ACCESS_TOKEN))
        .thenReturn(TokenResponse.of("token", TokenType.ACCESS_TOKEN.getValue(), 3600));
    Response response =
        new InvocationBuilder()
            .scope("scope")
            .clientSecret("secret")
            .grantType(CLIENT_CREDENTIALS)
            .requestedTokenType(TokenType.ACCESS_TOKEN)
            .realmContext(realmContext)
            .invoke(new DefaultOAuth2ApiService(tokenBroker, metaStoreManager));
    Assertions.assertThat(response.getEntity())
        .isInstanceOf(OAuthTokenResponse.class)
        .asInstanceOf(InstanceOfAssertFactories.type(OAuthTokenResponse.class))
        .returns("token", OAuthTokenResponse::token);
  }

  @Test
  public void testSupportClientIdAndSecret() {
    TokenBroker tokenBroker = Mockito.mock();
    when(tokenBroker.supportsGrantType(CLIENT_CREDENTIALS)).thenReturn(true);
    when(tokenBroker.supportsRequestedTokenType(TokenType.ACCESS_TOKEN)).thenReturn(true);
    when(tokenBroker.generateFromClientSecrets(
            "client",
            "secret",
            CLIENT_CREDENTIALS,
            "scope",
            metaStoreManager,
            TokenType.ACCESS_TOKEN))
        .thenReturn(TokenResponse.of("token", TokenType.ACCESS_TOKEN.getValue(), 3600));
    Response response =
        new InvocationBuilder()
            .scope("scope")
            .clientId("client")
            .clientSecret("secret")
            .grantType(CLIENT_CREDENTIALS)
            .requestedTokenType(TokenType.ACCESS_TOKEN)
            .realmContext(realmContext)
            .invoke(new DefaultOAuth2ApiService(tokenBroker, metaStoreManager));
    Assertions.assertThat(response.getEntity())
        .isInstanceOf(OAuthTokenResponse.class)
        .asInstanceOf(InstanceOfAssertFactories.type(OAuthTokenResponse.class))
        .returns("token", OAuthTokenResponse::token);
  }

  @Test
  public void testReadClientCredentialsFromAuthHeader() {
    TokenBroker tokenBroker = Mockito.mock();
    when(tokenBroker.supportsGrantType(TOKEN_EXCHANGE)).thenReturn(true);
    when(tokenBroker.supportsRequestedTokenType(TokenType.ACCESS_TOKEN)).thenReturn(true);
    when(tokenBroker.generateFromClientSecrets(
            "client", "secret", TOKEN_EXCHANGE, "scope", metaStoreManager, TokenType.ACCESS_TOKEN))
        .thenReturn(TokenResponse.of("token", TokenType.ACCESS_TOKEN.getValue(), 3600));
    Response response =
        new InvocationBuilder()
            .authHeader(
                "Basic "
                    + Base64.getEncoder()
                        .encodeToString("client:secret".getBytes(Charset.defaultCharset())))
            .scope("scope")
            .grantType(TOKEN_EXCHANGE)
            .requestedTokenType(TokenType.ACCESS_TOKEN)
            .realmContext(realmContext)
            .invoke(new DefaultOAuth2ApiService(tokenBroker, metaStoreManager));
    Assertions.assertThat(response.getEntity())
        .isInstanceOf(OAuthTokenResponse.class)
        .asInstanceOf(InstanceOfAssertFactories.type(OAuthTokenResponse.class))
        .returns("token", OAuthTokenResponse::token);
  }

  @Test
  public void testAuthHeaderRequiresValidCredentialPair() {
    TokenBroker tokenBroker = Mockito.mock();
    when(tokenBroker.supportsGrantType(TOKEN_EXCHANGE)).thenReturn(true);
    when(tokenBroker.supportsRequestedTokenType(TokenType.ACCESS_TOKEN)).thenReturn(true);
    when(tokenBroker.generateFromClientSecrets(
            null, "secret", TOKEN_EXCHANGE, "scope", metaStoreManager, TokenType.ACCESS_TOKEN))
        .thenReturn(TokenResponse.of("token", TokenType.ACCESS_TOKEN.getValue(), 3600));
    Response response =
        new InvocationBuilder()
            .authHeader(
                "Basic "
                    + Base64.getEncoder()
                        .encodeToString("secret".getBytes(Charset.defaultCharset())))
            .scope("scope")
            .grantType(TOKEN_EXCHANGE)
            .requestedTokenType(TokenType.ACCESS_TOKEN)
            .realmContext(realmContext)
            .invoke(new DefaultOAuth2ApiService(tokenBroker, metaStoreManager));
    Assertions.assertThat(response.getEntity())
        .isInstanceOf(OAuthTokenErrorResponse.class)
        .asInstanceOf(InstanceOfAssertFactories.type(OAuthTokenErrorResponse.class))
        .returns(OAuthError.invalid_request.name(), OAuthTokenErrorResponse::getError);
  }

  @Test
  public void testReadClientSecretFromAuthHeader() {
    TokenBroker tokenBroker = Mockito.mock();
    when(tokenBroker.supportsGrantType(TOKEN_EXCHANGE)).thenReturn(true);
    when(tokenBroker.supportsRequestedTokenType(TokenType.ACCESS_TOKEN)).thenReturn(true);

    when(tokenBroker.generateFromClientSecrets(
            "", "secret", TOKEN_EXCHANGE, "scope", metaStoreManager, TokenType.ACCESS_TOKEN))
        .thenReturn(TokenResponse.of("token", TokenType.ACCESS_TOKEN.getValue(), 3600));
    Response response =
        new InvocationBuilder()

            // here the auth header has a blank client id, providing a blank, but not null client id
            .authHeader(
                "Basic "
                    + Base64.getEncoder()
                        .encodeToString(":secret".getBytes(Charset.defaultCharset())))
            .scope("scope")
            .grantType(TOKEN_EXCHANGE)
            .requestedTokenType(TokenType.ACCESS_TOKEN)
            .realmContext(realmContext)
            .invoke(new DefaultOAuth2ApiService(tokenBroker, metaStoreManager));
    Assertions.assertThat(response.getEntity())
        .isInstanceOf(OAuthTokenResponse.class)
        .asInstanceOf(InstanceOfAssertFactories.type(OAuthTokenResponse.class))
        .returns("token", OAuthTokenResponse::token);
  }

  private static final class InvocationBuilder {
    private String authHeader;
    private String grantType;
    private String scope;
    private String clientId;
    private String clientSecret;
    private TokenType requestedTokenType;
    private RealmContext realmContext;

    public InvocationBuilder authHeader(String authHeader) {
      this.authHeader = authHeader;
      return this;
    }

    public InvocationBuilder grantType(String grantType) {
      this.grantType = grantType;
      return this;
    }

    public InvocationBuilder scope(String scope) {
      this.scope = scope;
      return this;
    }

    public InvocationBuilder clientId(String clientId) {
      this.clientId = clientId;
      return this;
    }

    public InvocationBuilder clientSecret(String clientSecret) {
      this.clientSecret = clientSecret;
      return this;
    }

    public InvocationBuilder requestedTokenType(TokenType requestedTokenType) {
      this.requestedTokenType = requestedTokenType;
      return this;
    }

    public InvocationBuilder realmContext(RealmContext realmContext) {
      this.realmContext = realmContext;
      return this;
    }

    public Response invoke(DefaultOAuth2ApiService instance) {
      return instance.getToken(
          authHeader,
          grantType,
          scope,
          clientId,
          clientSecret,
          requestedTokenType,
          null, // subjectToken
          null, // subjectTokenType
          null, // actorToken
          null, // actorTokenType
          realmContext,
          null); // securityContext
    }
  }
}
