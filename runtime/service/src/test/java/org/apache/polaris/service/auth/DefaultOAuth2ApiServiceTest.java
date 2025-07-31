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

import static org.mockito.Mockito.when;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.nio.charset.Charset;
import java.util.Base64;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.types.TokenType;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DefaultOAuth2ApiServiceTest {
  private static final String CLIENT_CREDENTIALS = "client_credentials";

  private CallContext callContext;

  @BeforeEach
  void setUp() {
    callContext = Mockito.mock(CallContext.class);
    when(callContext.getPolarisCallContext()).thenReturn(Mockito.mock(PolarisCallContext.class));
  }

  @Test
  public void testNoSupportGrantType() {
    RealmContext realmContext = () -> "realm";
    TokenBroker tokenBroker = Mockito.mock();
    when(tokenBroker.supportsGrantType(CLIENT_CREDENTIALS)).thenReturn(false);
    when(tokenBroker.supportsRequestedTokenType(TokenType.ACCESS_TOKEN)).thenReturn(true);
    when(tokenBroker.generateFromClientSecrets(
            "client",
            "secret",
            CLIENT_CREDENTIALS,
            "scope",
            callContext.getPolarisCallContext(),
            TokenType.ACCESS_TOKEN))
        .thenReturn(new TokenResponse("token", TokenType.ACCESS_TOKEN.getValue(), 3600));
    Response response =
        new InvocationBuilder()
            .scope("scope")
            .clientId("client")
            .clientSecret("secret")
            .grantType(CLIENT_CREDENTIALS)
            .requestedTokenType(TokenType.ACCESS_TOKEN)
            .realmContext(realmContext)
            .invoke(new DefaultOAuth2ApiService(tokenBroker, callContext));
    Assertions.assertThat(response.getEntity())
        .isInstanceOf(OAuthTokenErrorResponse.class)
        .asInstanceOf(InstanceOfAssertFactories.type(OAuthTokenErrorResponse.class))
        .returns(
            OAuthTokenErrorResponse.Error.unsupported_grant_type.name(),
            OAuthTokenErrorResponse::getError);
  }

  @Test
  public void testNoSupportRequestedTokenType() {
    RealmContext realmContext = () -> "realm";
    TokenBroker tokenBroker = Mockito.mock();
    when(tokenBroker.supportsGrantType(CLIENT_CREDENTIALS)).thenReturn(true);
    when(tokenBroker.supportsRequestedTokenType(TokenType.ACCESS_TOKEN)).thenReturn(false);
    when(tokenBroker.generateFromClientSecrets(
            "client",
            "secret",
            CLIENT_CREDENTIALS,
            "scope",
            callContext.getPolarisCallContext(),
            TokenType.ACCESS_TOKEN))
        .thenReturn(new TokenResponse("token", TokenType.ACCESS_TOKEN.getValue(), 3600));
    Response response =
        new InvocationBuilder()
            .scope("scope")
            .clientId("client")
            .clientSecret("secret")
            .grantType(CLIENT_CREDENTIALS)
            .requestedTokenType(TokenType.ACCESS_TOKEN)
            .realmContext(realmContext)
            .invoke(new DefaultOAuth2ApiService(tokenBroker, callContext));
    Assertions.assertThat(response.getEntity())
        .isInstanceOf(OAuthTokenErrorResponse.class)
        .asInstanceOf(InstanceOfAssertFactories.type(OAuthTokenErrorResponse.class))
        .returns(
            OAuthTokenErrorResponse.Error.invalid_request.name(),
            OAuthTokenErrorResponse::getError);
  }

  @Test
  public void testSupportClientIdNoSecret() {
    RealmContext realmContext = () -> "realm";
    TokenBroker tokenBroker = Mockito.mock();
    when(tokenBroker.supportsGrantType(CLIENT_CREDENTIALS)).thenReturn(true);
    when(tokenBroker.supportsRequestedTokenType(TokenType.ACCESS_TOKEN)).thenReturn(true);
    when(tokenBroker.generateFromClientSecrets(
            null,
            "secret",
            CLIENT_CREDENTIALS,
            "scope",
            callContext.getPolarisCallContext(),
            TokenType.ACCESS_TOKEN))
        .thenReturn(new TokenResponse("token", TokenType.ACCESS_TOKEN.getValue(), 3600));
    Response response =
        new InvocationBuilder()
            .scope("scope")
            .clientSecret("secret")
            .grantType(CLIENT_CREDENTIALS)
            .requestedTokenType(TokenType.ACCESS_TOKEN)
            .realmContext(realmContext)
            .invoke(new DefaultOAuth2ApiService(tokenBroker, callContext));
    Assertions.assertThat(response.getEntity())
        .isInstanceOf(OAuthTokenResponse.class)
        .asInstanceOf(InstanceOfAssertFactories.type(OAuthTokenResponse.class))
        .returns("token", OAuthTokenResponse::token);
  }

  @Test
  public void testSupportClientIdAndSecret() {
    RealmContext realmContext = () -> "realm";
    TokenBroker tokenBroker = Mockito.mock();
    when(tokenBroker.supportsGrantType(CLIENT_CREDENTIALS)).thenReturn(true);
    when(tokenBroker.supportsRequestedTokenType(TokenType.ACCESS_TOKEN)).thenReturn(true);
    when(tokenBroker.generateFromClientSecrets(
            "client",
            "secret",
            CLIENT_CREDENTIALS,
            "scope",
            callContext.getPolarisCallContext(),
            TokenType.ACCESS_TOKEN))
        .thenReturn(new TokenResponse("token", TokenType.ACCESS_TOKEN.getValue(), 3600));
    Response response =
        new InvocationBuilder()
            .scope("scope")
            .clientId("client")
            .clientSecret("secret")
            .grantType(CLIENT_CREDENTIALS)
            .requestedTokenType(TokenType.ACCESS_TOKEN)
            .realmContext(realmContext)
            .invoke(new DefaultOAuth2ApiService(tokenBroker, callContext));
    Assertions.assertThat(response.getEntity())
        .isInstanceOf(OAuthTokenResponse.class)
        .asInstanceOf(InstanceOfAssertFactories.type(OAuthTokenResponse.class))
        .returns("token", OAuthTokenResponse::token);
  }

  @Test
  public void testReadClientCredentialsFromAuthHeader() {
    RealmContext realmContext = () -> "realm";
    TokenBroker tokenBroker = Mockito.mock();
    when(tokenBroker.supportsGrantType(TokenRequestValidator.TOKEN_EXCHANGE)).thenReturn(true);
    when(tokenBroker.supportsRequestedTokenType(TokenType.ACCESS_TOKEN)).thenReturn(true);
    when(tokenBroker.generateFromClientSecrets(
            "client",
            "secret",
            TokenRequestValidator.TOKEN_EXCHANGE,
            "scope",
            callContext.getPolarisCallContext(),
            TokenType.ACCESS_TOKEN))
        .thenReturn(new TokenResponse("token", TokenType.ACCESS_TOKEN.getValue(), 3600));
    Response response =
        new InvocationBuilder()
            .authHeader(
                "Basic "
                    + Base64.getEncoder()
                        .encodeToString("client:secret".getBytes(Charset.defaultCharset())))
            .scope("scope")
            .grantType(TokenRequestValidator.TOKEN_EXCHANGE)
            .requestedTokenType(TokenType.ACCESS_TOKEN)
            .realmContext(realmContext)
            .invoke(new DefaultOAuth2ApiService(tokenBroker, callContext));
    Assertions.assertThat(response.getEntity())
        .isInstanceOf(OAuthTokenResponse.class)
        .asInstanceOf(InstanceOfAssertFactories.type(OAuthTokenResponse.class))
        .returns("token", OAuthTokenResponse::token);
  }

  @Test
  public void testAuthHeaderRequiresValidCredentialPair() {
    RealmContext realmContext = () -> "realm";
    TokenBroker tokenBroker = Mockito.mock();
    when(tokenBroker.supportsGrantType(TokenRequestValidator.TOKEN_EXCHANGE)).thenReturn(true);
    when(tokenBroker.supportsRequestedTokenType(TokenType.ACCESS_TOKEN)).thenReturn(true);
    when(tokenBroker.generateFromClientSecrets(
            null,
            "secret",
            TokenRequestValidator.TOKEN_EXCHANGE,
            "scope",
            callContext.getPolarisCallContext(),
            TokenType.ACCESS_TOKEN))
        .thenReturn(new TokenResponse("token", TokenType.ACCESS_TOKEN.getValue(), 3600));
    Response response =
        new InvocationBuilder()
            .authHeader(
                "Basic "
                    + Base64.getEncoder()
                        .encodeToString("secret".getBytes(Charset.defaultCharset())))
            .scope("scope")
            .grantType(TokenRequestValidator.TOKEN_EXCHANGE)
            .requestedTokenType(TokenType.ACCESS_TOKEN)
            .realmContext(realmContext)
            .invoke(new DefaultOAuth2ApiService(tokenBroker, callContext));
    Assertions.assertThat(response.getEntity())
        .isInstanceOf(OAuthTokenErrorResponse.class)
        .asInstanceOf(InstanceOfAssertFactories.type(OAuthTokenErrorResponse.class))
        .returns(
            OAuthTokenErrorResponse.Error.invalid_request.name(),
            OAuthTokenErrorResponse::getError);
  }

  @Test
  public void testReadClientSecretFromAuthHeader() {
    RealmContext realmContext = () -> "realm";
    TokenBroker tokenBroker = Mockito.mock();
    when(tokenBroker.supportsGrantType(TokenRequestValidator.TOKEN_EXCHANGE)).thenReturn(true);
    when(tokenBroker.supportsRequestedTokenType(TokenType.ACCESS_TOKEN)).thenReturn(true);

    when(tokenBroker.generateFromClientSecrets(
            "",
            "secret",
            TokenRequestValidator.TOKEN_EXCHANGE,
            "scope",
            callContext.getPolarisCallContext(),
            TokenType.ACCESS_TOKEN))
        .thenReturn(new TokenResponse("token", TokenType.ACCESS_TOKEN.getValue(), 3600));
    Response response =
        new InvocationBuilder()

            // here the auth header has a blank client id, providing a blank, but not null client id
            .authHeader(
                "Basic "
                    + Base64.getEncoder()
                        .encodeToString(":secret".getBytes(Charset.defaultCharset())))
            .scope("scope")
            .grantType(TokenRequestValidator.TOKEN_EXCHANGE)
            .requestedTokenType(TokenType.ACCESS_TOKEN)
            .realmContext(realmContext)
            .invoke(new DefaultOAuth2ApiService(tokenBroker, callContext));
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
    private String subjectToken;
    private TokenType subjectTokenType;
    private String actorToken;
    private TokenType actorTokenType;
    private RealmContext realmContext;
    private SecurityContext securityContext;

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

    public InvocationBuilder subjectToken(String subjectToken) {
      this.subjectToken = subjectToken;
      return this;
    }

    public InvocationBuilder subjectTokenType(TokenType subjectTokenType) {
      this.subjectTokenType = subjectTokenType;
      return this;
    }

    public InvocationBuilder actorToken(String actorToken) {
      this.actorToken = actorToken;
      return this;
    }

    public InvocationBuilder actorTokenType(TokenType actorTokenType) {
      this.actorTokenType = actorTokenType;
      return this;
    }

    public InvocationBuilder realmContext(RealmContext realmContext) {
      this.realmContext = realmContext;
      return this;
    }

    public InvocationBuilder securityContext(SecurityContext securityContext) {
      this.securityContext = securityContext;
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
          subjectToken,
          subjectTokenType,
          actorToken,
          actorTokenType,
          realmContext,
          securityContext);
    }
  }
}
