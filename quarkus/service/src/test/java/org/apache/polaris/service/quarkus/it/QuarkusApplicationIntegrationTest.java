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
package org.apache.polaris.service.quarkus.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthConfig;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.test.PolarisApplicationIntegrationTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(QuarkusApplicationIntegrationTest.Profile.class)
public class QuarkusApplicationIntegrationTest extends PolarisApplicationIntegrationTest {

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "quarkus.http.limits.max-body-size",
          "1000000",
          "polaris.realm-context.realms",
          "POLARIS,OTHER",
          "polaris.features.\"ALLOW_OVERLAPPING_CATALOG_URLS\"",
          "true",
          "polaris.features.\"SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION\"",
          "true",
          "polaris.features.\"ALLOW_SPECIFYING_FILE_IO_IMPL\"",
          "true",
          "polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"",
          "true",
          "polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"",
          "[\"FILE\",\"S3\"]",
          "polaris.readiness.ignore-severe-issues",
          "true");
    }
  }

  @Test
  public void testIcebergRestApiRefreshExpiredToken(
      PolarisApiEndpoints endpoints, ClientCredentials clientCredentials) throws IOException {
    String path = endpoints.catalogApiEndpoint() + "/v1/oauth/tokens";
    try (RESTClient client =
        HTTPClient.builder(Map.of())
            .withHeader(endpoints.realmHeaderName(), endpoints.realmId())
            .uri(path)
            .withAuthSession(AuthSession.EMPTY)
            .build()) {
      String credentialString =
          clientCredentials.clientId() + ":" + clientCredentials.clientSecret();
      String expiredToken =
          JWT.create().withExpiresAt(Instant.EPOCH).sign(Algorithm.HMAC256("irrelevant-secret"));
      var authConfig =
          AuthConfig.builder()
              .credential(credentialString)
              .scope("PRINCIPAL_ROLE:ALL")
              .oauth2ServerUri(path)
              .token(expiredToken)
              .build();

      var parentSession = new OAuth2Util.AuthSession(Map.of(), authConfig);
      var session =
          OAuth2Util.AuthSession.fromAccessToken(client, null, expiredToken, 0L, parentSession);

      assertThat(session.token()).isNotEqualTo(expiredToken); // implicit refresh
      assertThat(JWT.decode(session.token()).getExpiresAtAsInstant()).isAfter(Instant.EPOCH);
    }
  }

  @Test
  public void testIcebergRestApiRefreshValidToken(
      PolarisApiEndpoints endpoints, ClientCredentials clientCredentials) throws IOException {
    String path = endpoints.catalogApiEndpoint() + "/v1/oauth/tokens";
    try (RESTClient client =
        HTTPClient.builder(Map.of())
            .withHeader(endpoints.realmHeaderName(), endpoints.realmId())
            .uri(path)
            .withAuthSession(AuthSession.EMPTY)
            .build()) {
      var response =
          client.postForm(
              path,
              Map.of(
                  "grant_type",
                  "client_credentials",
                  "scope",
                  "PRINCIPAL_ROLE:ALL",
                  "client_id",
                  clientCredentials.clientId(),
                  "client_secret",
                  clientCredentials.clientSecret()),
              OAuthTokenResponse.class,
              Map.of(),
              ErrorHandlers.oauthErrorHandler());
      String token = response.token();
      var authConfig =
          AuthConfig.builder()
              .credential(clientCredentials.clientId() + ":" + clientCredentials.clientSecret())
              .scope("PRINCIPAL_ROLE:ALL")
              .oauth2ServerUri(path)
              .token(token)
              .build();
      var parentSession = new OAuth2Util.AuthSession(Map.of(), authConfig);
      var session = OAuth2Util.AuthSession.fromAccessToken(client, null, token, 0L, parentSession);
      session.refresh(client);
      assertThat(session.token()).isNotEqualTo(token);
      assertThat(JWT.decode(session.token()).getExpiresAtAsInstant()).isAfter(Instant.now());
    }
  }

  @Test
  public void testIcebergRestApiInvalidToken(
      PolarisApiEndpoints endpoints, ClientCredentials clientCredentials) throws IOException {
    String path = endpoints.catalogApiEndpoint() + "/v1/oauth/tokens";
    try (RESTClient client =
        HTTPClient.builder(Map.of())
            .withHeader(endpoints.realmHeaderName(), endpoints.realmId())
            .uri(path)
            .withAuthSession(AuthSession.EMPTY)
            .build()) {
      var response =
          client.postForm(
              path,
              Map.of(
                  "grant_type",
                  "client_credentials",
                  "scope",
                  "PRINCIPAL_ROLE:ALL",
                  "client_id",
                  clientCredentials.clientId(),
                  "client_secret",
                  clientCredentials.clientSecret()),
              OAuthTokenResponse.class,
              Map.of(),
              ErrorHandlers.oauthErrorHandler());
      String token = response.token();
      // mimics OAUth2Util.AuthSession refreshing the token
      assertThatThrownBy(
              () ->
                  client.postForm(
                      path,
                      Map.of(
                          "grant_type",
                          "urn:ietf:params:oauth:grant-type:token-exchange",
                          "scope",
                          "PRINCIPAL_ROLE:ALL",
                          "subject_token",
                          "invalid",
                          "subject_token_type",
                          "urn:ietf:params:oauth:token-type:access_token"),
                      OAuthTokenResponse.class,
                      Map.of("Authorization", "Bearer " + token),
                      ErrorHandlers.oauthErrorHandler()))
          .isInstanceOf(NotAuthorizedException.class)
          .hasMessageContaining("invalid_client");
    }
  }
}
