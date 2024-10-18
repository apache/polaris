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
import static org.apache.polaris.service.auth.JWTBroker.CLAIM_KEY_ACTIVE;
import static org.apache.polaris.service.auth.JWTBroker.CLAIM_KEY_CLIENT_ID;
import static org.apache.polaris.service.auth.JWTBroker.CLAIM_KEY_PRINCIPAL_ID;
import static org.apache.polaris.service.auth.JWTBroker.CLAIM_KEY_SCOPE;
import static org.apache.polaris.service.auth.JWTBroker.ISSUER_KEY;
import static org.apache.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.auth0.jwt.algorithms.Algorithm;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.UUID;
import org.apache.polaris.service.PolarisApplication;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.test.PolarisConnectionExtension;
import org.apache.polaris.service.test.PolarisRealm;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.org.awaitility.Awaitility;

/** This test class is to validate altered JWT tokens. */
@ExtendWith({DropwizardExtensionsSupport.class, PolarisConnectionExtension.class})
public class JWTIntegrationTest {
  private static final DropwizardAppExtension<PolarisApplicationConfig> EXT =
      new DropwizardAppExtension<>(
          PolarisApplication.class,
          ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
          ConfigOverride.config(
              "server.applicationConnectors[0].port",
              "0"), // Bind to random port to support parallelism
          ConfigOverride.config("server.adminConnectors[0].port", "0"),

          // disallow FILE urls for the sake of tests below
          ConfigOverride.config(
              "featureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES", "S3,GCS,AZURE"));
  private static String realm;
  private static String clientId;

  @BeforeAll
  public static void setup(
      PolarisConnectionExtension.PolarisToken adminToken, @PolarisRealm String polarisRealm)
      throws IOException {
    realm = polarisRealm;
    Base64.Decoder decoder = Base64.getUrlDecoder();
    String[] chunks = adminToken.token().split("\\.");
    String payload = new String(decoder.decode(chunks[1]), UTF_8);
    JsonElement jsonElement = JsonParser.parseString(payload);
    clientId = String.valueOf(((JsonObject) jsonElement).get("client_id"));

    // Set up test location
    PolarisConnectionExtension.createTestDir(realm);
  }

  @Test
  public void testTokenExpiry() {
    // TokenExpiredException - if the token has expired.
    String newToken =
        defaultJwt()
            .withExpiresAt(Instant.now().plus(1, ChronoUnit.SECONDS))
            .sign(Algorithm.HMAC256("polaris"));
    Awaitility.await("expected list of records should be produced")
        .atMost(Duration.ofSeconds(2))
        .pollDelay(Duration.ofSeconds(1))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              try (Response response =
                  newRequest(
                          "http://localhost:%d/api/management/v1/principals", "Bearer " + newToken)
                      .get()) {
                assertThat(response)
                    .returns(Response.Status.UNAUTHORIZED.getStatusCode(), Response::getStatus);
              }
            });
  }

  @Test
  public void testTokenInactive() {
    // InvalidClaimException - if a claim contained a different value than the expected one.
    String newToken =
        defaultJwt().withClaim(CLAIM_KEY_ACTIVE, false).sign(Algorithm.HMAC256("polaris"));
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals", "Bearer " + newToken)
            .get()) {
      assertThat(response)
          .returns(Response.Status.UNAUTHORIZED.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testTokenInvalidSignature() {
    // SignatureVerificationException - if the signature is invalid.
    String newToken = defaultJwt().sign(Algorithm.HMAC256("invalid_secret"));
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals", "Bearer " + newToken)
            .get()) {
      assertThat(response)
          .returns(Response.Status.UNAUTHORIZED.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testTokenInvalidPrincipalId() {
    String newToken =
        defaultJwt().withClaim(CLAIM_KEY_PRINCIPAL_ID, 0).sign(Algorithm.HMAC256("polaris"));
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/principals", "Bearer " + newToken)
            .get()) {
      assertThat(response)
          .returns(Response.Status.UNAUTHORIZED.getStatusCode(), Response::getStatus);
    }
  }

  private static Invocation.Builder newRequest(String url, String token) {
    return EXT.client()
        .target(String.format(url, EXT.getLocalPort()))
        .request("application/json")
        .header("Authorization", token)
        .header(REALM_PROPERTY_KEY, realm);
  }

  public static JWTCreator.Builder defaultJwt() {
    Instant now = Instant.now();
    return JWT.create()
        .withIssuer(ISSUER_KEY)
        .withSubject(String.valueOf(1))
        .withIssuedAt(now)
        .withExpiresAt(now.plus(10, ChronoUnit.SECONDS))
        .withJWTId(UUID.randomUUID().toString())
        .withClaim(CLAIM_KEY_ACTIVE, true)
        .withClaim(CLAIM_KEY_CLIENT_ID, clientId)
        .withClaim(CLAIM_KEY_PRINCIPAL_ID, 1)
        .withClaim(CLAIM_KEY_SCOPE, BasePolarisAuthenticator.PRINCIPAL_ROLE_ALL);
  }
}
