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

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.fail;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.QuarkusTestProfile.TestResourceEntry;
import io.quarkus.test.junit.TestProfile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.test.commons.OpaTestResource;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(OpaIntegrationTest.StaticTokenOpaProfile.class)
public class OpaIntegrationTest {

  /**
   * Test demonstrates OPA integration with bearer token authentication. The OPA container runs with
   * HTTP for simplicity in CI environments. The OpaPolarisAuthorizer is configured to disable SSL
   * verification for test purposes.
   */
  public static class StaticTokenOpaProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("polaris.authorization.type", "opa");
      config.put("polaris.authorization.opa.policy-path", "/v1/data/polaris/authz");
      config.put("polaris.authorization.opa.http.timeout-ms", "2000");

      // Configure OPA server authentication with static bearer token
      config.put("polaris.authorization.opa.auth.type", "bearer");
      config.put("polaris.authorization.opa.auth.bearer.type", "static-token");
      config.put(
          "polaris.authorization.opa.auth.bearer.static-token.value", "test-opa-bearer-token-12345");
      config.put(
          "polaris.authorization.opa.http.verify-ssl", "false"); // Disable SSL verification for tests

      // TODO: Add tests for OIDC and federated principal
      config.put("polaris.authentication.type", "internal");

      return config;
    }

    @Override
    public List<TestResourceEntry> testResources() {
      String customRegoPolicy =
          """
        package polaris.authz

        default allow := false

        # Allow root user for all operations
        allow {
          input.actor.principal == "root"
        }

        # Allow admin user for all operations
        allow {
          input.actor.principal == "admin"
        }

        # Deny stranger user explicitly (though default is false)
        allow {
          input.actor.principal == "stranger"
          false
        }
        """;

      return List.of(
          new TestResourceEntry(
              OpaTestResource.class,
              Map.of("policy-name", "polaris-authz", "rego-policy", customRegoPolicy)));
    }
  }

  @Test
  void testOpaAllowsRootUser() {
    // Test demonstrates the complete integration flow:
    // 1. OAuth token acquisition with internal authentication
    // 2. OPA policy allowing root users

    // Get a token using the catalog service OAuth endpoint
    String response =
        given()
            .contentType("application/x-www-form-urlencoded")
            .formParam("grant_type", "client_credentials")
            .formParam("client_id", "test-admin")
            .formParam("client_secret", "test-secret")
            .formParam("scope", "PRINCIPAL_ROLE:ALL")
            .when()
            .post("/api/catalog/v1/oauth/tokens")
            .then()
            .statusCode(200)
            .extract()
            .body()
            .asString();

    // Parse JSON response to get access_token
    String accessToken = null;
    if (response.contains("\"access_token\"")) {
      accessToken = response.substring(response.indexOf("\"access_token\"") + 15);
      accessToken = accessToken.substring(accessToken.indexOf("\"") + 1);
      accessToken = accessToken.substring(0, accessToken.indexOf("\""));
    }

    if (accessToken == null) {
      fail("Failed to parse access_token from OAuth response: " + response);
    }

    // Use the Bearer token to test OPA authorization
    // The JWT token has principal "root" which our policy allows
    given()
        .header("Authorization", "Bearer " + accessToken)
        .when()
        .get("/api/management/v1/principals")
        .then()
        .statusCode(200); // Should succeed - "root" user is allowed by policy
  }

  @Test
  void testOpaPolicyDeniesStrangerUser() {
    // Create a "stranger" principal and get its access token
    String strangerToken = createPrincipalAndGetToken("stranger");

    // Use the stranger token to test OPA authorization - should be denied
    given()
        .header("Authorization", "Bearer " + strangerToken)
        .when()
        .get("/api/management/v1/principals")
        .then()
        .statusCode(403); // Should be forbidden by OPA policy - stranger is denied
  }

  @Test
  void testOpaAllowsAdminUser() {
    // Create an "admin" principal and get its access token
    String adminToken = createPrincipalAndGetToken("admin");

    // Use the admin token to test OPA authorization - should be allowed
    given()
        .header("Authorization", "Bearer " + adminToken)
        .when()
        .get("/api/management/v1/principals")
        .then()
        .statusCode(200); // Should succeed - admin user is allowed by policy
  }

  @Test
  void testOpaBearerTokenAuthentication() {
    // Test that OpaPolarisAuthorizer is configured to send bearer tokens
    // and can handle HTTP connections for testing
    String rootToken = getRootToken();

    given()
        .header("Authorization", "Bearer " + rootToken)
        .when()
        .get("/api/management/v1/principals")
        .then()
        .statusCode(200);
  }

  /** Helper method to create a principal and get an OAuth access token for that principal */
  private String createPrincipalAndGetToken(String principalName) {
    // First get root token to create the principal
    String rootToken = getRootToken();

    // Create the principal using the root token
    String createResponse =
        given()
            .contentType("application/json")
            .header("Authorization", "Bearer " + rootToken)
            .body("{\"principal\":{\"name\":\"" + principalName + "\",\"properties\":{}}}")
            .when()
            .post("/api/management/v1/principals")
            .then()
            .statusCode(201)
            .extract()
            .body()
            .asString();

    // Parse the principal's credentials from the response
    String clientId = extractJsonValue(createResponse, "clientId");
    String clientSecret = extractJsonValue(createResponse, "clientSecret");

    if (clientId == null || clientSecret == null) {
      fail("Could not parse principal credentials from response: " + createResponse);
    }

    // Get access token for the newly created principal
    String tokenResponse =
        given()
            .contentType("application/x-www-form-urlencoded")
            .formParam("grant_type", "client_credentials")
            .formParam("client_id", clientId)
            .formParam("client_secret", clientSecret)
            .formParam("scope", "PRINCIPAL_ROLE:ALL")
            .when()
            .post("/api/catalog/v1/oauth/tokens")
            .then()
            .statusCode(200)
            .extract()
            .body()
            .asString();

    String accessToken = extractJsonValue(tokenResponse, "access_token");
    if (accessToken == null) {
      fail("Could not get access token for principal " + principalName);
    }

    return accessToken;
  }

  /** Helper method to get root access token */
  private String getRootToken() {
    String response =
        given()
            .contentType("application/x-www-form-urlencoded")
            .formParam("grant_type", "client_credentials")
            .formParam("client_id", "test-admin")
            .formParam("client_secret", "test-secret")
            .formParam("scope", "PRINCIPAL_ROLE:ALL")
            .when()
            .post("/api/catalog/v1/oauth/tokens")
            .then()
            .statusCode(200)
            .extract()
            .body()
            .asString();

    String accessToken = extractJsonValue(response, "access_token");
    if (accessToken == null) {
      fail("Failed to parse access_token from admin OAuth response: " + response);
    }
    return accessToken;
  }

  /** Simple JSON value extractor */
  private String extractJsonValue(String json, String key) {
    String searchKey = "\"" + key + "\"";
    if (json.contains(searchKey)) {
      String value = json.substring(json.indexOf(searchKey) + searchKey.length());
      value = value.substring(value.indexOf("\"") + 1);
      value = value.substring(0, value.indexOf("\""));
      return value;
    }
    return null;
  }
}
