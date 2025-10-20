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
package org.apache.polaris.extension.auth.opa;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.QuarkusTestProfile.TestResourceEntry;
import io.quarkus.test.junit.TestProfile;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for OPA with file-based bearer token authentication.
 *
 * <p>These tests verify that OpaPolarisAuthorizer correctly reads bearer tokens from a file and
 * uses them to authenticate with OPA.
 */
@QuarkusTest
@TestProfile(OpaFileTokenIntegrationTest.FileTokenOpaProfile.class)
public class OpaFileTokenIntegrationTest extends OpaIntegrationTestBase {

  /**
   * Test profile for OPA integration with file-based bearer token authentication. The OPA
   * container runs with HTTP for simplicity in CI environments.
   */
  public static class FileTokenOpaProfile implements QuarkusTestProfile {
    // Static field to hold token file path for test access
    public static Path tokenFilePath;

    @Override
    public Map<String, String> getConfigOverrides() {
      try {
        // Create token file early so SmallRye Config validation sees the property
        tokenFilePath = Files.createTempFile("opa-test-token", ".txt");
        Files.writeString(tokenFilePath, "test-opa-bearer-token-from-file");

        Map<String, String> config = new HashMap<>();
        config.put("polaris.authorization.type", "opa");

        // Configure file-based bearer token authentication
        config.put("polaris.authorization.opa.auth.type", "bearer");
        config.put("polaris.authorization.opa.auth.bearer.type", "file-based");
        config.put(
            "polaris.authorization.opa.auth.bearer.file-based.path", tokenFilePath.toString());
        config.put("polaris.authorization.opa.auth.bearer.file-based.refresh-interval", "PT1S");

        return config;
      } catch (IOException e) {
        throw new RuntimeException("Failed to create test token file", e);
      }
    }

    @Override
    public List<TestResourceEntry> testResources() {
      return List.of(new TestResourceEntry(OpaTestResource.class));
    }
  }

  @Test
  void testOpaAllowsRootUser() {
    String rootToken = getRootToken();

    // Use the Bearer token to test OPA authorization
    // The JWT token has principal "root" which our policy allows
    given()
        .header("Authorization", "Bearer " + rootToken)
        .when()
        .get("api/management/v1/catalogs")
        .then()
        .statusCode(200); // Should succeed - "root" user is allowed by policy
  }

  @Test
  void testCreatePrincipalAndGetToken() {
    // Test the helper method createPrincipalAndGetToken
    // useful for debugging and ensuring that the helper method works correctly
    assertDoesNotThrow(
        () -> {
          createPrincipalAndGetToken("test-user");
        });
  }

  @Test
  void testOpaPolicyDeniesStrangerUser() {
    // Create a "stranger" principal and get its access token
    String strangerToken = createPrincipalAndGetToken("stranger");

    // Use the stranger token to test OPA authorization - should be denied
    given()
        .header("Authorization", "Bearer " + strangerToken)
        .when()
        .get("/api/management/v1/catalogs")
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
        .get("/api/management/v1/catalogs")
        .then()
        .statusCode(200); // Should succeed - admin user is allowed by policy
  }
}
