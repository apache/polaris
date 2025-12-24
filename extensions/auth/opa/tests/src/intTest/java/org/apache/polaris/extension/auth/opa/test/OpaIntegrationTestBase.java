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

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import io.restassured.config.ObjectMapperConfig;
import io.restassured.http.ContentType;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;

/**
 * Base class for OPA integration tests providing common helper methods for authentication and
 * principal management.
 */
public abstract class OpaIntegrationTestBase {

  private static final ObjectMapper JSON = new ObjectMapper();
  private final List<String> catalogsToCleanup = new ArrayList<>();

  static {
    RestAssured.config =
        RestAssured.config()
            .objectMapperConfig(
                ObjectMapperConfig.objectMapperConfig()
                    .jackson2ObjectMapperFactory((cls, charset) -> new ObjectMapper()));
  }

  protected String toJson(Object value) {
    try {
      return JSON.writeValueAsString(value);
    } catch (Exception e) {
      throw new UncheckedIOException("Failed to serialize to JSON", new java.io.IOException(e));
    }
  }

  /**
   * Helper method to get root access token using the default test admin credentials.
   *
   * @return the access token for the root user
   */
  protected String getRootToken() {
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

  /**
   * Helper method to create a principal and get an OAuth access token for that principal.
   *
   * @param principalName the name of the principal to create
   * @return the access token for the newly created principal
   */
  protected String createPrincipalAndGetToken(String principalName) {
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

  /**
   * Simple JSON value extractor for parsing values from JSON responses.
   *
   * @param json the JSON string to parse
   * @param key the key to extract the value for
   * @return the extracted value, or null if not found
   */
  protected String extractJsonValue(String json, String key) {
    String searchKey = "\"" + key + "\"";
    if (json.contains(searchKey)) {
      String value = json.substring(json.indexOf(searchKey) + searchKey.length());
      value = value.substring(value.indexOf("\"") + 1);
      value = value.substring(0, value.indexOf("\""));
      return value;
    }
    return null;
  }

  @AfterEach
  void cleanupCatalogs() {
    // Use root token for cleanup to avoid cascading auth failures
    String rootToken;
    try {
      rootToken = getRootToken();
    } catch (Exception e) {
      return;
    }
    List<String> reversed = new ArrayList<>(catalogsToCleanup);
    Collections.reverse(reversed);
    for (String catalog : reversed) {
      try {
        given()
            .header("Authorization", "Bearer " + rootToken)
            .delete("/api/management/v1/catalogs/{cat}", catalog)
            .then()
            .statusCode(org.hamcrest.Matchers.anything());
      } catch (Exception ignored) {
        // best effort
      }
    }
    catalogsToCleanup.clear();
  }

  protected String createFileCatalog(
      String token, String catalogName, String baseLocation, List<String> allowedLocations) {
    // Create a File Catalog to use for testing
    Map<String, Object> body =
        Map.of(
            "type",
            "INTERNAL",
            "name",
            catalogName,
            "properties",
            Map.of("default-base-location", baseLocation),
            "storageConfigInfo",
            Map.of("storageType", "FILE", "allowedLocations", allowedLocations));

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + token)
        .body(toJson(body))
        .post("/api/management/v1/catalogs")
        .then()
        .statusCode(201);
    catalogsToCleanup.add(catalogName);
    return baseLocation;
  }

  protected void createNamespace(String token, String catalogName, String namespace) {
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + token)
        .body("{\"namespace\":[\"" + namespace + "\"]}")
        .post("/api/catalog/v1/{cat}/namespaces", catalogName)
        .then()
        .statusCode(200);
  }
}
