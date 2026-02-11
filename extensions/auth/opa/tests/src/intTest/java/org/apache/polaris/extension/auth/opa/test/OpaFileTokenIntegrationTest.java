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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for OPA with file-based bearer token authentication.
 *
 * <p>These tests verify that OpaPolarisAuthorizer correctly reads bearer tokens from a file and
 * uses them to authenticate with OPA.
 */
@QuarkusTest
@TestProfile(OpaTestProfiles.FileToken.class)
public class OpaFileTokenIntegrationTest extends OpaIntegrationTestBase {

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
  void testOpaDeniesRbacPrincipalCreation() {
    String rootToken = getRootToken();

    Map<String, Object> createPrincipalBody =
        Map.of("principal", Map.of("name", "opa-test-user", "properties", Map.of()));
    given()
        .contentType("application/json")
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(createPrincipalBody))
        .when()
        .post("/api/management/v1/principals")
        .then()
        .statusCode(403);
  }

  @Test
  void testOpaDeniesRbacPrincipalRoleCreation() {
    String rootToken = getRootToken();

    given()
        .contentType("application/json")
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(Map.of("name", "opa-test-role", "properties", Map.of())))
        .post("/api/management/v1/principal-roles")
        .then()
        .statusCode(403);
  }
}
