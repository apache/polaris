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
package org.apache.polaris.extension.auth.ranger.test;

import static io.restassured.RestAssured.given;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(RangerTestProfiles.EmbeddedPolicy.class)
public class RangerIntegrationTest extends RangerIntegrationTestBase {

  @Test
  void testRangerAllowsRootUser() {
    String rootToken = getRootToken();

    given()
        .header("Authorization", "Bearer " + rootToken)
        .when()
        .get("/api/management/v1/catalogs")
        .then()
        .statusCode(200);
  }

  @Test
  void testRangerDeniesUser2ListCatalogs() {
    String user2Token = createPrincipalAndGetToken("user2");

    given()
        .header("Authorization", "Bearer " + user2Token)
        .when()
        .get("/api/management/v1/catalogs")
        .then()
        .statusCode(403);
  }

  @Test
  void testRangerAllowsAdmin1User() {
    String adminToken = createPrincipalAndGetToken("admin1");

    given()
        .header("Authorization", "Bearer " + adminToken)
        .when()
        .get("/api/management/v1/catalogs")
        .then()
        .statusCode(200);
  }

  @Test
  void testUser1ListCatalogsAllowedButCreateCatalogDenied() throws Exception {
    String user1Token = createPrincipalAndGetToken("user1");

    given()
        .header("Authorization", "Bearer " + user1Token)
        .when()
        .get("/api/management/v1/catalogs")
        .then()
        .statusCode(200);

    String catalogName = "ranger-user1-cat-" + UUID.randomUUID().toString().replace("-", "");
    String baseLocation = Files.createTempDirectory("ranger-user1-cat").toUri().toString();
    Map<String, Object> body =
        Map.of(
            "type",
            "INTERNAL",
            "name",
            catalogName,
            "properties",
            Map.of("default-base-location", baseLocation),
            "storageConfigInfo",
            Map.of("storageType", "FILE", "allowedLocations", List.of(baseLocation)));

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + user1Token)
        .body(toJson(body))
        .post("/api/management/v1/catalogs")
        .then()
        .statusCode(403);
  }
}
