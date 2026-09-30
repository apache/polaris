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
package org.apache.polaris.persistence.nosql.cassandra;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CassandraMtlsSmokeTest {

  @Test
  void persistsPrincipalThroughServerRuntime() {
    var port = Integer.getInteger("quarkus.http.test-port");
    assertThat(port).isNotNull();
    var server = new RequestSpecBuilder().setBaseUri("http://localhost").setPort(port).build();
    String token =
        given()
            .spec(server)
            .contentType(ContentType.URLENC)
            .formParam("grant_type", "client_credentials")
            .formParam("client_id", "test-admin")
            .formParam("client_secret", "test-secret")
            .formParam("scope", "PRINCIPAL_ROLE:ALL")
            .post("/api/catalog/v1/oauth/tokens")
            .then()
            .statusCode(200)
            .extract()
            .path("access_token");

    var principalName = "server-smoke-" + UUID.randomUUID();
    var createdPrincipal =
        given()
            .spec(server)
            .auth()
            .oauth2(token)
            .contentType(ContentType.JSON)
            .body(
                """
                {"principal":{"name":"%s","properties":{"smoke-test":"Cassandra"}}}
                """
                    .formatted(principalName))
            .post("/api/management/v1/principals")
            .then()
            .statusCode(201)
            .extract()
            .jsonPath();
    var fetchedPrincipal =
        given()
            .spec(server)
            .auth()
            .oauth2(token)
            .get("/api/management/v1/principals/{name}", principalName)
            .then()
            .statusCode(200)
            .extract()
            .jsonPath();

    assertThat(fetchedPrincipal.getString("name")).isEqualTo(principalName);
    assertThat(fetchedPrincipal.getString("clientId"))
        .isEqualTo(createdPrincipal.getString("principal.clientId"));
    assertThat(fetchedPrincipal.<String, String>getMap("properties"))
        .containsAllEntriesOf(Map.of("smoke-test", "Cassandra"));

    var managementPort = Integer.getInteger("quarkus.management.test-port");
    assertThat(managementPort).isNotNull();
    given()
        .port(managementPort)
        .get("/q/health/ready")
        .then()
        .statusCode(200)
        .body("status", equalTo("UP"))
        .body("checks.find { it.name == 'Cassandra health check' }.status", equalTo("UP"));
    var metrics =
        given().port(managementPort).get("/q/metrics").then().statusCode(200).extract().asString();
    assertThat(metrics).contains("cassandra");
  }
}
