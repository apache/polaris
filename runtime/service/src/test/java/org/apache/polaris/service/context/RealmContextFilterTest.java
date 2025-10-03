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

package org.apache.polaris.service.context;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;

import io.quarkus.test.common.http.TestHTTPEndpoint;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import org.apache.polaris.service.catalog.api.IcebergRestOAuth2Api;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestHTTPEndpoint(IcebergRestOAuth2Api.class)
@TestProfile(RealmContextFilterTest.Profile.class)
@SuppressWarnings("UastIncorrectHttpHeaderInspection")
class RealmContextFilterTest {

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.realm-context.header-name",
          REALM_HEADER,
          "polaris.realm-context.realms",
          "realm1,realm2",
          "polaris.bootstrap.credentials",
          "realm1,client1,secret1;realm2,client2,secret2");
    }
  }

  private static final String REALM_HEADER = "test-header-r123";

  @Test
  public void testInvalidRealmHeaderValue() {
    givenTokenRequest("client1", "secret1")
        .header(REALM_HEADER, "INVALID")
        .when()
        .post()
        .then()
        .statusCode(Response.Status.NOT_FOUND.getStatusCode())
        .body("error.message", is("Missing or invalid realm"))
        .body("error.type", is("MissingOrInvalidRealm"))
        .body("error.code", is(Response.Status.NOT_FOUND.getStatusCode()));
  }

  @Test
  public void testNoRealmHeader() {
    // The default realm is "realm1" so the second pair of secrets is not valid without
    // an explicit header
    givenTokenRequest("client2", "secret2")
        .header("irrelevant-header", "fake-realm")
        .when()
        .post()
        .then()
        .statusCode(Response.Status.UNAUTHORIZED.getStatusCode());
  }

  @Test
  public void testDefaultRealm() {
    // The default realm is "realm1", now credentials match
    givenTokenRequest("client1", "secret1")
        .header("irrelevant-header", "fake-realm")
        .when()
        .post()
        .then()
        .statusCode(Response.Status.OK.getStatusCode());
  }

  @Test
  public void testValidRealmHeaderDefaultRealm() {
    givenTokenRequest("client2", "secret2")
        .header(REALM_HEADER, "realm2")
        .when()
        .post()
        .then()
        .statusCode(Response.Status.OK.getStatusCode());
  }

  private static RequestSpecification givenTokenRequest(String clientId, String clientSecret) {
    return given()
        .contentType(ContentType.URLENC)
        .formParam("grant_type", "client_credentials")
        .formParam("scope", "PRINCIPAL_ROLE:ALL")
        .formParam("client_id", clientId)
        .formParam("client_secret", clientSecret);
  }
}
