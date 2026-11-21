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
package org.apache.polaris.service.auth.internal;

import static io.restassured.RestAssured.given;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Tokens are only accepted by the realm they were issued for. */
@QuarkusTest
@TestProfile(InternalTokenRealmBindingTest.Profile.class)
class InternalTokenRealmBindingTest {

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.realm-context.realms",
          "realm1,realm2",
          "polaris.bootstrap.credentials",
          "realm1,client1,secret1;realm2,client2,secret2",
          "polaris.authentication.token-broker.type",
          "symmetric-key",
          "polaris.authentication.token-broker.symmetric-key.secret",
          "secret");
    }
  }

  private static final String REALM_HEADER = "Polaris-Realm";

  @ParameterizedTest
  @CsvSource({"realm1, client1, secret1, realm2", "realm2, client2, secret2, realm1"})
  void tokenIsOnlyAcceptedInIssuingRealm(
      String realm, String clientId, String clientSecret, String otherRealm) {
    String token = fetchToken(realm, clientId, clientSecret);

    listPrincipalRoles(realm, token).statusCode(Response.Status.OK.getStatusCode());
    listPrincipalRoles(otherRealm, token).statusCode(Response.Status.UNAUTHORIZED.getStatusCode());
  }

  private static String fetchToken(String realm, String clientId, String clientSecret) {
    return given()
        .header(REALM_HEADER, realm)
        .contentType(ContentType.URLENC)
        .formParam("grant_type", "client_credentials")
        .formParam("scope", "PRINCIPAL_ROLE:ALL")
        .formParam("client_id", clientId)
        .formParam("client_secret", clientSecret)
        .when()
        .post("/api/catalog/v1/oauth/tokens")
        .then()
        .statusCode(Response.Status.OK.getStatusCode())
        .extract()
        .path("access_token");
  }

  private static ValidatableResponse listPrincipalRoles(String realm, String token) {
    return given()
        .header(REALM_HEADER, realm)
        .header("Authorization", "Bearer " + token)
        .when()
        .get("/api/management/v1/principal-roles")
        .then();
  }
}
