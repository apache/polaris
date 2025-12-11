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
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.QuarkusTestProfile.TestResourceEntry;
import io.quarkus.test.junit.TestProfile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Integration test that exercises OPA authorization when Polaris uses the external authenticator.
 *
 * <p>Authentication is driven entirely by test headers via {@link
 * TestExternalHeaderAuthenticationMechanism}, ensuring no internal principal lookups occur.
 */
@QuarkusTest
@TestProfile(OpaExternalAuthIntegrationTest.ExternalOpaProfile.class)
public class OpaExternalAuthIntegrationTest extends OpaIntegrationTestBase {

  public static class ExternalOpaProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("polaris.authorization.type", "opa");
      config.put("polaris.authorization.opa.auth.type", "bearer");
      config.put(
          "polaris.authorization.opa.auth.bearer.static-token.value",
          "test-opa-bearer-token-12345");

      // Enable external authentication and skip internal token services
      config.put("polaris.authentication.type", "external");
      config.put("polaris.authentication.authenticator.type", "external");
      config.put("polaris.authentication.token-broker.type", "none");
      config.put("polaris.authentication.token-service.type", "disabled");

      // OIDC not used in this flow
      config.put("quarkus.oidc.enabled", "false");

      return config;
    }

    @Override
    public List<TestResourceEntry> testResources() {
      return List.of(new TestResourceEntry(OpaTestResource.class));
    }
  }

  @Test
  void testExternalPrincipalAllowed() {
    given()
        .header(TestExternalHeaderAuthenticationMechanism.PRINCIPAL_HEADER, "admin")
        .header(TestExternalHeaderAuthenticationMechanism.ROLES_HEADER, "ext-role")
        .when()
        .get("/api/management/v1/catalogs")
        .then()
        .statusCode(200);
  }

  @Test
  void testExternalPrincipalDenied() {
    given()
        .header(TestExternalHeaderAuthenticationMechanism.PRINCIPAL_HEADER, "stranger")
        .header(TestExternalHeaderAuthenticationMechanism.ROLES_HEADER, "unknown-role")
        .when()
        .get("/api/management/v1/catalogs")
        .then()
        .statusCode(403);
  }
}
