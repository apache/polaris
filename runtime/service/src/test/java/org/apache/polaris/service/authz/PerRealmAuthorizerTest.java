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
package org.apache.polaris.service.authz;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizerFactory;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.auth.oidc.TestPolarisAuthorizer;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the authorizer type and tenant key are resolved per realm at request time.
 *
 * <ul>
 *   <li>realm1: no authorizer config, falls back to the default (internal) authorizer
 *   <li>realm2: "per-realm-test" authorizer with the "deny" tenant, which denies LIST_CATALOGS
 *   <li>realm3: "per-realm-test" authorizer without tenant key, which allows everything
 * </ul>
 */
@QuarkusTest
@TestProfile(PerRealmAuthorizerTest.Profile.class)
public class PerRealmAuthorizerTest {

  private static final String REALM_HEADER = "Polaris-Realm";

  record Invocation(String realm, @Nullable String tenantKey) {}

  /** Records the (realm, tenant key) pairs the "per-realm-test" factory was invoked with. */
  static final List<Invocation> INVOCATIONS = new CopyOnWriteArrayList<>();

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.realm-context.realms",
          "realm1,realm2,realm3",
          "polaris.bootstrap.credentials",
          "realm1,client1,secret1;realm2,client2,secret2;realm3,client3,secret3",
          "polaris.authorization.authorizer.realm2.type",
          "per-realm-test",
          "polaris.authorization.authorizer.realm2.tenant-key",
          "deny",
          "polaris.authorization.authorizer.realm3.type",
          "per-realm-test");
    }

    @ApplicationScoped
    @Identifier("per-realm-test")
    public PolarisAuthorizerFactory testAuthorizerFactory(RealmContext realmContext) {
      return tenantKey -> {
        INVOCATIONS.add(new Invocation(realmContext.getRealmIdentifier(), tenantKey));
        TestPolarisAuthorizer authorizer = new TestPolarisAuthorizer();
        if ("deny".equals(tenantKey)) {
          authorizer.setDecision(
              PolarisAuthorizableOperation.LIST_CATALOGS,
              AuthorizationDecision.deny("denied by tenant 'deny'"));
        }
        return authorizer;
      };
    }
  }

  @BeforeEach
  void reset() {
    INVOCATIONS.clear();
  }

  @Test
  void defaultAuthorizerForUnconfiguredRealm() {
    listCatalogs("realm1", "client1", "secret1", 200);
    assertThat(INVOCATIONS).isEmpty();
  }

  @Test
  void realmSpecificAuthorizerWithTenantKey() {
    listCatalogs("realm2", "client2", "secret2", 403);
    assertThat(INVOCATIONS).contains(new Invocation("realm2", "deny"));
  }

  @Test
  void realmSpecificAuthorizerWithoutTenantKey() {
    listCatalogs("realm3", "client3", "secret3", 200);
    assertThat(INVOCATIONS).contains(new Invocation("realm3", null));
  }

  private static void listCatalogs(
      String realm, String clientId, String clientSecret, int expectedStatus) {
    String token =
        given()
            .header(REALM_HEADER, realm)
            .contentType(ContentType.URLENC)
            .formParam("grant_type", "client_credentials")
            .formParam("scope", "PRINCIPAL_ROLE:ALL")
            .formParam("client_id", clientId)
            .formParam("client_secret", clientSecret)
            .when()
            .post("/api/catalog/v1/oauth/tokens")
            .then()
            .statusCode(200)
            .extract()
            .path("access_token");
    given()
        .header(REALM_HEADER, realm)
        .auth()
        .oauth2(token)
        .when()
        .get("/api/management/v1/catalogs")
        .then()
        .statusCode(expectedStatus);
  }
}
