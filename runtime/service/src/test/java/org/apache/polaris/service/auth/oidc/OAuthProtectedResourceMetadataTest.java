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
package org.apache.polaris.service.auth.oidc;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.path.json.JsonPath;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Verifies that enabling {@code quarkus.oidc.resource-metadata.enabled} causes Polaris to serve the
 * OAuth 2.0 Protected Resource Metadata endpoint ({@code /.well-known/oauth-protected-resource} per
 * RFC 9728), advertising the authorization server backing the WireMock-issued OIDC tenant used by
 * {@link AbstractOidcAuthTest}.
 *
 * <p>This is purely a discovery document: Quarkus 3.40 does not add a {@code resource_metadata}
 * hint to {@code WWW-Authenticate} challenges on unauthenticated requests (verified against the
 * {@code quarkus-oidc}/{@code quarkus-vertx-http} 3.40.1.1 bytecode -- no such string exists), so
 * there is nothing to assert there.
 *
 * <p>See {@link OAuthProtectedResourceMetadataDisabledTest} for the disabled-by-default case.
 */
@QuarkusTest
@TestProfile(OAuthProtectedResourceMetadataTest.Profile.class)
class OAuthProtectedResourceMetadataTest extends AbstractOidcAuthTest {

  // The catalog resource URI that clients would use to derive the well-known path.
  // RFC 9728: GET /.well-known/oauth-protected-resource<path> where <path> is the
  // path component of the resource URI.
  static final String CATALOG_RESOURCE = "https://catalog.example.com/api/catalog";

  public static class Profile extends AbstractOidcAuthTest.Profile {

    @Override
    public Map<String, String> getConfigOverrides() {
      ImmutableMap.Builder<String, String> config = ImmutableMap.builder();
      config.putAll(super.getConfigOverrides());
      config.put("quarkus.oidc.resource-metadata.enabled", "true");
      config.put("quarkus.oidc.resource-metadata.resource", CATALOG_RESOURCE);
      // Same real issuer the WireMock OIDC tenant is configured against (see
      // AbstractOidcAuthTest.Profile), so the assertion below is checking that the real
      // authorization server is correctly advertised, not a value we invented for the test.
      config.put(
          "quarkus.oidc.resource-metadata.authorization-server", "${keycloak.url}/realms/quarkus");
      return config.build();
    }
  }

  @Test
  void wellKnownEndpointAdvertisesAuthorizationServer() {
    // RFC 9728 section 4: the well-known URI is formed by inserting
    // /.well-known/oauth-protected-resource before the path component of the resource URI.
    // A client starting from CATALOG_RESOURCE derives this path automatically.
    String body =
        given()
            .get("/.well-known/oauth-protected-resource/api/catalog")
            .then()
            .statusCode(200)
            .extract()
            .body()
            .asString();

    JsonPath json = JsonPath.from(body);
    assertThat(json.getString("resource")).isEqualTo(CATALOG_RESOURCE);
    assertThat(json.getList("authorization_servers", String.class))
        .hasSize(1)
        .first()
        .satisfies(issuer -> assertThat(issuer).asString().endsWith("/realms/quarkus"));
  }
}
