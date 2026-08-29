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
package org.apache.polaris.service.auth;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.path.json.JsonPath;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Verifies that enabling {@code quarkus.oidc.resource-metadata.enabled} causes Polaris to serve the
 * OAuth 2.0 Protected Resource Metadata endpoint ({@code /.well-known/oauth-protected-resource} per
 * RFC 9728). The endpoint lets catalog clients auto-discover the authorization server URL from the
 * catalog URI without requiring explicit configuration.
 */
@QuarkusTest
@TestProfile(OAuthProtectedResourceMetadataTest.ResourceMetadataProfile.class)
class OAuthProtectedResourceMetadataTest {

  static final String AUTH_SERVER_URL = "https://auth.example.com/realms/polaris";

  // The catalog resource URI that clients would use to derive the well-known path.
  // RFC 9728: GET /.well-known/oauth-protected-resource<path> where <path> is the
  // path component of the resource URI.
  static final String CATALOG_RESOURCE = "https://catalog.example.com/api/catalog";

  public static class ResourceMetadataProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.ofEntries(
          // Enable the OIDC tenant so the ResourceMetadataHandler registers the route.
          Map.entry("quarkus.oidc.tenant-enabled", "true"),
          // Disable OIDC discovery so Quarkus does not attempt to contact the auth server
          // at startup. The well-known endpoint is public and requires no JWT validation.
          Map.entry("quarkus.oidc.discovery-enabled", "false"),
          Map.entry("quarkus.oidc.auth-server-url", "https://auth.example.com/realms/polaris"),
          // Required by Quarkus when discovery is disabled; not called by the test since
          // /.well-known/oauth-protected-resource is unauthenticated.
          Map.entry("quarkus.oidc.jwks-path", "protocol/openid-connect/certs"),
          // Defer JWKS loading to first authenticated request so startup does not attempt to
          // fetch JWKS from the test auth-server-url. Without this the tenant stays "not ready"
          // and ResourceMetadataHandler never registers the route.
          Map.entry("quarkus.oidc.jwks.resolve-early", "false"),
          Map.entry("quarkus.oidc.resource-metadata.enabled", "true"),
          Map.entry("quarkus.oidc.resource-metadata.resource", CATALOG_RESOURCE),
          Map.entry("quarkus.oidc.resource-metadata.authorization-server", AUTH_SERVER_URL));
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
        .containsExactly(AUTH_SERVER_URL);
  }
}
