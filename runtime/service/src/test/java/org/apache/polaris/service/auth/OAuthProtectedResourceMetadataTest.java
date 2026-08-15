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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.notNullValue;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
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
          Map.entry("quarkus.oidc.resource-metadata.enabled", "true"),
          Map.entry("quarkus.oidc.resource-metadata.authorization-server", AUTH_SERVER_URL));
    }
  }

  @Test
  void wellKnownEndpointAdvertisesAuthorizationServer() {
    given()
        .get("/.well-known/oauth-protected-resource")
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON)
        .body("resource", notNullValue())
        .body("authorization_servers", contains(AUTH_SERVER_URL));
  }
}
