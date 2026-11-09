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

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.Map;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.junit.jupiter.api.Test;

/**
 * OIDC tests for opaque (non-JWT) access tokens, validated through the token introspection endpoint
 * of the WireMock OIDC server. Callers are <em>external</em> principals authorized by the test
 * authorizer, as in {@link ExternalPrincipalOidcAuthTest}.
 *
 * <p>The WireMock OIDC server accepts the opaque tokens {@code alice} (scope {@code user}) and
 * {@code admin} (scope {@code user admin}), and rejects the opaque token {@code expired}. The
 * introspection response carries the token's username in the {@code username} field.
 */
@QuarkusTest
@TestProfile(OpaqueTokenOidcAuthTest.Profile.class)
public class OpaqueTokenOidcAuthTest extends AbstractOidcAuthTest {

  public static class Profile extends ExternalPrincipalOidcAuthTest.Profile {

    @Override
    public Map<String, String> getConfigOverrides() {
      ImmutableMap.Builder<String, String> config = ImmutableMap.builder();
      config.putAll(super.getConfigOverrides());
      config.put("quarkus.oidc.credentials.secret", "secret");
      // The introspection response carries the principal name in the "username" field
      config.put("polaris.oidc.principal-mapper.name-claim-path", "username");
      // Roles come from the introspection "scope" field: turn "admin" into a role that the test
      // authorizer denies.
      config.put("polaris.oidc.principal-roles-mapper.mappings[0].regex", "^admin$");
      config.put(
          "polaris.oidc.principal-roles-mapper.mappings[0].replacement",
          TestPolarisAuthorizer.DENY_PREFIX + "_admin");
      return config.buildKeepingLast();
    }
  }

  @Test
  void allowedOpaqueTokenPrincipal(PolarisApiEndpoints endpoints) {
    asUser(endpoints, "alice").get(listCatalogs(endpoints)).then().statusCode(200);
  }

  @Test
  void unauthenticatedOpaqueTokenPrincipal(PolarisApiEndpoints endpoints) {
    // Expired token
    asUser(endpoints, "expired").get(listCatalogs(endpoints)).then().statusCode(401);
    // Unknown token (no introspection response)
    asUser(endpoints, "unknown").get(listCatalogs(endpoints)).then().statusCode(401);
  }

  @Test
  void unauthorizedOpaqueTokenPrincipal(PolarisApiEndpoints endpoints) {
    // The "admin" scope is mapped to a denied role
    asUser(endpoints, "admin").get(listCatalogs(endpoints)).then().statusCode(403);
  }
}
