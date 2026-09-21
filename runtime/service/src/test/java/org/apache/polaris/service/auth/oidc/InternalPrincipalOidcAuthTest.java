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
import io.quarkus.test.oidc.server.OidcWiremockTestResource;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.service.auth.DefaultAuthenticator;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * OIDC tests for <em>internal</em> principals: an OIDC-authenticated caller mapped to a Polaris
 * principal that <em>does</em> exist in the metastore ({@code principal-mode=internal}), authorized
 * by the built-in {@code internal} authorizer via its grants.
 */
@QuarkusTest
@TestProfile(InternalPrincipalOidcAuthTest.Profile.class)
public class InternalPrincipalOidcAuthTest extends AbstractOidcAuthTest {

  /** An ad-hoc principal created with no principal roles, hence no privileges on any securable. */
  private static final String UNPRIVILEGED_PRINCIPAL = "unprivileged-user";

  public static class Profile extends AbstractOidcAuthTest.Profile {

    @Override
    public Map<String, String> getConfigOverrides() {
      ImmutableMap.Builder<String, String> config = ImmutableMap.builder();
      config.putAll(super.getConfigOverrides());
      config.put("polaris.authentication.principal-mode", "internal");
      return config.build();
    }
  }

  private PolarisApiEndpoints endpoints;
  private PolarisClient client;

  @BeforeAll
  void createUnprivilegedPrincipal(PolarisApiEndpoints endpoints) {
    this.endpoints = endpoints;
    client = PolarisClient.polarisClient(endpoints);
    // Create an unprivileged principal with no principal roles: the principal exists in the
    // metastore (so it authenticates) but holds no grants (so authorization must deny it).
    managementApiAsRoot().createPrincipal(UNPRIVILEGED_PRINCIPAL);
  }

  @AfterAll
  void deleteUnprivilegedPrincipal() throws Exception {
    if (client != null) {
      managementApiAsRoot().deletePrincipal(UNPRIVILEGED_PRINCIPAL);
      client.close();
    }
  }

  private ManagementApi managementApiAsRoot() {
    String rootToken =
        OidcWiremockTestResource.getAccessToken(
            "root", Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_ALL));
    return client.managementApi(rootToken);
  }

  @Test
  void allowedInternalPrincipal() {
    asUser(endpoints, "root", Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_ALL))
        .get(listCatalogs(endpoints))
        .then()
        .statusCode(200);
  }

  @Test
  void unauthenticatedInternalPrincipal() {
    // Internal-principal mode requires a backing metastore entity; a valid token for a principal
    // that does not exist must fail authentication (401), not merely authorization (403).
    asUser(endpoints, "non-existent", Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_ALL))
        .get(listCatalogs(endpoints))
        .then()
        .statusCode(401);
    // Non-existent role should also fail authentication (401)
    asUser(endpoints, "root", Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_PREFIX + "non-existent"))
        .get(listCatalogs(endpoints))
        .then()
        .statusCode(401);
  }

  @Test
  void unauthorizedInternalPrincipal() {
    // A known principal with no grants authenticates successfully, but the internal authorizer
    // denies the operation for lack of privileges (403, not 401).
    asUser(endpoints, UNPRIVILEGED_PRINCIPAL, Set.of(DefaultAuthenticator.PRINCIPAL_ROLE_ALL))
        .get(listCatalogs(endpoints))
        .then()
        .statusCode(403);
  }
}
