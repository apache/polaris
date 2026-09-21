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
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.auth.PolarisAuthorizerFactory;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.junit.jupiter.api.Test;

/**
 * OIDC tests for <em>external</em> principals: an OIDC-authenticated caller that has no backing
 * entity in the Polaris metastore ({@code principal-mode=external}), authorized by a non-default
 * authorizer.
 */
@QuarkusTest
@TestProfile(ExternalPrincipalOidcAuthTest.Profile.class)
public class ExternalPrincipalOidcAuthTest extends AbstractOidcAuthTest {

  public static class Profile extends AbstractOidcAuthTest.Profile {

    @Override
    public Map<String, String> getConfigOverrides() {
      ImmutableMap.Builder<String, String> config = ImmutableMap.builder();
      config.putAll(super.getConfigOverrides());
      config.put("polaris.authentication.principal-mode", "external");
      config.put("polaris.authorization.type", "test");
      return config.build();
    }

    @ApplicationScoped
    @Identifier("test")
    public PolarisAuthorizerFactory getAuthorizerFactory() {
      return realmConfig -> new TestPolarisAuthorizer();
    }
  }

  @Test
  void allowedExternalPrincipal(PolarisApiEndpoints endpoints) {
    asUser(endpoints, "alice", Set.of("admin")).get(listCatalogs(endpoints)).then().statusCode(200);
  }

  @Test
  void unauthenticatedExternalPrincipal(PolarisApiEndpoints endpoints) {
    // Invalid OIDC token (not signed by the WireMock OIDC server)
    asUser(endpoints, "bad-token").get(listCatalogs(endpoints)).then().statusCode(401);
  }

  @Test
  void unauthorizedExternalPrincipal(PolarisApiEndpoints endpoints) {
    // Bad username
    asUser(endpoints, "denied_bob", Set.of("admin"))
        .get(listCatalogs(endpoints))
        .then()
        .statusCode(403);
    // Bad roles
    asUser(endpoints, "bob", Set.of("denied_role"))
        .get(listCatalogs(endpoints))
        .then()
        .statusCode(403);
  }
}
