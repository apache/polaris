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

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.oidc.server.OidcWiremockTestResource;
import io.restassured.specification.RequestSpecification;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Shared harness for authorization tests that exercise the external OIDC authentication pipeline.
 * Identities are minted as real signed bearer tokens by an in-JVM WireMock OIDC server ({@link
 * OidcWiremockTestResource}).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(PolarisIntegrationTestExtension.class)
public abstract class AbstractOidcAuthTest {

  public abstract static class Profile implements QuarkusTestProfile {

    /**
     * Config shared by all OIDC auth tests: external authentication backed by the WireMock OIDC
     * server, mapping the {@code preferred_username} claim to the principal name. {@code
     * keycloak.url} is published by {@link OidcWiremockTestResource} and already ends with {@code
     * /auth}; the mock serves the OIDC discovery document under {@code
     * {keycloak.url}/realms/quarkus}.
     */
    @Override
    public Map<String, String> getConfigOverrides() {
      ImmutableMap.Builder<String, String> config = ImmutableMap.builder();
      config.put("quarkus.oidc.tenant-enabled", "true");
      config.put("quarkus.oidc.auth-server-url", "${keycloak.url}/realms/quarkus");
      config.put("quarkus.oidc.client-id", "quarkus-app");
      config.put("polaris.authentication.type", "external");
      config.put("polaris.oidc.principal-mapper.name-claim-path", "preferred_username");
      config.put("polaris.readiness.ignore-severe-issues", "true");
      return config.build();
    }

    @Override
    public List<TestResourceEntry> testResources() {
      return List.of(new TestResourceEntry(OidcWiremockTestResource.class));
    }
  }

  /**
   * Builds a request bearing a freshly minted OIDC token for {@code user} carrying {@code roles}.
   */
  protected RequestSpecification asUser(
      PolarisApiEndpoints endpoints, String user, Set<String> roles) {
    String token = OidcWiremockTestResource.getAccessToken(user, roles);
    return asUser(endpoints, token);
  }

  /**
   * Builds a request bearing the given OIDC {@code token}.
   *
   * <p>Note that this method does not validate the token; it is assumed to be valid and signed by
   * the WireMock OIDC server.
   */
  protected RequestSpecification asUser(PolarisApiEndpoints endpoints, String token) {
    RequestSpecification request = given().auth().oauth2(token);
    for (Map.Entry<String, String> header : endpoints.extraHeaders().entrySet()) {
      request = request.header(header.getKey(), header.getValue());
    }
    return request;
  }

  /** The Polaris management "list catalogs" endpoint — a simple authenticated+authorized call. */
  protected String listCatalogs(PolarisApiEndpoints endpoints) {
    return endpoints.managementApiEndpoint() + "/v1/catalogs";
  }
}
