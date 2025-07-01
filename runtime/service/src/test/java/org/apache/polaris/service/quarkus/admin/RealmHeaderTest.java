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
package org.apache.polaris.service.quarkus.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(RealmHeaderTest.Profile.class)
public class RealmHeaderTest {
  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.realm-context.header-name",
          REALM_HEADER,
          "polaris.realm-context.realms",
          "realm1,realm2",
          "polaris.bootstrap.credentials",
          "realm1,client1,secret1;realm2,client2,secret2");
    }
  }

  private static final String REALM_HEADER = "test-header-r123";

  private static final URI baseUri =
      URI.create(
          "http://localhost:"
              + Objects.requireNonNull(
                  Integer.getInteger("quarkus.http.test-port"),
                  "System property not set correctly: quarkus.http.test-port"));

  private Response request(String realm, String header, String clientId, String secret) {
    try (PolarisClient client =
        PolarisClient.polarisClient(new PolarisApiEndpoints(baseUri, realm, header))) {
      return client
          .catalogApiPlain()
          .request("v1/oauth/tokens")
          .post(
              Entity.form(
                  new MultivaluedHashMap<>(
                      Map.of(
                          "grant_type",
                          "client_credentials",
                          "scope",
                          "PRINCIPAL_ROLE:ALL",
                          "client_id",
                          clientId,
                          "client_secret",
                          secret))));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testInvalidRealmHeaderValue() {
    try (Response response = request("INVALID", REALM_HEADER, "dummy", "dummy")) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      assertThat(response.readEntity(ErrorResponse.class))
          .extracting(ErrorResponse::code, ErrorResponse::type, ErrorResponse::message)
          .containsExactly(
              Response.Status.NOT_FOUND.getStatusCode(),
              "MissingOrInvalidRealm",
              "Unknown realm: INVALID");
    }
  }

  @Test
  public void testNoRealmHeader() {
    try (Response response = request("fake-realm", "irrelevant-header", "client2", "secret2")) {
      // The default realm is "realm2" so the second pair of secrets is not valid without
      // an explicit header
      assertThat(response.getStatus()).isEqualTo(Response.Status.UNAUTHORIZED.getStatusCode());
    }
  }

  @Test
  public void testDefaultRealm() {
    try (Response response = request("fake-realm", "irrelevant-header", "client1", "secret1")) {
      // The default realm is "realm1", now credentials match
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  @Test
  public void testValidRealmHeaderDefaultRealm() {
    try (Response response = request("realm2", REALM_HEADER, "client2", "secret2")) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }
}
