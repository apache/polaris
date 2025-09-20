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
package org.apache.polaris.service.admin;

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
import java.util.UUID;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(RequestIdHeaderTest.Profile.class)
public class RequestIdHeaderTest {
  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.log.request-id-header-name",
          REQUEST_ID_HEADER,
          "polaris.realm-context.header-name",
          REALM_HEADER,
          "polaris.realm-context.realms",
          REALM);
    }
  }

  private static final String REQUEST_ID_HEADER = "x-test-request-id-random";
  private static final String REALM_HEADER = "realm";
  private static final String REALM = "realm1";
  private static final String CLIENT_ID = "client1";
  private static final String CLIENT_SECRET = "secret1";

  private static final URI baseUri =
      URI.create(
          "http://localhost:"
              + Objects.requireNonNull(
                  Integer.getInteger("quarkus.http.test-port"),
                  "System property not set correctly: quarkus.http.test-port"));

  private Response request(Map<String, String> headers) {
    try (PolarisClient client =
        PolarisClient.polarisClient(new PolarisApiEndpoints(baseUri, REALM, headers))) {
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
                          CLIENT_ID,
                          "client_secret",
                          CLIENT_SECRET))));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testRequestIdHeaderSpecified() {
    String requestId = "pre-requested-request-id";
    Map<String, String> headers = Map.of(REALM_HEADER, REALM, REQUEST_ID_HEADER, requestId);
    try (Response response = request(headers)) {
      assertThat(response.getHeaders()).containsKey(REQUEST_ID_HEADER);
      assertThat(response.getHeaders().get(REQUEST_ID_HEADER)).hasSize(1);
      assertThat(response.getHeaders().get(REQUEST_ID_HEADER)).allMatch(s -> s.equals(requestId));
    }
  }

  @Test
  public void testRequestIdHeaderNotSpecified() {
    Map<String, String> headers = Map.of(REALM_HEADER, REALM);
    try (Response response = request(headers)) {
      assertThat(response.getHeaders()).containsKey(REQUEST_ID_HEADER);
      assertThat(response.getHeaders().get(REQUEST_ID_HEADER)).hasSize(1);
      assertThat(response.getHeaders().get(REQUEST_ID_HEADER))
          .allMatch(s -> isValidUUID(s.toString()));
    }
  }

  private boolean isValidUUID(String str) {
    try {
      UUID.fromString(str);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
