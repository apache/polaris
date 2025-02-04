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
package org.apache.polaris.service.quarkus.it;

import static org.apache.polaris.service.it.env.PolarisApiEndpoints.REALM_HEADER;
import static org.assertj.core.api.Assertions.assertThat;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthConfig;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.test.PolarisApplicationIntegrationTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(QuarkusApplicationIntegrationTest.Profile.class)
public class QuarkusApplicationIntegrationTest extends PolarisApplicationIntegrationTest {

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "quarkus.http.limits.max-body-size", "1000000",
          "polaris.features.defaults.\"ALLOW_OVERLAPPING_CATALOG_URLS\"", "true",
          "polaris.features.defaults.\"SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION\"", "true");
    }
  }

  @Test
  public void testIcebergRestApiRefreshToken(
      PolarisApiEndpoints endpoints, ClientCredentials clientCredentials) throws IOException {
    String path = endpoints.catalogApiEndpoint() + "/v1/oauth/tokens";
    try (RESTClient client =
        HTTPClient.builder(Map.of())
            .withHeader(REALM_HEADER, endpoints.realm())
            .uri(path)
            .build()) {
      String credentialString =
          clientCredentials.clientId() + ":" + clientCredentials.clientSecret();
      String expiredToken =
          JWT.create().withExpiresAt(Instant.EPOCH).sign(Algorithm.HMAC256("irrelevant-secret"));
      var authConfig =
          AuthConfig.builder()
              .credential(credentialString)
              .scope("PRINCIPAL_ROLE:ALL")
              .oauth2ServerUri(path)
              .token(expiredToken)
              .build();

      var parentSession = new OAuth2Util.AuthSession(Map.of(), authConfig);
      var session =
          OAuth2Util.AuthSession.fromAccessToken(client, null, expiredToken, 0L, parentSession);

      assertThat(session.token()).isNotEqualTo(expiredToken); // implicit refresh
      assertThat(JWT.decode(session.token()).getExpiresAtAsInstant()).isAfter(Instant.EPOCH);
    }
  }
}
