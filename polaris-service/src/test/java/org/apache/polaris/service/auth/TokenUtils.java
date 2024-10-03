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

import static org.apache.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;

public class TokenUtils {

  /** Get token against specified realm */
  public static String getTokenFromSecrets(
      Client client, int port, String clientId, String clientSecret, String realm) {
    String token;

    Invocation.Builder builder =
        client
            .target(String.format("http://localhost:%d/api/catalog/v1/oauth/tokens", port))
            .request("application/json");
    if (realm != null) {
      builder = builder.header(REALM_PROPERTY_KEY, realm);
    }

    try (Response response =
        builder.post(
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
                        clientSecret))))) {
      assertThat(response).returns(200, Response::getStatus);
      token = response.readEntity(OAuthTokenResponse.class).token();
    }
    return token;
  }
}
