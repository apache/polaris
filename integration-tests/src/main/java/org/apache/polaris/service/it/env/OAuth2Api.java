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
package org.apache.polaris.service.it.env;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.Map;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;

/**
 * A simple facade to an OAuth2 token endpoint. It works with both Polaris internal token endpoint
 * and with external identity providers.
 */
public class OAuth2Api extends RestApi {

  private final String endpointPath;

  public OAuth2Api(Client client, URI issuerUrl, String endpointPath) {
    super(client, issuerUrl);
    this.endpointPath = endpointPath;
  }

  public String obtainAccessToken(ClientCredentials credentials, String scope) {
    return obtainAccessToken(
        Map.of(
            "grant_type",
            "client_credentials",
            "client_id",
            credentials.clientId(),
            "client_secret",
            credentials.clientSecret(),
            "scope",
            scope));
  }

  public String obtainAccessToken(Map<String, String> requestBody) {
    try (Response response =
        request(endpointPath).post(Entity.form(new MultivaluedHashMap<>(requestBody)))) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      String token = response.readEntity(OAuthTokenResponse.class).token();
      return token;
    }
  }
}
