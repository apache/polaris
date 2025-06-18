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
package org.apache.polaris.spark.quarkus.it;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.polaris.service.it.ext.PolarisServerManagerLoader.polarisServerManager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jakarta.rs.json.JacksonJsonProvider;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import java.util.Map;
import java.util.Random;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;

/**
 * That class provides rest client that is can be used to talk to Polaris Management service and
 * auth token endpoint. This class is currently used by Spark Client tests for commands that can not
 * be issued through spark command, such as createCatalog etc.
 */
public final class PolarisManagementClient implements AutoCloseable {
  private final PolarisApiEndpoints endpoints;
  private final Client client;
  // Use an alphanumeric ID for widest compatibility in HTTP and SQL.
  // Use MAX_RADIX for shorter output.
  private final String clientId =
      Long.toString(Math.abs(new Random().nextLong()), Character.MAX_RADIX);
  // initialization an Iceberg rest client for fetch token
  private final RESTClient restClient;

  private PolarisManagementClient(PolarisApiEndpoints endpoints) {
    this.endpoints = endpoints;

    this.client =
        ClientBuilder.newBuilder()
            .readTimeout(5, MINUTES)
            .connectTimeout(1, MINUTES)
            .register(new JacksonJsonProvider(new ObjectMapper()))
            .build();

    this.restClient = HTTPClient.builder(Map.of()).uri(endpoints.catalogApiEndpoint()).build();
  }

  public static PolarisManagementClient managementClient(PolarisApiEndpoints endpoints) {
    return new PolarisManagementClient(endpoints);
  }

  /** This method should be used by test code to make top-level entity names. */
  public String newEntityName(String hint) {
    return polarisServerManager().transformEntityName(hint + "_" + clientId);
  }

  public ManagementApi managementApi(String authToken) {
    return new ManagementApi(client, endpoints, authToken, endpoints.managementApiEndpoint());
  }

  public ManagementApi managementApi(ClientCredentials credentials) {
    return managementApi(obtainToken(credentials));
  }

  /** Requests an access token from the Polaris server for the given {@link ClientCredentials}. */
  public String obtainToken(ClientCredentials credentials) {
    OAuthTokenResponse response =
        OAuth2Util.fetchToken(
            restClient.withAuthSession(AuthSession.EMPTY),
            Map.of(),
            String.format("%s:%s", credentials.clientId(), credentials.clientSecret()),
            "PRINCIPAL_ROLE:ALL",
            endpoints.catalogApiEndpoint() + "/v1/oauth/tokens",
            Map.of("grant_type", "client_credentials"));
    return response.token();
  }

  @Override
  public void close() throws Exception {
    client.close();
    restClient.close();
  }
}
