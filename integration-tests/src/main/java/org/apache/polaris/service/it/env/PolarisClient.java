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

import static java.util.concurrent.TimeUnit.MINUTES;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.jakarta.rs.json.JacksonJsonProvider;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;

public final class PolarisClient implements AutoCloseable {
  private final PolarisApiEndpoints endpoints;
  private final Client client;

  private PolarisClient(PolarisApiEndpoints endpoints) {
    this.endpoints = endpoints;

    ObjectMapper mapper = new ObjectMapper();
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
    RESTSerializers.registerAll(mapper);

    this.client =
        ClientBuilder.newBuilder()
            .readTimeout(5, MINUTES)
            .connectTimeout(1, MINUTES)
            .register(new JacksonJsonProvider(mapper))
            .build();
  }

  public static PolarisClient polarisClient(PolarisApiEndpoints endpoints) {
    return new PolarisClient(endpoints);
  }

  public ManagementApi managementApi(String authToken) {
    return new ManagementApi(client, endpoints, authToken, endpoints.managementApiEndpoint());
  }

  public ManagementApi managementApi(ClientCredentials credentials) {
    return managementApi(obtainToken(credentials));
  }

  public ManagementApi managementApi(PrincipalWithCredentials principal) {
    return managementApi(obtainToken(principal));
  }

  public CatalogApi catalogApi(PrincipalWithCredentials principal) {
    return new CatalogApi(
        client, endpoints, obtainToken(principal), endpoints.catalogApiEndpoint());
  }

  public CatalogApi catalogApi(ClientCredentials credentials) {
    return new CatalogApi(
        client, endpoints, obtainToken(credentials), endpoints.catalogApiEndpoint());
  }

  public String obtainToken(PrincipalWithCredentials principal) {
    return obtainToken(
        new ClientCredentials(
            principal.getCredentials().getClientId(),
            principal.getCredentials().getClientSecret(),
            "dummy-principal"));
  }

  public String obtainToken(ClientCredentials credentials) {
    CatalogApi anon = new CatalogApi(client, endpoints, null, endpoints.catalogApiEndpoint());
    return anon.obtainToken(credentials);
  }

  @Override
  public void close() throws Exception {
    client.close();
  }
}
