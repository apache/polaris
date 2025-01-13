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

import static org.apache.polaris.service.it.ext.PolarisServerManagerLoader.polarisServerManager;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import jakarta.ws.rs.client.Client;
import java.util.Random;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;

public final class PolarisClient implements AutoCloseable {
  private final PolarisApiEndpoints endpoints;
  private final Client client;
  // Use an alphanumeric ID for widest compatibility in HTTP and SQL.
  // Use MAX_RADIX for shorter output.
  private final String clientId =
      Long.toString(Math.abs(new Random().nextLong()), Character.MAX_RADIX);

  private PolarisClient(PolarisApiEndpoints endpoints) {
    this.endpoints = endpoints;

    this.client = polarisServerManager().createClient();
  }

  public static PolarisClient polarisClient(PolarisApiEndpoints endpoints) {
    return new PolarisClient(endpoints);
  }

  public static ObjectMapper buildObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
    RESTSerializers.registerAll(mapper);
    return mapper;
  }

  public String newEntityName(String hint) {
    return polarisServerManager().transformEntityName(hint + "_" + clientId);
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
            principal.getCredentials().getClientSecret()));
  }

  public String obtainToken(ClientCredentials credentials) {
    CatalogApi anon = new CatalogApi(client, endpoints, null, endpoints.catalogApiEndpoint());
    return anon.obtainToken(credentials);
  }

  private boolean ownedName(String name) {
    return name != null && name.contains(clientId);
  }

  public void cleanUp(ClientCredentials credentials) {
    ManagementApi managementApi = managementApi(credentials);
    CatalogApi catalogApi = catalogApi(credentials);

    managementApi.listCatalogs().stream()
        .filter(c -> ownedName(c.getName()))
        .forEach(
            c -> {
              catalogApi.purge(c.getName());
              managementApi.dropCatalog(c.getName());
            });

    managementApi.listPrincipalRoles().stream()
        .filter(r -> ownedName(r.getName()))
        .forEach(managementApi::deletePrincipalRole);
    managementApi.listPrincipals().stream()
        .filter(p -> ownedName(p.getName()))
        .forEach(p -> managementApi.deletePrincipal(p.getName()));
  }

  @Override
  public void close() throws Exception {
    client.close();
  }
}
