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
import org.apache.polaris.service.it.ext.PolarisServerManager;

/**
 * This is a holder for the heavy-weight HTTP client for accessing Polaris APIs. This class provides
 * method for constructing light-weight API wrappers for Iceberg REST and Polaris Management API
 * that reuse the same shared HTTP client.
 */
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

  /**
   * Utility method that creates an {@link ObjectMapper} sufficient for (de-)serializing client-side
   * payloads for Iceberg REST and Polaris Management APIs.
   *
   * <p>It is recommended for {@link PolarisServerManager} implementations to use this {@link
   * ObjectMapper} if the make custom {@link PolarisServerManager#createClient() clients}.
   */
  public static ObjectMapper buildObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
    RESTSerializers.registerAll(mapper);
    return mapper;
  }

  /**
   * This method should be used by test code to make top-level entity names. The purpose of this
   * method is two-fold:
   * <li>Identify top-level entities for latger clean-up by {@link #cleanUp(ClientCredentials)}.
   * <li>Allow {@link PolarisServerManager}s to customize top-level entities per environment.
   */
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

  public CatalogApi catalogApiPlain() {
    return new CatalogApi(client, endpoints, null, endpoints.catalogApiEndpoint());
  }

  public GenericTableApi genericTableApi(PrincipalWithCredentials principal) {
    return new GenericTableApi(
        client, endpoints, obtainToken(principal), endpoints.catalogApiEndpoint());
  }

  public GenericTableApi genericTableApi(ClientCredentials credentials) {
    return new GenericTableApi(
        client, endpoints, obtainToken(credentials), endpoints.catalogApiEndpoint());
  }

  public PolicyApi policyApi(PrincipalWithCredentials principal) {
    return new PolicyApi(client, endpoints, obtainToken(principal), endpoints.catalogApiEndpoint());
  }

  public PolicyApi policyApi(ClientCredentials credentials) {
    return new PolicyApi(
        client, endpoints, obtainToken(credentials), endpoints.catalogApiEndpoint());
  }

  /**
   * Requests an access token from the Polaris server for the client ID/secret pair that is part of
   * the given principal data object.
   */
  public String obtainToken(PrincipalWithCredentials principal) {
    return obtainToken(
        new ClientCredentials(
            principal.getCredentials().getClientId(),
            principal.getCredentials().getClientSecret()));
  }

  /** Requests an access token from the Polaris server for the given {@link ClientCredentials}. */
  public String obtainToken(ClientCredentials credentials) {
    return polarisServerManager().accessManager(client).obtainAccessToken(endpoints, credentials);
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
