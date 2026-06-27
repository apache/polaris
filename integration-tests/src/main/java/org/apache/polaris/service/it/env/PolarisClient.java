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
import com.fasterxml.jackson.databind.json.JsonMapper;
import jakarta.ws.rs.client.Client;
import java.net.URI;
import java.util.Map;
import java.util.Random;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.service.it.ext.PolarisServerManager;
import org.jspecify.annotations.Nullable;

/**
 * This is a holder for the heavy-weight HTTP client for accessing Polaris APIs, and optionally,
 * platform APIs (such as metrics or health endpoints).
 *
 * <p>This class provides methods for constructing light-weight API wrappers for Iceberg REST APis,
 * Polaris Management APIs, and platform APIs that reuse the same shared HTTP client.
 */
public final class PolarisClient implements AutoCloseable {
  private final PolarisApiEndpoints endpoints;
  private final @Nullable PlatformApiEndpoints platformEndpoints;
  private final Client client;
  // Use an alphanumeric ID for widest compatibility in HTTP and SQL.
  // Use MAX_RADIX for shorter output.
  private final String clientId =
      Long.toString(Math.abs(new Random().nextLong()), Character.MAX_RADIX);

  private PolarisClient(PolarisApiEndpoints endpoints) {
    this(endpoints, null);
  }

  private PolarisClient(
      PolarisApiEndpoints endpoints, @Nullable PlatformApiEndpoints platformEndpoints) {
    this.endpoints = endpoints;
    this.platformEndpoints = platformEndpoints;
    this.client = polarisServerManager().createClient();
  }

  /**
   * Builds a {@link PolarisClient} for the given {@link PolarisApiEndpoints}. The returned client
   * cannot access platform endpoints; use {@link #polarisClient(PolarisApiEndpoints,
   * PlatformApiEndpoints)} if platform endpoints are required by the test.
   */
  public static PolarisClient polarisClient(PolarisApiEndpoints endpoints) {
    return new PolarisClient(endpoints);
  }

  /**
   * Builds a {@link PolarisClient} for the given {@link PolarisApiEndpoints} and {@link
   * PlatformApiEndpoints}. The returned client can access both Polaris APIs and platform endpoints
   * (e.g. metrics or health endpoints).
   */
  public static PolarisClient polarisClient(
      PolarisApiEndpoints endpoints, PlatformApiEndpoints platformEndpoints) {
    return new PolarisClient(endpoints, platformEndpoints);
  }

  /**
   * Utility method that creates an {@link ObjectMapper} sufficient for (de-)serializing client-side
   * payloads for Iceberg REST and Polaris Management APIs.
   *
   * <p>It is recommended for {@link PolarisServerManager} implementations to use this {@link
   * ObjectMapper} if the make custom {@link PolarisServerManager#createClient() clients}.
   */
  public static ObjectMapper buildObjectMapper() {
    ObjectMapper mapper = JsonMapper.builder().build();
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
    RESTSerializers.registerAll(mapper);
    return mapper;
  }

  /**
   * This method should be used by test code to make top-level entity names. The purpose of this
   * method is two-fold:
   * <li>Identify top-level entities for later clean-up by {@link #cleanUp(String)}.
   * <li>Allow {@link PolarisServerManager}s to customize top-level entities per environment.
   */
  public String newEntityName(String hint) {
    return polarisServerManager().transformEntityName(hint + "_" + clientId);
  }

  public ManagementApi managementApi(String authToken) {
    return new ManagementApi(client, endpoints, authToken, endpoints.managementApiEndpoint());
  }

  public CatalogApi catalogApi(String authToken) {
    return new CatalogApi(client, endpoints, authToken, endpoints.catalogApiEndpoint());
  }

  public CatalogApi catalogApiPlain() {
    return new CatalogApi(client, endpoints, null, endpoints.catalogApiEndpoint());
  }

  public GenericTableApi genericTableApi(String authToken) {
    return new GenericTableApi(client, endpoints, authToken, endpoints.catalogApiEndpoint());
  }

  public DirectoryApi directoryApi(String authToken) {
    return new DirectoryApi(client, endpoints, authToken, endpoints.catalogApiEndpoint());
  }

  public PolicyApi policyApi(String authToken) {
    return new PolicyApi(client, endpoints, authToken, endpoints.catalogApiEndpoint());
  }

  public PlatformMetricsApi platformMetricsApi() {
    if (platformEndpoints == null) {
      throw new IllegalStateException("Platform endpoints are not available");
    }
    return new PlatformMetricsApi(client, platformEndpoints.metricsApiEndpoint());
  }

  /** Requests an access token from the Polaris server for the given principal. */
  public String obtainToken(PrincipalWithCredentials credentials) {
    return obtainToken(new ClientPrincipal(credentials));
  }

  /** Requests an access token from the Polaris server for the given principal. */
  public String obtainToken(ClientPrincipal principal) {
    return obtainToken(principal.credentials());
  }

  /** Requests an access token from the Polaris server for the given credentials. */
  public String obtainToken(ClientCredentials credentials) {
    return polarisServerManager().accessManager(client).obtainAccessToken(endpoints, credentials);
  }

  /**
   * Requests an access token from the authorization server denoted by the issuer URL and token
   * endpoint path.
   */
  public String obtainToken(URI issuerUrl, String endpointPath, Map<String, String> requestBody) {
    return new OAuth2Api(client, issuerUrl, endpointPath).obtainAccessToken(requestBody);
  }

  public boolean ownedName(String name) {
    return name != null && name.contains(clientId);
  }

  public void cleanUp(String authToken) {
    ManagementApi managementApi = managementApi(authToken);
    CatalogApi catalogApi = catalogApi(authToken);

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
