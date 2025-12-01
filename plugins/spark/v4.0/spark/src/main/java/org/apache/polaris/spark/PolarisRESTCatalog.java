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
package org.apache.polaris.spark;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.util.EnvironmentUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.polaris.core.rest.PolarisEndpoints;
import org.apache.polaris.core.rest.PolarisResourcePaths;
import org.apache.polaris.spark.rest.CreateGenericTableRESTRequest;
import org.apache.polaris.spark.rest.CreateGenericTableRequest;
import org.apache.polaris.spark.rest.GenericTable;
import org.apache.polaris.spark.rest.ListGenericTablesRESTResponse;
import org.apache.polaris.spark.rest.LoadGenericTableRESTResponse;

/**
 * [[PolarisRESTCatalog]] talks to Polaris REST APIs, and implements the PolarisCatalog interfaces,
 * which are generic table related APIs at this moment. This class doesn't interact with any Spark
 * objects.
 */
public class PolarisRESTCatalog implements PolarisCatalog, Closeable {
  public static final String REST_PAGE_SIZE = "rest-page-size";

  private final Function<Map<String, String>, RESTClient> clientBuilder;

  private RESTClient restClient = null;
  private CloseableGroup closeables = null;
  private Set<Endpoint> endpoints;
  private OAuth2Util.AuthSession catalogAuth = null;
  private PolarisResourcePaths pathGenerator = null;
  private Integer pageSize = null;

  // the default endpoints to config if server doesn't specify the 'endpoints' configuration.
  private static final Set<Endpoint> DEFAULT_ENDPOINTS = PolarisEndpoints.GENERIC_TABLE_ENDPOINTS;

  public PolarisRESTCatalog() {
    this(config -> HTTPClient.builder(config).uri(config.get(CatalogProperties.URI)).build());
  }

  public PolarisRESTCatalog(Function<Map<String, String>, RESTClient> clientBuilder) {
    this.clientBuilder = clientBuilder;
  }

  public void initialize(Map<String, String> unresolved, OAuth2Util.AuthSession catalogAuth) {
    Preconditions.checkArgument(unresolved != null, "Invalid configuration: null");

    // Resolve any configuration that is supplied by environment variables.
    // For example: if we have an entity ("key", "env:envVar") in the unresolved,
    // and envVar is configured to envValue in system env. After resolve, we got
    // entity ("key", "envValue").
    Map<String, String> props = EnvironmentUtil.resolveAll(unresolved);

    // TODO: switch to use authManager once iceberg dependency is updated to 1.9.0
    this.catalogAuth = catalogAuth;

    ConfigResponse config;
    try (RESTClient initClient = clientBuilder.apply(props).withAuthSession(catalogAuth)) {
      config = fetchConfig(initClient, catalogAuth.headers(), props);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close HTTP client", e);
    }

    // call getConfig to get the server configurations
    Map<String, String> mergedProps = config.merge(props);
    if (config.endpoints().isEmpty()) {
      this.endpoints = DEFAULT_ENDPOINTS;
    } else {
      this.endpoints = ImmutableSet.copyOf(config.endpoints());
    }

    this.pathGenerator = PolarisResourcePaths.forCatalogProperties(mergedProps);
    this.restClient = clientBuilder.apply(mergedProps).withAuthSession(catalogAuth);

    this.pageSize = PropertyUtil.propertyAsNullableInt(mergedProps, REST_PAGE_SIZE);
    if (pageSize != null) {
      Preconditions.checkArgument(
          pageSize > 0, "Invalid value for %s, must be a positive integer", REST_PAGE_SIZE);
    }

    this.closeables = new CloseableGroup();
    this.closeables.addCloseable(this.restClient);
    this.closeables.setSuppressCloseFailure(true);
  }

  protected static ConfigResponse fetchConfig(
      RESTClient client, Map<String, String> headers, Map<String, String> properties) {
    // send the client's warehouse location to the service to keep in sync
    // this is needed for cases where the warehouse is configured at client side,
    // and used by Polaris server as catalog name.
    ImmutableMap.Builder<String, String> queryParams = ImmutableMap.builder();
    if (properties.containsKey(CatalogProperties.WAREHOUSE_LOCATION)) {
      queryParams.put(
          CatalogProperties.WAREHOUSE_LOCATION,
          properties.get(CatalogProperties.WAREHOUSE_LOCATION));
    }

    ConfigResponse configResponse =
        client.get(
            ResourcePaths.config(),
            queryParams.build(),
            ConfigResponse.class,
            headers,
            ErrorHandlers.defaultErrorHandler());
    configResponse.validate();
    return configResponse;
  }

  @Override
  public void close() throws IOException {
    if (closeables != null) {
      closeables.close();
    }
  }

  @Override
  public List<TableIdentifier> listGenericTables(Namespace ns) {
    Endpoint.check(endpoints, PolarisEndpoints.V1_LIST_GENERIC_TABLES);

    Map<String, String> queryParams = Maps.newHashMap();
    ImmutableList.Builder<TableIdentifier> tables = ImmutableList.builder();
    String pageToken = "";
    if (pageSize != null) {
      queryParams.put("pageSize", String.valueOf(pageSize));
    }

    do {
      queryParams.put("pageToken", pageToken);
      ListGenericTablesRESTResponse response =
          restClient
              .withAuthSession(this.catalogAuth)
              .get(
                  pathGenerator.genericTables(ns),
                  queryParams,
                  ListGenericTablesRESTResponse.class,
                  Map.of(),
                  ErrorHandlers.namespaceErrorHandler());
      pageToken = response.getNextPageToken();
      tables.addAll(response.getIdentifiers());
    } while (pageToken != null);

    return tables.build();
  }

  @Override
  public boolean dropGenericTable(TableIdentifier identifier) {
    Endpoint.check(endpoints, PolarisEndpoints.V1_DELETE_GENERIC_TABLE);

    try {
      restClient
          .withAuthSession(this.catalogAuth)
          .delete(
              pathGenerator.genericTable(identifier),
              null,
              Map.of(),
              ErrorHandlers.tableErrorHandler());
      return true;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  @Override
  public GenericTable createGenericTable(
      TableIdentifier identifier,
      String format,
      String baseLocation,
      String doc,
      Map<String, String> props) {
    Endpoint.check(endpoints, PolarisEndpoints.V1_CREATE_GENERIC_TABLE);
    CreateGenericTableRESTRequest request =
        new CreateGenericTableRESTRequest(
            CreateGenericTableRequest.builder()
                .setName(identifier.name())
                .setFormat(format)
                .setBaseLocation(baseLocation)
                .setDoc(doc)
                .setProperties(props)
                .build());

    LoadGenericTableRESTResponse response =
        restClient
            .withAuthSession(this.catalogAuth)
            .post(
                pathGenerator.genericTables(identifier.namespace()),
                request,
                LoadGenericTableRESTResponse.class,
                Map.of(),
                ErrorHandlers.tableErrorHandler());

    return response.getTable();
  }

  @Override
  public GenericTable loadGenericTable(TableIdentifier identifier) {
    Endpoint.check(endpoints, PolarisEndpoints.V1_LOAD_GENERIC_TABLE);
    LoadGenericTableRESTResponse response =
        restClient
            .withAuthSession(this.catalogAuth)
            .get(
                pathGenerator.genericTable(identifier),
                null,
                LoadGenericTableRESTResponse.class,
                Map.of(),
                ErrorHandlers.tableErrorHandler());

    return response.getTable();
  }
}
