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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.*;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.util.EnvironmentUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.polaris.core.rest.PolarisEndpoint;
import org.apache.polaris.core.rest.PolarisResourcePaths;
import org.apache.polaris.service.types.GenericTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PolarisRESTCatalog implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(PolarisRESTCatalog.class);

  public static final String REST_PAGE_SIZE = "rest-page-size";

  private RESTClient restClient = null;
  private CloseableGroup closeables = null;
  private Set<Endpoint> endpoints;
  private OAuth2Util.AuthSession catalogAuth = null;
  private PolarisResourcePaths paths = null;
  private Integer pageSize = null;

  // TODO: update to use the predefined GENERIC_TABLE_ENDPOINTS
  private static final Set<Endpoint> DEFAULT_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(PolarisEndpoint.V1_CREATE_GENERIC_TABLE)
          .add(PolarisEndpoint.V1_LOAD_GENERIC_TABLE)
          .add(PolarisEndpoint.V1_DELETE_GENERIC_TABLE)
          .add(PolarisEndpoint.V1_LIST_GENERIC_TABLES)
          .build();

  public void initialize(Map<String, String> unresolved, OAuth2Util.AuthSession catalogAuth) {
    Preconditions.checkArgument(unresolved != null, "Invalid configuration: null");

    // resolve any configuration that is supplied by environment variables
    Map<String, String> props = EnvironmentUtil.resolveAll(unresolved);

    // TODO: switch to use authManager once iceberg dependency is updated to 1.9.0
    this.catalogAuth = catalogAuth;

    this.restClient =
        HTTPClient.builder(props)
            .uri(props.get(CatalogProperties.URI))
            .build()
            .withAuthSession(catalogAuth);

    // call getConfig to get the server configurations
    ConfigResponse config = fetchConfig(this.restClient, catalogAuth.headers(), props);
    Map<String, String> mergedProps = config.merge(props);
    if (config.endpoints().isEmpty()) {
      this.endpoints = DEFAULT_ENDPOINTS;
    } else {
      this.endpoints = ImmutableSet.copyOf(config.endpoints());
    }

    this.paths = PolarisResourcePaths.forCatalogProperties(mergedProps);
    this.restClient =
        HTTPClient.builder(mergedProps)
            .uri(mergedProps.get(CatalogProperties.URI))
            .build()
            .withAuthSession(catalogAuth);

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

  public List<TableIdentifier> listTables(Namespace ns) {
    throw new UnsupportedOperationException("listTables not supported");
  }

  public boolean dropTable(TableIdentifier identifier) {
    throw new UnsupportedOperationException("dropTable not supported");
  }

  public GenericTable createTable(TableIdentifier identifier, String format, Map<String, String> props) {
    throw new UnsupportedOperationException("CreateTable not supported");
  }

  public GenericTable loadTable(TableIdentifier identifier) {
    throw new UnsupportedOperationException("loadTable not supported");
  }

}