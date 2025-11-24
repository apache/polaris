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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.polaris.core.rest.PolarisEndpoints;
import org.apache.polaris.spark.rest.GenericTable;
import org.apache.polaris.spark.rest.ListGenericTablesRESTResponse;
import org.apache.polaris.spark.rest.LoadGenericTableRESTResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class PolarisRESTCatalogTest {

  private RESTClient mockClient;
  private OAuth2Util.AuthSession mockAuthSession;
  private PolarisRESTCatalog catalog;

  @BeforeEach
  public void setup() {
    mockClient = mock(RESTClient.class);
    mockAuthSession = mock(OAuth2Util.AuthSession.class);
    when(mockAuthSession.headers()).thenReturn(ImmutableMap.of("Authorization", "Bearer token"));
    when(mockClient.withAuthSession(any())).thenReturn(mockClient);

    catalog = new PolarisRESTCatalog(config -> mockClient);
  }

  @Test
  public void testInitializeWithDefaultEndpoints() {
    ConfigResponse configResponse =
        ConfigResponse.builder()
            .withDefaults(ImmutableMap.of())
            .withOverrides(ImmutableMap.of())
            .build();

    when(mockClient.get(any(), anyMap(), eq(ConfigResponse.class), anyMap(), any()))
        .thenReturn(configResponse);

    Map<String, String> properties =
        ImmutableMap.of(CatalogProperties.URI, "http://localhost:8181");

    catalog.initialize(properties, mockAuthSession);

    verify(mockClient).get(any(), anyMap(), eq(ConfigResponse.class), anyMap(), any());
  }

  @Test
  public void testInitializeWithCustomEndpoints() {
    ConfigResponse configResponse =
        ConfigResponse.builder()
            .withDefaults(ImmutableMap.of())
            .withOverrides(ImmutableMap.of())
            .withEndpoints(
                ImmutableList.of(
                    PolarisEndpoints.V1_LIST_GENERIC_TABLES,
                    PolarisEndpoints.V1_CREATE_GENERIC_TABLE))
            .build();

    when(mockClient.get(any(), anyMap(), eq(ConfigResponse.class), anyMap(), any()))
        .thenReturn(configResponse);

    Map<String, String> properties =
        ImmutableMap.of(CatalogProperties.URI, "http://localhost:8181");

    catalog.initialize(properties, mockAuthSession);

    verify(mockClient).get(any(), anyMap(), eq(ConfigResponse.class), anyMap(), any());
  }

  @Test
  public void testInitializeWithPageSize() {
    ConfigResponse configResponse =
        ConfigResponse.builder()
            .withDefaults(ImmutableMap.of())
            .withOverrides(ImmutableMap.of())
            .build();

    when(mockClient.get(any(), anyMap(), eq(ConfigResponse.class), anyMap(), any()))
        .thenReturn(configResponse);

    Map<String, String> properties =
        ImmutableMap.of(
            CatalogProperties.URI,
            "http://localhost:8181",
            PolarisRESTCatalog.REST_PAGE_SIZE,
            "10");

    catalog.initialize(properties, mockAuthSession);

    verify(mockClient).get(any(), anyMap(), eq(ConfigResponse.class), anyMap(), any());
  }

  @Test
  public void testInitializeWithInvalidPageSize() {
    ConfigResponse configResponse =
        ConfigResponse.builder()
            .withDefaults(ImmutableMap.of())
            .withOverrides(ImmutableMap.of())
            .build();

    when(mockClient.get(any(), anyMap(), eq(ConfigResponse.class), anyMap(), any()))
        .thenReturn(configResponse);

    Map<String, String> properties =
        ImmutableMap.of(
            CatalogProperties.URI,
            "http://localhost:8181",
            PolarisRESTCatalog.REST_PAGE_SIZE,
            "-1");

    assertThatThrownBy(() -> catalog.initialize(properties, mockAuthSession))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be a positive integer");
  }

  @Test
  public void testInitializeWithNullConfig() {
    assertThatThrownBy(() -> catalog.initialize(null, mockAuthSession))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid configuration: null");
  }

  @Test
  public void testListGenericTables() {
    initializeCatalog();

    Namespace namespace = Namespace.of("test_ns");
    TableIdentifier table1 = TableIdentifier.of(namespace, "table1");
    TableIdentifier table2 = TableIdentifier.of(namespace, "table2");

    ListGenericTablesRESTResponse response =
        new ListGenericTablesRESTResponse(null, ImmutableSet.of(table1, table2));

    when(mockClient.get(any(), anyMap(), eq(ListGenericTablesRESTResponse.class), anyMap(), any()))
        .thenReturn(response);

    List<TableIdentifier> tables = catalog.listGenericTables(namespace);

    assertThat(tables).hasSize(2);
    assertThat(tables).contains(table1, table2);
  }

  @Test
  public void testListGenericTablesWithPagination() {
    ConfigResponse configResponse =
        ConfigResponse.builder()
            .withDefaults(ImmutableMap.of())
            .withOverrides(ImmutableMap.of())
            .build();

    when(mockClient.get(any(), anyMap(), eq(ConfigResponse.class), anyMap(), any()))
        .thenReturn(configResponse);

    Map<String, String> properties =
        ImmutableMap.of(
            CatalogProperties.URI, "http://localhost:8181", PolarisRESTCatalog.REST_PAGE_SIZE, "2");

    catalog.initialize(properties, mockAuthSession);

    Namespace namespace = Namespace.of("test_ns");
    TableIdentifier table1 = TableIdentifier.of(namespace, "table1");
    TableIdentifier table2 = TableIdentifier.of(namespace, "table2");
    TableIdentifier table3 = TableIdentifier.of(namespace, "table3");

    ListGenericTablesRESTResponse response1 =
        new ListGenericTablesRESTResponse("page2", ImmutableSet.of(table1, table2));
    ListGenericTablesRESTResponse response2 =
        new ListGenericTablesRESTResponse(null, ImmutableSet.of(table3));

    when(mockClient.get(any(), anyMap(), eq(ListGenericTablesRESTResponse.class), anyMap(), any()))
        .thenReturn(response1, response2);

    List<TableIdentifier> tables = catalog.listGenericTables(namespace);

    assertThat(tables).hasSize(3);
    assertThat(tables).contains(table1, table2, table3);
  }

  @Test
  public void testCreateGenericTable() {
    initializeCatalog();

    TableIdentifier identifier = TableIdentifier.of("test_ns", "test_table");
    GenericTable table =
        GenericTable.builder()
            .setName("test_table")
            .setFormat("delta")
            .setBaseLocation("s3://bucket/path")
            .setDoc("Test table")
            .setProperties(ImmutableMap.of("key", "value"))
            .build();

    LoadGenericTableRESTResponse response = new LoadGenericTableRESTResponse(table);

    when(mockClient.post(any(), any(), eq(LoadGenericTableRESTResponse.class), anyMap(), any()))
        .thenReturn(response);

    GenericTable result =
        catalog.createGenericTable(
            identifier, "delta", "s3://bucket/path", "Test table", ImmutableMap.of("key", "value"));

    assertThat(result.getName()).isEqualTo("test_table");
    assertThat(result.getFormat()).isEqualTo("delta");
    assertThat(result.getBaseLocation()).isEqualTo("s3://bucket/path");
  }

  @Test
  public void testLoadGenericTable() {
    initializeCatalog();

    TableIdentifier identifier = TableIdentifier.of("test_ns", "test_table");
    GenericTable table = GenericTable.builder().setName("test_table").setFormat("delta").build();

    LoadGenericTableRESTResponse response = new LoadGenericTableRESTResponse(table);

    when(mockClient.get(any(), any(), eq(LoadGenericTableRESTResponse.class), anyMap(), any()))
        .thenReturn(response);

    GenericTable result = catalog.loadGenericTable(identifier);

    assertThat(result.getName()).isEqualTo("test_table");
    assertThat(result.getFormat()).isEqualTo("delta");
  }

  @Test
  public void testDropGenericTableSuccess() {
    initializeCatalog();

    TableIdentifier identifier = TableIdentifier.of("test_ns", "test_table");

    when(mockClient.delete(any(), any(), anyMap(), any())).thenReturn(null);

    boolean result = catalog.dropGenericTable(identifier);

    assertThat(result).isTrue();
  }

  @Test
  public void testDropGenericTableNotFound() {
    initializeCatalog();

    TableIdentifier identifier = TableIdentifier.of("test_ns", "test_table");

    when(mockClient.delete(any(), any(), anyMap(), any()))
        .thenThrow(new NoSuchTableException("Table not found"));

    boolean result = catalog.dropGenericTable(identifier);

    assertThat(result).isFalse();
  }

  @Test
  public void testFetchConfigWithWarehouseLocation() {
    RESTClient client = mock(RESTClient.class);
    Map<String, String> headers = ImmutableMap.of("Authorization", "Bearer token");
    Map<String, String> properties =
        ImmutableMap.of(
            CatalogProperties.URI,
            "http://localhost:8181",
            CatalogProperties.WAREHOUSE_LOCATION,
            "s3://warehouse");

    ConfigResponse expectedResponse =
        ConfigResponse.builder()
            .withDefaults(ImmutableMap.of())
            .withOverrides(ImmutableMap.of())
            .build();

    when(client.get(any(), anyMap(), eq(ConfigResponse.class), anyMap(), any()))
        .thenReturn(expectedResponse);

    ConfigResponse response = PolarisRESTCatalog.fetchConfig(client, headers, properties);

    assertThat(response).isNotNull();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> queryParamsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(client)
        .get(any(), queryParamsCaptor.capture(), eq(ConfigResponse.class), anyMap(), any());

    Map<String, String> capturedParams = queryParamsCaptor.getValue();
    assertThat(capturedParams)
        .containsEntry(CatalogProperties.WAREHOUSE_LOCATION, "s3://warehouse");
  }

  private void initializeCatalog() {
    ConfigResponse configResponse =
        ConfigResponse.builder()
            .withDefaults(ImmutableMap.of())
            .withOverrides(ImmutableMap.of())
            .withEndpoints(
                ImmutableList.of(
                    PolarisEndpoints.V1_LIST_GENERIC_TABLES,
                    PolarisEndpoints.V1_CREATE_GENERIC_TABLE,
                    PolarisEndpoints.V1_LOAD_GENERIC_TABLE,
                    PolarisEndpoints.V1_DELETE_GENERIC_TABLE))
            .build();

    when(mockClient.get(any(), anyMap(), eq(ConfigResponse.class), anyMap(), any()))
        .thenReturn(configResponse);

    Map<String, String> properties =
        ImmutableMap.of(CatalogProperties.URI, "http://localhost:8181");

    catalog.initialize(properties, mockAuthSession);
  }
}
