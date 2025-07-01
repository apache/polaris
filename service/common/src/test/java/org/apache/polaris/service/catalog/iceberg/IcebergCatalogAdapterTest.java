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

package org.apache.polaris.service.catalog.iceberg;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.BearerAuthenticationParameters;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.TestServices;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Strings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

public class IcebergCatalogAdapterTest {

  private static final String FEDERATED_CATALOG_NAME = "polaris-federated-catalog";

  private TestServices testServices;
  private IcebergCatalogAdapter catalogAdapter;

  @BeforeEach
  public void setUp() {
    // Set up test services with catalog federation enabled
    testServices =
        TestServices.builder().config(Map.of("ENABLE_CATALOG_FEDERATION", "true")).build();
    catalogAdapter = Mockito.spy(testServices.catalogAdapter());

    // Prepare storage and connection configs for a federated Iceberg REST catalog
    String storageLocation = "s3://my-bucket/path/to/data";
    AwsStorageConfigInfo storageConfig =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/polaris-user-role")
            .setExternalId("externalId")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(storageLocation, "s3://externally-owned-bucket"))
            .build();

    AuthenticationParameters authParams =
        BearerAuthenticationParameters.builder()
            .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.BEARER)
            .setBearerToken("xxx")
            .build();

    IcebergRestConnectionConfigInfo connectionConfig =
        IcebergRestConnectionConfigInfo.builder()
            .setConnectionType(ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
            .setAuthenticationParameters(authParams)
            .setUri("http://localhost:8080/api/v1/catalogs")
            .setRemoteCatalogName("remote-catalog")
            .build();

    // Register the catalog in the test environment
    testServices
        .catalogsApi()
        .createCatalog(
            new CreateCatalogRequest(
                ExternalCatalog.builder()
                    .setName(FEDERATED_CATALOG_NAME)
                    .setProperties(
                        CatalogProperties.builder().setDefaultBaseLocation(storageLocation).build())
                    .setConnectionConfigInfo(connectionConfig)
                    .setStorageConfigInfo(storageConfig)
                    .build()),
            testServices.realmContext(),
            testServices.securityContext());
  }

  @ParameterizedTest(name = "[{index}] initialPageToken={0}, pageSize={1}")
  @MethodSource("paginationTestCases")
  void testPaginationForNonIcebergCatalog(String initialPageToken, Integer pageSize)
      throws IOException {

    try (InMemoryCatalog inMemoryCatalog = new InMemoryCatalog()) {
      // Initialize and replace the default handler with one backed by in-memory catalog
      inMemoryCatalog.initialize("inMemory", Map.of());
      mockCatalogAdapter(inMemoryCatalog);

      // Set up 10 entities in the catalog: 10 namespaces, 10 tables, 10 views
      int entityCount = 10;
      for (int i = 0; i < entityCount; ++i) {
        inMemoryCatalog.createNamespace(Namespace.of("ns" + i));
        inMemoryCatalog.createTable(TableIdentifier.of("ns0", "table" + i), new Schema());
        inMemoryCatalog
            .buildView(TableIdentifier.of("ns0", "view" + i))
            .withSchema(new Schema())
            .withDefaultNamespace(Namespace.of("ns0"))
            .withQuery("a", "SELECT * FROM ns0.table" + i)
            .create();
      }

      // Determine starting index for pagination based on the initial page token
      int pageStart =
          Strings.isNullOrEmpty(initialPageToken) ? 0 : Integer.parseInt(initialPageToken);
      int remain = entityCount - pageStart;

      // Initial tokens for pagination
      String listNamespacePageToken = initialPageToken;
      String listTablesPageToken = initialPageToken;
      String listViewsPageToken = initialPageToken;

      // Simulate page-by-page fetching until all entities are consumed
      while (remain > 0) {
        int expectedSize =
            (pageSize != null && initialPageToken != null) ? Math.min(remain, pageSize) : remain;

        // Verify namespaces pagination
        ListNamespacesResponse namespacesResponse =
            (ListNamespacesResponse)
                catalogAdapter
                    .listNamespaces(
                        FEDERATED_CATALOG_NAME,
                        listNamespacePageToken,
                        pageSize,
                        null,
                        testServices.realmContext(),
                        testServices.securityContext())
                    .getEntity();
        Assertions.assertThat(namespacesResponse.namespaces()).hasSize(expectedSize);
        listNamespacePageToken = namespacesResponse.nextPageToken();

        // Verify tables pagination
        ListTablesResponse tablesResponse =
            (ListTablesResponse)
                catalogAdapter
                    .listTables(
                        FEDERATED_CATALOG_NAME,
                        "ns0",
                        listTablesPageToken,
                        pageSize,
                        testServices.realmContext(),
                        testServices.securityContext())
                    .getEntity();
        Assertions.assertThat(tablesResponse.identifiers()).hasSize(expectedSize);
        listTablesPageToken = tablesResponse.nextPageToken();

        // Verify views pagination
        ListTablesResponse viewsResponse =
            (ListTablesResponse)
                catalogAdapter
                    .listViews(
                        FEDERATED_CATALOG_NAME,
                        "ns0",
                        listViewsPageToken,
                        pageSize,
                        testServices.realmContext(),
                        testServices.securityContext())
                    .getEntity();
        Assertions.assertThat(viewsResponse.identifiers()).hasSize(expectedSize);
        listViewsPageToken = viewsResponse.nextPageToken();

        remain -= expectedSize;
      }
    }
  }

  private void mockCatalogAdapter(org.apache.iceberg.catalog.Catalog catalog) {
    // Override handler creation to inject in-memory catalog and suppress actual close()
    Mockito.doAnswer(
            invocation -> {
              IcebergCatalogHandler realHandler =
                  (IcebergCatalogHandler) invocation.callRealMethod();
              IcebergCatalogHandler wrappedHandler = Mockito.spy(realHandler);

              // Override initializeCatalog to inject test catalog using reflection
              Mockito.doAnswer(
                      innerInvocation -> {
                        for (String fieldName :
                            List.of("baseCatalog", "namespaceCatalog", "viewCatalog")) {
                          Field field = IcebergCatalogHandler.class.getDeclaredField(fieldName);
                          field.setAccessible(true);
                          field.set(wrappedHandler, catalog);
                        }
                        return null;
                      })
                  .when(wrappedHandler)
                  .initializeCatalog();

              // Prevent catalog from being closed during test lifecycle
              Mockito.doNothing().when(wrappedHandler).close();

              return wrappedHandler;
            })
        .when(catalogAdapter)
        .newHandlerWrapper(Mockito.any(), Mockito.any());
  }

  private static Stream<Arguments> paginationTestCases() {
    return Stream.of(
        Arguments.of(null, null),
        Arguments.of(null, 1),
        Arguments.of(null, 3),
        Arguments.of(null, 5),
        Arguments.of(null, 10),
        Arguments.of(null, 20),
        Arguments.of("", null),
        Arguments.of("", 1),
        Arguments.of("", 3),
        Arguments.of("", 5),
        Arguments.of("", 10),
        Arguments.of("", 20),
        Arguments.of("5", null),
        Arguments.of("5", 1),
        Arguments.of("5", 3),
        Arguments.of("5", 5),
        Arguments.of("5", 10));
  }
}
