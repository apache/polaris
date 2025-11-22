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

import static org.apache.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import jakarta.ws.rs.core.Response;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.events.IcebergRestCatalogEvents;
import org.apache.polaris.service.events.listeners.TestPolarisEventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class CommitTransactionEventTest {
  private static final String namespace = "ns";
  private static final String catalog = "test-catalog";
  private static final String propertyName = "custom-property-1";

  private String catalogLocation;

  @BeforeEach
  public void setUp(@TempDir Path tempDir) {
    catalogLocation = tempDir.toAbsolutePath().toUri().toString();
    if (catalogLocation.endsWith("/")) {
      catalogLocation = catalogLocation.substring(0, catalogLocation.length() - 1);
    }
  }

  @Test
  void testEventsForSuccessfulTransaction() {
    TestServices testServices = createTestServices(false);
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);

    executeTransactionTest("test-table-1", "test-table-2", testServices);

    // Verify that all (Before/After/Stage)CommitTable events were emitted
    TestPolarisEventListener testEventListener =
        (TestPolarisEventListener) testServices.polarisEventListener();
    assertThat(
            testEventListener
                .getLatest(IcebergRestCatalogEvents.BeforeCommitTableEvent.class)
                .identifier()
                .name())
        .isEqualTo("test-table-2");
    assertThat(
            testEventListener
                .getLatest(IcebergRestCatalogEvents.StageCommitTableEvent.class)
                .identifier()
                .name())
        .isEqualTo("test-table-2");
    assertThat(
            testEventListener
                .getLatest(IcebergRestCatalogEvents.AfterCommitTableEvent.class)
                .identifier()
                .name())
        .isEqualTo("test-table-2");
    assertThat(
            testEventListener
                .getLatest(IcebergRestCatalogEvents.AfterCommitTableEvent.class)
                .metadataAfter()
                .properties())
        .containsKey(propertyName);
  }

  @Test
  void testEventsForUnSuccessfulTransaction() {
    TestServices testServices = createTestServices(true);
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);

    executeTransactionTest("test-table-3", "test-table-4", testServices);

    // Verify that all (Before/Stage)CommitTable events were emitted
    TestPolarisEventListener testEventListener =
        (TestPolarisEventListener) testServices.polarisEventListener();
    assertThat(
            testEventListener
                .getLatest(IcebergRestCatalogEvents.BeforeCommitTableEvent.class)
                .identifier()
                .name())
        .isEqualTo("test-table-4");
    assertThat(
            testEventListener
                .getLatest(IcebergRestCatalogEvents.StageCommitTableEvent.class)
                .identifier()
                .name())
        .isEqualTo("test-table-4");

    // Verify that the AfterCommitTable events that were emitted were for the earlier create table
    // call and therefore, we did not emit an AfterCommitTableEvent for the failed transaction
    assertThat(
            testEventListener
                .getLatest(IcebergRestCatalogEvents.AfterCommitTableEvent.class)
                .metadataBefore())
        .isNull();
  }

  private void createCatalogAndNamespace(
      TestServices services, Map<String, String> catalogConfig, String catalogLocation) {
    CatalogProperties.Builder propertiesBuilder =
        CatalogProperties.builder()
            .setDefaultBaseLocation(String.format("%s/%s", catalogLocation, catalog))
            .putAll(catalogConfig);

    StorageConfigInfo config =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .build();
    Catalog catalogObject =
        new Catalog(
            Catalog.TypeEnum.INTERNAL, catalog, propertiesBuilder.build(), 0L, 0L, 1, config);
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalogObject),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }

    CreateNamespaceRequest createNamespaceRequest =
        CreateNamespaceRequest.builder().withNamespace(Namespace.of(namespace)).build();
    try (Response response =
        services
            .restApi()
            .createNamespace(
                catalog,
                createNamespaceRequest,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  private void createTable(TestServices services, String tableName, String baseLocation) {
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder()
            .withName(tableName)
            .withLocation(String.format("%s/%s/%s/%s", baseLocation, catalog, namespace, tableName))
            .withSchema(SCHEMA)
            .build();
    services
        .restApi()
        .createTable(
            catalog,
            namespace,
            createTableRequest,
            null,
            services.realmContext(),
            services.securityContext());
  }

  /**
   * Creates TestServices with optional hijacking for failure simulation.
   *
   * @param shouldFail if true, creates services that will fail during transaction commit
   */
  private TestServices createTestServices(boolean shouldFail) {
    Map<String, Object> config =
        Map.of(
            "ALLOW_INSECURE_STORAGE_TYPES",
            "true",
            "SUPPORTED_CATALOG_STORAGE_TYPES",
            List.of("FILE"));
    TestServices testServices = TestServices.builder().config(config).build();
    if (!shouldFail) {
      return testServices;
    }

    // Create a spy on the existing testServices' metastore manager for failure simulation
    PolarisMetaStoreManager spyMetaStoreManager = spy(testServices.metaStoreManager());

    // Return an error when trying to apply the updates on the spy
    doReturn(new EntitiesResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, ""))
        .when(spyMetaStoreManager)
        .updateEntitiesPropertiesIfNotChanged(any(), any());

    MetaStoreManagerFactory metaStoreManagerFactorySpy =
        spy(testServices.metaStoreManagerFactory());
    doReturn(spyMetaStoreManager)
        .when(metaStoreManagerFactorySpy)
        .getOrCreateMetaStoreManager(any());

    return TestServices.builder()
        .metaStoreManagerFactory(metaStoreManagerFactorySpy)
        .config(config)
        .build();
  }

  /**
   * Executes a transaction test with the specified parameters.
   *
   * @param table1Name name of the first table
   * @param table2Name name of the second table
   * @param testServices TestServices object that will be operated on
   */
  private void executeTransactionTest(
      String table1Name, String table2Name, TestServices testServices) {
    // Setup the test tables
    createTable(testServices, table1Name, catalogLocation);
    createTable(testServices, table2Name, catalogLocation);

    CommitTransactionRequest commitRequest =
        new CommitTransactionRequest(
            List.of(
                UpdateTableRequest.create(
                    TableIdentifier.of(namespace, table1Name),
                    List.of(),
                    List.of(new MetadataUpdate.SetProperties(Map.of(propertyName, "value1")))),
                UpdateTableRequest.create(
                    TableIdentifier.of(namespace, table2Name),
                    List.of(),
                    List.of(new MetadataUpdate.SetProperties(Map.of(propertyName, "value2"))))));

    // Ignore any errors that occur during transaction commit
    try {
      testServices
          .restApi()
          .commitTransaction(
              catalog, commitRequest, testServices.realmContext(), testServices.securityContext());
    } catch (Exception ignored) {
    }
  }
}
