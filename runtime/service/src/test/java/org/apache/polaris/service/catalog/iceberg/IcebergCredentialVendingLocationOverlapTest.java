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

import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_TABLE_LOCATION_OVERLAP;
import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION;
import static org.apache.polaris.core.config.FeatureConfiguration.OPTIMIZED_SIBLING_CHECK;
import static org.apache.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.ws.rs.core.Response;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.AccessDelegationMode;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests that credentials are not vended for a table whose locations outside its base location
 * overlap another table, for instance because the overlapping table was created while location
 * overlap was allowed.
 *
 * <p>Unstructured table locations are allowed: otherwise, credentials are only vended for locations
 * under the table's base location.
 */
public class IcebergCredentialVendingLocationOverlapTest {

  private static final String CATALOG = "test-catalog";
  private static final Namespace NAMESPACE = Namespace.of("ns");
  private static final TableIdentifier T1 = TableIdentifier.of(NAMESPACE, "t1");
  private static final TableIdentifier T2 = TableIdentifier.of(NAMESPACE, "t2");

  @TempDir Path tmpDir;

  private TestServices services;
  private String catalogLocation;

  private void setUp(boolean optimizedSiblingCheck) {
    services =
        TestServices.builder()
            .config(
                Map.of(
                    "ALLOW_INSECURE_STORAGE_TYPES",
                    "true",
                    "SUPPORTED_CATALOG_STORAGE_TYPES",
                    List.of("FILE"),
                    ALLOW_UNSTRUCTURED_TABLE_LOCATION.key(),
                    "true",
                    OPTIMIZED_SIBLING_CHECK.key(),
                    String.valueOf(optimizedSiblingCheck)))
            .build();
    catalogLocation = tmpDir.toAbsolutePath().toUri() + CATALOG;
    // Location overlap is allowed at first, so that overlapping tables can be created.
    Catalog catalog =
        new Catalog(
            Catalog.TypeEnum.INTERNAL,
            CATALOG,
            CatalogProperties.builder()
                .setDefaultBaseLocation(catalogLocation)
                .putAll(Map.of(ALLOW_TABLE_LOCATION_OVERLAP.catalogConfig(), "true"))
                .build(),
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            1,
            FileStorageConfigInfo.builder()
                .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
                .setAllowedLocations(List.of(catalogLocation))
                .build());
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
    handler().createNamespace(CreateNamespaceRequest.builder().withNamespace(NAMESPACE).build());
  }

  private void disallowTableLocationOverlap() {
    Catalog fetched;
    try (Response response =
        services
            .catalogsApi()
            .getCatalog(CATALOG, services.realmContext(), services.securityContext())) {
      fetched = response.readEntity(Catalog.class);
    }
    UpdateCatalogRequest request =
        UpdateCatalogRequest.builder()
            .setCurrentEntityVersion(fetched.getEntityVersion())
            .setProperties(
                Map.of(
                    "default-base-location",
                    catalogLocation,
                    ALLOW_TABLE_LOCATION_OVERLAP.catalogConfig(),
                    "false"))
            .build();
    try (Response response =
        services
            .catalogsApi()
            .updateCatalog(CATALOG, request, services.realmContext(), services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  private IcebergCatalogHandler handler() {
    return services.catalogAdapter().newHandler(services.securityContext(), CATALOG);
  }

  private void createTable(TableIdentifier identifier, String location, String writeDataPath) {
    CreateTableRequest.Builder request =
        CreateTableRequest.builder()
            .withName(identifier.name())
            .withSchema(SCHEMA)
            .withLocation(location);
    if (writeDataPath != null) {
      request.setProperty(TableProperties.WRITE_DATA_LOCATION, writeDataPath);
    }
    handler()
        .createTableDirect(
            NAMESPACE,
            request.build(),
            EnumSet.noneOf(AccessDelegationMode.class),
            Optional.empty());
  }

  private String tableLocation(String path) {
    return catalogLocation + "/" + NAMESPACE + "/" + path;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCredentialsNotVendedForWritePathOverlappingSibling(boolean optimizedSiblingCheck) {
    setUp(optimizedSiblingCheck);
    createTable(T1, null, tableLocation("t2"));
    createTable(T2, null, null);
    disallowTableLocationOverlap();

    assertThatThrownBy(() -> handler().loadCredentials(T1, Optional.empty()))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("overlap another table or namespace");
    assertThatThrownBy(
            () ->
                handler()
                    .loadTable(
                        T1,
                        "all",
                        null,
                        EnumSet.of(AccessDelegationMode.VENDED_CREDENTIALS),
                        Optional.empty()))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("overlap another table or namespace");

    // Loading the table without credentials, and vending credentials for the other table, still
    // work.
    assertThatCode(
            () ->
                handler()
                    .loadTable(
                        T1,
                        "all",
                        null,
                        EnumSet.noneOf(AccessDelegationMode.class),
                        Optional.empty()))
        .doesNotThrowAnyException();
    assertThatCode(() -> handler().loadCredentials(T2, Optional.empty()))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCredentialsVendedForWritePathContainingOwnLocation(boolean optimizedSiblingCheck) {
    setUp(optimizedSiblingCheck);
    // The write path contains the table's own location, but no other table or namespace.
    createTable(T1, tableLocation("dir/t1"), tableLocation("dir"));
    disallowTableLocationOverlap();

    assertThatCode(() -> handler().loadCredentials(T1, Optional.empty()))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCredentialsVendedWhenTableLocationOverlapAllowed(boolean optimizedSiblingCheck) {
    setUp(optimizedSiblingCheck);
    createTable(T1, null, tableLocation("t2"));
    createTable(T2, null, null);

    assertThatCode(() -> handler().loadCredentials(T1, Optional.empty()))
        .doesNotThrowAnyException();
  }
}
