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

import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.http.IcebergHttpUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies the end-to-end ETag behavior after {@link IcebergCatalogHandler} became responsible for
 * computing the etag (packaged in {@link ETaggedLoadTableResponse}). The adapter only copies the
 * etag onto the HTTP response, so any regression in the handler surfaces here as a missing or
 * incorrect {@code ETag} header.
 */
public class IcebergCatalogAdapterETagTest {

  private static final String CATALOG = "test-catalog";
  private static final String NAMESPACE = "ns";

  private String catalogLocation;

  @BeforeEach
  public void setUp(@TempDir Path tempDir) {
    catalogLocation = tempDir.toAbsolutePath().toUri().toString();
    if (catalogLocation.endsWith("/")) {
      catalogLocation = catalogLocation.substring(0, catalogLocation.length() - 1);
    }
  }

  @Test
  void createTableDirectResponseContainsExpectedETag() {
    TestServices services = createTestServices();
    createCatalogAndNamespace(services);

    try (Response response = createTable(services, "etag_direct_table")) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(response.getHeaders()).containsKey(HttpHeaders.ETAG);

      LoadTableResponse body = (LoadTableResponse) response.getEntity();
      String expected =
          IcebergHttpUtil.generateETagForMetadataFileLocation(body.metadataLocation());
      assertThat(response.getHeaders().getFirst(HttpHeaders.ETAG)).hasToString(expected);
    }
  }

  @Test
  void stagedCreateResponseOmitsETag() {
    TestServices services = createTestServices();
    createCatalogAndNamespace(services);

    CreateTableRequest stagedRequest =
        CreateTableRequest.builder()
            .withName("staged_table")
            .withLocation(tableLocation("staged_table"))
            .withSchema(SCHEMA)
            .stageCreate()
            .build();

    try (Response response =
        services
            .restApi()
            .createTable(
                CATALOG,
                NAMESPACE,
                stagedRequest,
                null,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      // Staged creates have no metadata location yet, so the handler must omit the etag.
      assertThat(response.getHeaders()).doesNotContainKey(HttpHeaders.ETAG);
    }
  }

  @Test
  void loadTableResponseContainsETagMatchingCreate() {
    TestServices services = createTestServices();
    createCatalogAndNamespace(services);

    String createETag;
    try (Response createResponse = createTable(services, "etag_load_table")) {
      createETag = createResponse.getHeaders().getFirst(HttpHeaders.ETAG).toString();
    }

    try (Response loadResponse =
        services
            .restApi()
            .loadTable(
                CATALOG,
                NAMESPACE,
                "etag_load_table",
                null,
                null,
                null,
                services.realmContext(),
                services.securityContext())) {
      assertThat(loadResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(loadResponse.getHeaders()).containsKey(HttpHeaders.ETAG);
      assertThat(loadResponse.getHeaders().getFirst(HttpHeaders.ETAG)).hasToString(createETag);
    }
  }

  @Test
  void loadTableWithMatchingIfNoneMatchReturnsNotModified() {
    TestServices services = createTestServices();
    createCatalogAndNamespace(services);

    String etag;
    try (Response createResponse = createTable(services, "etag_ifnm_match_table")) {
      etag = createResponse.getHeaders().getFirst(HttpHeaders.ETAG).toString();
    }

    try (Response loadResponse =
        services
            .restApi()
            .loadTable(
                CATALOG,
                NAMESPACE,
                "etag_ifnm_match_table",
                null,
                etag,
                null,
                services.realmContext(),
                services.securityContext())) {
      assertThat(loadResponse.getStatus())
          .isEqualTo(Response.Status.NOT_MODIFIED.getStatusCode());
      assertThat(loadResponse.hasEntity()).isFalse();
    }
  }

  @Test
  void loadTableWithMismatchedIfNoneMatchReturnsOkWithCurrentETag() {
    TestServices services = createTestServices();
    createCatalogAndNamespace(services);

    String currentETag;
    try (Response createResponse = createTable(services, "etag_ifnm_miss_table")) {
      currentETag = createResponse.getHeaders().getFirst(HttpHeaders.ETAG).toString();
    }

    String staleETag = "W/\"stale-etag-value\"";
    try (Response loadResponse =
        services
            .restApi()
            .loadTable(
                CATALOG,
                NAMESPACE,
                "etag_ifnm_miss_table",
                null,
                staleETag,
                null,
                services.realmContext(),
                services.securityContext())) {
      assertThat(loadResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(loadResponse.getHeaders().getFirst(HttpHeaders.ETAG)).hasToString(currentETag);
    }
  }

  @Test
  void registerTableResponseContainsExpectedETag() {
    TestServices services = createTestServices();
    createCatalogAndNamespace(services);

    // Create a table so that a metadata file exists on disk, then drop it (keeping the files) so
    // we can re-register a new table at the same metadata location without a location conflict.
    String tableName = "registered_table";
    String metadataLocation;
    try (Response createResponse = createTable(services, tableName)) {
      metadataLocation = ((LoadTableResponse) createResponse.getEntity()).metadataLocation();
    }
    try (Response dropResponse =
        services
            .restApi()
            .dropTable(
                CATALOG,
                NAMESPACE,
                tableName,
                false,
                services.realmContext(),
                services.securityContext())) {
      assertThat(dropResponse.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    final String finalMetadataLocation = metadataLocation;
    RegisterTableRequest registerRequest =
        new RegisterTableRequest() {
          @Override
          public String name() {
            return tableName;
          }

          @Override
          public String metadataLocation() {
            return finalMetadataLocation;
          }
        };

    try (Response registerResponse =
        services
            .restApi()
            .registerTable(
                CATALOG,
                NAMESPACE,
                registerRequest,
                services.realmContext(),
                services.securityContext())) {
      assertThat(registerResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(registerResponse.getHeaders()).containsKey(HttpHeaders.ETAG);

      String expected = IcebergHttpUtil.generateETagForMetadataFileLocation(metadataLocation);
      assertThat(registerResponse.getHeaders().getFirst(HttpHeaders.ETAG)).hasToString(expected);
    }
  }

  private Response createTable(TestServices services, String tableName) {
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder()
            .withName(tableName)
            .withLocation(tableLocation(tableName))
            .withSchema(SCHEMA)
            .build();
    return services
        .restApi()
        .createTable(
            CATALOG,
            NAMESPACE,
            createTableRequest,
            null,
            services.realmContext(),
            services.securityContext());
  }

  private String tableLocation(String tableName) {
    return String.format("%s/%s/%s/%s", catalogLocation, CATALOG, NAMESPACE, tableName);
  }

  private void createCatalogAndNamespace(TestServices services) {
    CatalogProperties props =
        CatalogProperties.builder()
            .setDefaultBaseLocation(String.format("%s/%s", catalogLocation, CATALOG))
            .build();
    StorageConfigInfo storage =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .build();
    Catalog catalog =
        new Catalog(Catalog.TypeEnum.INTERNAL, CATALOG, props, 0L, 0L, 1, storage);
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }

    try (Response response =
        services
            .restApi()
            .createNamespace(
                CATALOG,
                org.apache.iceberg.rest.requests.CreateNamespaceRequest.builder()
                    .withNamespace(org.apache.iceberg.catalog.Namespace.of(NAMESPACE))
                    .build(),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  private TestServices createTestServices() {
    Map<String, Object> config =
        Map.of(
            "ALLOW_INSECURE_STORAGE_TYPES",
            "true",
            "SUPPORTED_CATALOG_STORAGE_TYPES",
            List.of("FILE"));
    return TestServices.builder().config(config).build();
  }
}
