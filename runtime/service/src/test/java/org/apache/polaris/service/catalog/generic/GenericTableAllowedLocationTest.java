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
package org.apache.polaris.service.catalog.generic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.ws.rs.core.Response;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.types.CreateGenericTableRequest;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class GenericTableAllowedLocationTest {
  private static final String CATALOG = "test-catalog";
  private static final String NAMESPACE = "ns";
  private static final UUID IDEMPOTENCY_KEY = new UUID(116617318654508422L, -7820829973016961092L);

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testNamespaceLocationRule(boolean allowUnstructuredLocation, @TempDir Path tempDir) {
    TestServices services =
        TestServices.builder()
            .config(
                Map.of(
                    "ALLOW_INSECURE_STORAGE_TYPES",
                    true,
                    "SUPPORTED_CATALOG_STORAGE_TYPES",
                    List.of("FILE"),
                    "ALLOW_NAMESPACE_CUSTOM_LOCATION",
                    "true"))
            .build();
    String catalogLocation = tempDir.resolve(CATALOG).toUri().toString();
    String namespaceLocation = tempDir.resolve(CATALOG).resolve(NAMESPACE).toUri().toString();
    String insideNamespaceLocation =
        tempDir.resolve(CATALOG).resolve(NAMESPACE).resolve("inside").toUri().toString();
    String outsideNamespaceLocation = tempDir.resolve("other").toUri().toString();

    Catalog catalog =
        new Catalog(
            Catalog.TypeEnum.INTERNAL,
            CATALOG,
            CatalogProperties.builder()
                .setDefaultBaseLocation(catalogLocation)
                .putAll(
                    Map.of(
                        FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
                        Boolean.toString(allowUnstructuredLocation)))
                .build(),
            1725487592064L,
            1725487592064L,
            1,
            FileStorageConfigInfo.builder()
                .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
                .setAllowedLocations(List.of(tempDir.toUri().toString()))
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

    CreateNamespaceRequest namespaceRequest =
        CreateNamespaceRequest.builder()
            .withNamespace(Namespace.of(NAMESPACE))
            .setProperties(new HashMap<>(Map.of("location", namespaceLocation)))
            .build();
    try (Response response =
        services
            .restApi()
            .createNamespace(
                CATALOG,
                namespaceRequest,
                IDEMPOTENCY_KEY,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    CreateGenericTableRequest insideNamespace =
        CreateGenericTableRequest.builder()
            .setName("inside")
            .setFormat("delta")
            .setBaseLocation(insideNamespaceLocation)
            .setProperties(Map.of())
            .build();
    try (Response response =
        services
            .genericTableApi()
            .createGenericTable(
                CATALOG,
                NAMESPACE,
                insideNamespace,
                null,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    CreateGenericTableRequest outsideNamespace =
        CreateGenericTableRequest.builder()
            .setName("outside")
            .setFormat("delta")
            .setBaseLocation(outsideNamespaceLocation)
            .setProperties(Map.of())
            .build();
    if (allowUnstructuredLocation) {
      try (Response response =
          services
              .genericTableApi()
              .createGenericTable(
                  CATALOG,
                  NAMESPACE,
                  outsideNamespace,
                  null,
                  services.realmContext(),
                  services.securityContext())) {
        assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      }
    } else {
      assertThatThrownBy(
              () ->
                  services
                      .genericTableApi()
                      .createGenericTable(
                          CATALOG,
                          NAMESPACE,
                          outsideNamespace,
                          null,
                          services.realmContext(),
                          services.securityContext()))
          .isInstanceOf(ForbiddenException.class)
          .hasMessageContaining("Invalid locations");
    }
  }
}
