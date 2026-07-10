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
package org.apache.polaris.service.catalog.semanticmodel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.semanticmodel.types.CreateSemanticModelRequest;
import org.apache.polaris.service.catalog.semanticmodel.types.SemanticModelDocument;
import org.apache.polaris.service.catalog.semanticmodel.types.SemanticModelIdentifier;
import org.apache.polaris.service.types.CreateGenericTableRequest;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;

/**
 * Shared bootstrap for {@link SemanticModelCatalogHandler} tests that run against the real
 * resolution manifest via {@link TestServices}: it stands up a catalog, a namespace, and a source
 * table ({@code ns1.t1}) so semantic-model {@code dataset.source} references resolve, and builds
 * handlers with a caller-supplied authorizer.
 */
abstract class AbstractSemanticModelCatalogHandlerTest {

  protected static final String CATALOG_NAME = "sm-catalog";
  protected static final String CATALOG_BASE_LOCATION = "file:///tmp/polaris-sm-test";
  protected static final Namespace NS = Namespace.of("ns1");
  protected static final String SOURCE_TABLE = "t1";
  private static final UUID IDEMPOTENCY_KEY = new UUID(116617318654508422L, -7820829973016961092L);

  protected TestServices services;

  @BeforeEach
  void bootstrap() {
    services =
        TestServices.builder()
            .config(
                Map.of(
                    "ALLOW_INSECURE_STORAGE_TYPES",
                    "true",
                    "SUPPORTED_CATALOG_STORAGE_TYPES",
                    List.of("FILE")))
            .build();
    createCatalogNamespaceAndTable();
  }

  /** Builds a handler wired to the given authorizer against the bootstrapped services. */
  protected SemanticModelCatalogHandler handler(PolarisAuthorizer authorizer) {
    return ImmutableSemanticModelCatalogHandler.builder()
        .catalogName(CATALOG_NAME)
        .polarisPrincipal(services.principal())
        .callContext(services.newCallContext())
        .resolutionManifestFactory(services.resolutionManifestFactory())
        .metaStoreManager(services.metaStoreManager())
        .authorizer(authorizer)
        .build();
  }

  /**
   * Handler that skips authorization; use for fixtures and non-authz assertions. The authorizer is
   * a mock stubbed to resolve the manifest (mirroring {@link TestServices}), since the base handler
   * now drives resolution through {@code resolveAuthorizationInputs} rather than resolving inline.
   */
  protected SemanticModelCatalogHandler passthroughHandler() {
    PolarisAuthorizer authorizer = Mockito.mock(PolarisAuthorizer.class);
    Mockito.doAnswer(
            invocation -> {
              AuthorizationState authzState = invocation.getArgument(0);
              authzState.getResolutionManifest().resolveAll();
              return null;
            })
        .when(authorizer)
        .resolveAuthorizationInputs(any(), any());
    return handler(authorizer);
  }

  protected static SemanticModelIdentifier identifier(String name) {
    return SemanticModelIdentifier.builder()
        .setNamespace(List.of(NS.levels()))
        .setName(name)
        .build();
  }

  protected static CreateSemanticModelRequest createRequest(String name, String model) {
    return CreateSemanticModelRequest.builder().setName(name).setDocument(doc(model)).build();
  }

  protected static SemanticModelDocument doc(String model) {
    return SemanticModelDocument.builder().setVersion("0.1.1").setSemanticModel(model).build();
  }

  protected static String modelJson(String source) {
    return "[{\"name\":\"m\",\"datasets\":[{\"name\":\"d\",\"source\":\"" + source + "\"}]}]";
  }

  private void createCatalogNamespaceAndTable() {
    Catalog catalogObject =
        new Catalog(
            Catalog.TypeEnum.INTERNAL,
            CATALOG_NAME,
            CatalogProperties.builder()
                .setDefaultBaseLocation(CATALOG_BASE_LOCATION + "/" + CATALOG_NAME)
                .build(),
            1725487592064L,
            1725487592064L,
            1,
            FileStorageConfigInfo.builder()
                .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
                .build());
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalogObject),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }

    try (Response response =
        services
            .restApi()
            .createNamespace(
                CATALOG_NAME,
                CreateNamespaceRequest.builder().withNamespace(NS).build(),
                IDEMPOTENCY_KEY,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    try (Response response =
        services
            .genericTableApi()
            .createGenericTable(
                CATALOG_NAME,
                NS.toString(),
                CreateGenericTableRequest.builder(SOURCE_TABLE, "iceberg")
                    .setBaseLocation(CATALOG_BASE_LOCATION + "/" + CATALOG_NAME + "/ns1/t1")
                    .build(),
                null,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }
}
