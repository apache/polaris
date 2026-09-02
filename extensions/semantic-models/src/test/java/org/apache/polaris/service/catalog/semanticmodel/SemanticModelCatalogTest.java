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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.GenerateEntityIdResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.semantic.SemanticModelEntity;
import org.apache.polaris.core.semantic.exceptions.NoSuchSemanticModelException;
import org.apache.polaris.core.semantic.exceptions.SemanticModelVersionMismatchException;
import org.apache.polaris.service.catalog.semanticmodel.types.LoadSemanticModelResponse;
import org.apache.polaris.service.catalog.semanticmodel.types.SemanticModelDocument;
import org.apache.polaris.service.catalog.semanticmodel.types.SemanticModelIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

/**
 * Unit tests for {@link SemanticModelCatalog}. Following the {@code extensions/auth/opa} pattern,
 * these are plain JUnit + Mockito tests that drive the catalog directly against a mocked resolution
 * view and metastore manager — no Quarkus bootstrap. Full-stack coverage is left to the
 * integration-test layer.
 */
class SemanticModelCatalogTest {

  private static final long CATALOG_ID = 1L;
  private static final Namespace NS = Namespace.of("sales");
  private static final String MODEL = "m";
  private static final SemanticModelIdentifier IDENTIFIER =
      SemanticModelIdentifier.builder().setNamespace(List.of("sales")).setName(MODEL).build();
  private static final String VALID_MODEL_JSON =
      "[{\"name\":\"m\",\"datasets\":[{\"name\":\"d\",\"source\":\"sales.store_sales\"}]}]";

  private PolarisResolutionManifestCatalogView view;
  private PolarisMetaStoreManager metaStoreManager;
  private ResolutionManifestFactory resolutionManifestFactory;
  private PolarisResolutionManifest sourceManifest;
  private SemanticModelCatalog catalog;

  private PolarisEntity catalogEntity;
  private PolarisEntity namespaceEntity;

  @BeforeEach
  void setUp() {
    view = mock(PolarisResolutionManifestCatalogView.class);
    metaStoreManager = mock(PolarisMetaStoreManager.class);
    resolutionManifestFactory = mock(ResolutionManifestFactory.class);
    sourceManifest = mock(PolarisResolutionManifest.class);
    PolarisPrincipal principal = mock(PolarisPrincipal.class);
    CallContext callContext = mock(CallContext.class);
    PolarisCallContext polarisCallContext = mock(PolarisCallContext.class);
    when(callContext.getPolarisCallContext()).thenReturn(polarisCallContext);

    catalogEntity = entity(PolarisEntityType.CATALOG, CATALOG_ID, "cat", 0L);
    namespaceEntity = entity(PolarisEntityType.NAMESPACE, 2L, "sales", CATALOG_ID);
    when(view.getResolvedCatalogEntity()).thenReturn(new CatalogEntity(catalogEntity));
    when(view.getResolvedPath(ResolvedPathKey.ofNamespace(NS)))
        .thenReturn(path(catalogEntity, namespaceEntity));
    // Source tables are resolved through a fresh single-use manifest, not the request view.
    when(resolutionManifestFactory.createResolutionManifest(any(), any()))
        .thenReturn(sourceManifest);

    catalog =
        new SemanticModelCatalog(
            metaStoreManager, callContext, view, resolutionManifestFactory, principal);
  }

  private SemanticModelDocument doc(String semanticModelJson) {
    return SemanticModelDocument.builder()
        .setVersion("0.1.1")
        .setSemanticModel(semanticModelJson)
        .build();
  }

  private void stubResolvableSource() {
    PolarisEntity table =
        new PolarisEntity.Builder()
            .setType(PolarisEntityType.TABLE_LIKE)
            .setSubType(PolarisEntitySubType.ICEBERG_TABLE)
            .setId(5L)
            .setCatalogId(CATALOG_ID)
            .setParentId(2L)
            .setName("store_sales")
            .build();
    when(sourceManifest.getPassthroughResolvedPath(
            eq(ResolvedPathKey.ofTableLike(TableIdentifier.of(NS, "store_sales"))),
            eq(PolarisEntitySubType.ANY_SUBTYPE)))
        .thenReturn(path(catalogEntity, namespaceEntity, table));
  }

  private void stubExistingModel(int entityVersion) {
    SemanticModelEntity stored =
        new SemanticModelEntity.Builder(NS, MODEL)
            .setSpecVersion("0.1.1")
            .setContent(VALID_MODEL_JSON)
            .setId(10L)
            .setCatalogId(CATALOG_ID)
            .setParentId(2L)
            .setEntityVersion(entityVersion)
            .build();
    when(view.getPassthroughResolvedPath(
            eq(ResolvedPathKey.ofSemanticModel(NS, MODEL)), eq(PolarisEntitySubType.NULL_SUBTYPE)))
        .thenReturn(path(catalogEntity, namespaceEntity, stored));
  }

  @Test
  void createResolvesSourcesAndPersists() {
    stubResolvableSource();
    when(metaStoreManager.generateNewEntityId(any())).thenReturn(new GenerateEntityIdResult(10L));
    // Echo the entity back as the persisted result.
    when(metaStoreManager.createEntityIfNotExists(any(), any(), any()))
        .thenAnswer(SemanticModelCatalogTest::echoPersistedEntity);

    LoadSemanticModelResponse response =
        catalog.createSemanticModel(IDENTIFIER, doc(VALID_MODEL_JSON));

    assertThat(response.getDocument().getSemanticModel()).isEqualTo(VALID_MODEL_JSON);
    assertThat(response.getDocument().getVersion()).isEqualTo("0.1.1");
    assertThat(response.getEntityVersion()).isEqualTo("1");
  }

  @Test
  void createRejectsEmptyDocument() {
    assertThatThrownBy(() -> catalog.createSemanticModel(IDENTIFIER, doc("")))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("must not be empty");
  }

  @Test
  void createRejectsUnresolvedSourceInObjectDocument() {
    String objectDocument =
        "{\"name\":\"m\",\"datasets\":[{\"name\":\"d\",\"source\":\"sales.missing\"}]}";
    assertThatThrownBy(() -> catalog.createSemanticModel(IDENTIFIER, doc(objectDocument)))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("/semantic_model/datasets/0/source")
        .hasMessageContaining("sales.missing");
  }

  @Test
  void createRejectsDatasetWithoutSource() {
    String noSource = "[{\"name\":\"m\",\"datasets\":[{\"name\":\"d\"}]}]";
    assertThatThrownBy(() -> catalog.createSemanticModel(IDENTIFIER, doc(noSource)))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("/semantic_model/0/datasets/0/source")
        .hasMessageContaining("must define a string 'source'");
  }

  @Test
  void createRejectsUnresolvedSource() {
    // No stub for the source table -> passthrough resolution returns null.
    assertThatThrownBy(() -> catalog.createSemanticModel(IDENTIFIER, doc(VALID_MODEL_JSON)))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("/semantic_model/0/datasets/0/source")
        .hasMessageContaining("store_sales");
  }

  @Test
  void createRejectsExistingModel() {
    stubResolvableSource();
    when(metaStoreManager.generateNewEntityId(any())).thenReturn(new GenerateEntityIdResult(10L));
    // Duplicates are surfaced by the persistence layer, not a pre-check.
    when(metaStoreManager.createEntityIfNotExists(any(), any(), any()))
        .thenReturn(new EntityResult(BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, null));
    assertThatThrownBy(() -> catalog.createSemanticModel(IDENTIFIER, doc(VALID_MODEL_JSON)))
        .isInstanceOf(AlreadyExistsException.class);
  }

  @Test
  void loadReturnsStoredDocument() {
    stubExistingModel(4);
    LoadSemanticModelResponse response = catalog.loadSemanticModel(IDENTIFIER);
    assertThat(response.getDocument().getSemanticModel()).isEqualTo(VALID_MODEL_JSON);
    assertThat(response.getEntityVersion()).isEqualTo("4");
  }

  @Test
  void loadRejectsMissingModel() {
    assertThatThrownBy(() -> catalog.loadSemanticModel(IDENTIFIER))
        .isInstanceOf(NoSuchSemanticModelException.class);
  }

  @Test
  void updateRejectsVersionMismatch() {
    stubExistingModel(3);
    assertThatThrownBy(() -> catalog.updateSemanticModel(IDENTIFIER, doc(VALID_MODEL_JSON), "1"))
        .isInstanceOf(SemanticModelVersionMismatchException.class);
  }

  @Test
  void updatePersistsWhenVersionMatches() {
    stubExistingModel(3);
    stubResolvableSource();
    when(metaStoreManager.updateEntityPropertiesIfNotChanged(any(), any(), any()))
        .thenAnswer(SemanticModelCatalogTest::echoPersistedEntity);

    LoadSemanticModelResponse response =
        catalog.updateSemanticModel(IDENTIFIER, doc(VALID_MODEL_JSON), "3");
    assertThat(response.getDocument().getSemanticModel()).isEqualTo(VALID_MODEL_JSON);
  }

  @Test
  void dropRemovesModel() {
    stubExistingModel(1);
    when(metaStoreManager.dropEntityIfExists(any(), any(), any(), any(), eq(false)))
        .thenReturn(new DropEntityResult());
    assertThatCode(() -> catalog.dropSemanticModel(IDENTIFIER)).doesNotThrowAnyException();
  }

  @Test
  void dropRejectsMissingModel() {
    assertThatThrownBy(() -> catalog.dropSemanticModel(IDENTIFIER))
        .isInstanceOf(NoSuchSemanticModelException.class);
  }

  @Test
  void listReturnsIdentifiers() {
    EntityNameLookupRecord record =
        new EntityNameLookupRecord(
            entity(PolarisEntityType.SEMANTIC_MODEL, 10L, MODEL, CATALOG_ID));
    when(metaStoreManager.listEntities(
            any(),
            any(),
            eq(PolarisEntityType.SEMANTIC_MODEL),
            eq(PolarisEntitySubType.NULL_SUBTYPE),
            any()))
        .thenReturn(new ListEntitiesResult(Page.fromItems(List.of(record))));

    var response = catalog.listSemanticModels(NS, PageToken.readEverything());
    assertThat(response.getIdentifiers())
        .singleElement()
        .satisfies(
            id -> {
              assertThat(id.getName()).isEqualTo(MODEL);
              assertThat(id.getNamespace()).containsExactly("sales");
            });
  }

  // ---- helpers ----

  /** Mockito answer that echoes the entity passed to a persist call back as the stored result. */
  private static EntityResult echoPersistedEntity(InvocationOnMock invocation) {
    return new EntityResult((PolarisBaseEntity) invocation.getArgument(2));
  }

  private static PolarisEntity entity(
      PolarisEntityType type, long id, String name, long catalogId) {
    return new PolarisEntity.Builder()
        .setType(type)
        .setId(id)
        .setCatalogId(catalogId)
        .setName(name)
        .build();
  }

  private static PolarisResolvedPathWrapper path(PolarisEntity... entities) {
    return new PolarisResolvedPathWrapper(
        Arrays.stream(entities)
            .map(e -> new ResolvedPolarisEntity(e, List.of(), List.of()))
            .toList());
  }
}
