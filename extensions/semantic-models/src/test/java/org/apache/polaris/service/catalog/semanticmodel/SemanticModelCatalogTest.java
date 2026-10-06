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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
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
      "{\"name\":\"m\",\"datasets\":[{\"name\":\"d\",\"source\":\"sales.store_sales\"}]}";

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
        .setVersion("0.2.0.dev0")
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
            .setSpecVersion("0.2.0.dev0")
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
    assertThat(response.getDocument().getVersion()).isEqualTo("0.2.0.dev0");
    assertThat(response.getEntityVersion()).isEqualTo("1");
  }

  @Test
  void createRejectsEmptyDocument() {
    assertThatThrownBy(() -> catalog.createSemanticModel(IDENTIFIER, doc("")))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("must not be empty");
  }

  @Test
  void createRejectsArrayDocument() {
    assertThatThrownBy(
            () -> catalog.createSemanticModel(IDENTIFIER, doc("[" + VALID_MODEL_JSON + "]")))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("must be a JSON object");
  }

  @Test
  void createRejectsObjectFormDatasets() {
    // An object here would otherwise skip every dataset.source check, persisting a model whose
    // sources were never resolved.
    String objectDatasets = "{\"name\":\"m\",\"datasets\":{\"d\":{\"source\":\"does.not.exist\"}}}";
    assertThatThrownBy(() -> catalog.createSemanticModel(IDENTIFIER, doc(objectDatasets)))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("'semantic_model.datasets' must be a JSON array");
  }

  @Test
  void createAcceptsModelWithoutDatasets() {
    when(metaStoreManager.generateNewEntityId(any())).thenReturn(new GenerateEntityIdResult(10L));
    when(metaStoreManager.createEntityIfNotExists(any(), any(), any()))
        .thenAnswer(SemanticModelCatalogTest::echoPersistedEntity);

    String noDatasets = "{\"name\":\"m\"}";

    assertThatCode(() -> catalog.createSemanticModel(IDENTIFIER, doc(noDatasets)))
        .doesNotThrowAnyException();
  }

  @Test
  void createRejectsDatasetWithoutSource() {
    String noSource = "{\"name\":\"m\",\"datasets\":[{\"name\":\"d\"}]}";
    assertThatThrownBy(() -> catalog.createSemanticModel(IDENTIFIER, doc(noSource)))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("/semantic_model/datasets/0/source")
        .hasMessageContaining("must define a string 'source'");
  }

  @Test
  void createRejectsUnresolvedSource() {
    // No stub for the source table -> passthrough resolution returns null.
    assertThatThrownBy(() -> catalog.createSemanticModel(IDENTIFIER, doc(VALID_MODEL_JSON)))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("/semantic_model/datasets/0/source")
        .hasMessageContaining("store_sales");
  }

  @Test
  void createAcceptsDottedTableNameWhenUnambiguous() {
    // EntityNameValidator allows '.' in names. source "a.b.c" must be able to bind ns=[a],
    // table=b.c when that is the only matching TABLE_LIKE.
    Namespace nsA = Namespace.of("a");
    PolarisEntity nsEntity = entity(PolarisEntityType.NAMESPACE, 20L, "a", CATALOG_ID);
    PolarisEntity dottedTable =
        new PolarisEntity.Builder()
            .setType(PolarisEntityType.TABLE_LIKE)
            .setSubType(PolarisEntitySubType.ICEBERG_TABLE)
            .setId(21L)
            .setCatalogId(CATALOG_ID)
            .setParentId(20L)
            .setName("b.c")
            .build();
    when(sourceManifest.getPassthroughResolvedPath(
            eq(ResolvedPathKey.ofTableLike(TableIdentifier.of(nsA, "b.c"))),
            eq(PolarisEntitySubType.ANY_SUBTYPE)))
        .thenReturn(path(catalogEntity, nsEntity, dottedTable));
    when(metaStoreManager.generateNewEntityId(any())).thenReturn(new GenerateEntityIdResult(10L));
    when(metaStoreManager.createEntityIfNotExists(any(), any(), any()))
        .thenAnswer(SemanticModelCatalogTest::echoPersistedEntity);

    String model = "{\"name\":\"m\",\"datasets\":[{\"name\":\"d\",\"source\":\"a.b.c\"}]}";
    assertThatCode(() -> catalog.createSemanticModel(IDENTIFIER, doc(model)))
        .doesNotThrowAnyException();
  }

  @Test
  void createRejectsAmbiguousDottedSource() {
    // Both ns=[a], table=b.c and ns=[a, b], table=c exist -> refuse to guess.
    Namespace nsA = Namespace.of("a");
    Namespace nsAB = Namespace.of("a", "b");
    PolarisEntity nsAEntity = entity(PolarisEntityType.NAMESPACE, 20L, "a", CATALOG_ID);
    PolarisEntity nsBEntity = entity(PolarisEntityType.NAMESPACE, 21L, "b", CATALOG_ID);
    PolarisEntity dottedTable =
        new PolarisEntity.Builder()
            .setType(PolarisEntityType.TABLE_LIKE)
            .setSubType(PolarisEntitySubType.ICEBERG_TABLE)
            .setId(22L)
            .setCatalogId(CATALOG_ID)
            .setParentId(20L)
            .setName("b.c")
            .build();
    PolarisEntity nestedTable =
        new PolarisEntity.Builder()
            .setType(PolarisEntityType.TABLE_LIKE)
            .setSubType(PolarisEntitySubType.ICEBERG_TABLE)
            .setId(23L)
            .setCatalogId(CATALOG_ID)
            .setParentId(21L)
            .setName("c")
            .build();
    when(sourceManifest.getPassthroughResolvedPath(
            eq(ResolvedPathKey.ofTableLike(TableIdentifier.of(nsA, "b.c"))),
            eq(PolarisEntitySubType.ANY_SUBTYPE)))
        .thenReturn(path(catalogEntity, nsAEntity, dottedTable));
    when(sourceManifest.getPassthroughResolvedPath(
            eq(ResolvedPathKey.ofTableLike(TableIdentifier.of(nsAB, "c"))),
            eq(PolarisEntitySubType.ANY_SUBTYPE)))
        .thenReturn(path(catalogEntity, nsAEntity, nsBEntity, nestedTable));

    String model = "{\"name\":\"m\",\"datasets\":[{\"name\":\"d\",\"source\":\"a.b.c\"}]}";
    assertThatThrownBy(() -> catalog.createSemanticModel(IDENTIFIER, doc(model)))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("/semantic_model/datasets/0/source")
        .hasMessageContaining("could not be resolved uniquely");
  }

  @Test
  void createRejectsSourceWithTooManySegments() {
    String tooMany =
        "{\"name\":\"m\",\"datasets\":[{\"name\":\"d\",\"source\":\""
            + "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q"
            + "\"}]}";
    assertThatThrownBy(() -> catalog.createSemanticModel(IDENTIFIER, doc(tooMany)))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("/semantic_model/datasets/0/source")
        .hasMessageContaining("too many");
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
  void updateRejectsObjectFormDatasets() {
    // update shares resolveAndValidateSources with create, so the shape is rejected there too.
    stubExistingModel(3);
    String objectDatasets = "{\"name\":\"m\",\"datasets\":{\"d\":{\"source\":\"does.not.exist\"}}}";
    assertThatThrownBy(() -> catalog.updateSemanticModel(IDENTIFIER, doc(objectDatasets), "3"))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("'semantic_model.datasets' must be a JSON array");
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

  @ParameterizedTest
  @EnumSource(
      value = BaseResult.ReturnStatus.class,
      names = {"ENTITY_NOT_FOUND", "CATALOG_PATH_CANNOT_BE_RESOLVED"})
  void dropRejectsModelRemovedAfterResolution(BaseResult.ReturnStatus status) {
    stubExistingModel(1);
    when(metaStoreManager.dropEntityIfExists(any(), any(), any(), any(), eq(false)))
        .thenReturn(new DropEntityResult(status, null));
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
