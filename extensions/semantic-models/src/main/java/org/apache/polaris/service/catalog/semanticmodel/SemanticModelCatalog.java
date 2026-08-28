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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.semantic.SemanticModelEntity;
import org.apache.polaris.core.semantic.exceptions.NoSuchSemanticModelException;
import org.apache.polaris.core.semantic.exceptions.SemanticModelVersionMismatchException;
import org.apache.polaris.service.catalog.semanticmodel.types.ListSemanticModelsResponse;
import org.apache.polaris.service.catalog.semanticmodel.types.LoadSemanticModelResponse;
import org.apache.polaris.service.catalog.semanticmodel.types.SemanticModelDocument;
import org.apache.polaris.service.catalog.semanticmodel.types.SemanticModelIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core create/list/load/update/drop logic for Apache Ossie semantic models.
 *
 * <p>The Ossie document body is stored inside the entity {@code properties} map. Writes resolve
 * every {@code dataset.source} to a {@code TABLE_LIKE} entity in the current catalog, and updates
 * use optimistic concurrency on the entity version. Document schema validation is a separate
 * concern (see {@link SemanticDocumentValidator}).
 */
public class SemanticModelCatalog {
  private static final Logger LOGGER = LoggerFactory.getLogger(SemanticModelCatalog.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Separator between the namespace path and the name in an Ossie {@code dataset.source}, per the
   * IRC catalog object-identifier scheme (e.g. {@code sales.store_sales}).
   */
  private static final char SOURCE_SEPARATOR = '.';

  private final CallContext callContext;
  private final PolarisResolutionManifestCatalogView resolvedEntityView;
  private final CatalogEntity catalogEntity;
  private final long catalogId;
  private final PolarisMetaStoreManager metaStoreManager;
  private final ResolutionManifestFactory resolutionManifestFactory;
  private final PolarisPrincipal principal;

  public SemanticModelCatalog(
      PolarisMetaStoreManager metaStoreManager,
      CallContext callContext,
      PolarisResolutionManifestCatalogView resolvedEntityView,
      ResolutionManifestFactory resolutionManifestFactory,
      PolarisPrincipal principal) {
    this.callContext = callContext;
    this.resolvedEntityView = resolvedEntityView;
    this.catalogEntity = resolvedEntityView.getResolvedCatalogEntity();
    this.catalogId = catalogEntity.getId();
    this.metaStoreManager = metaStoreManager;
    this.resolutionManifestFactory = resolutionManifestFactory;
    this.principal = principal;
  }

  public LoadSemanticModelResponse createSemanticModel(
      SemanticModelIdentifier identifier, SemanticModelDocument document) {
    Namespace namespace = toNamespace(identifier);
    PolarisResolvedPathWrapper resolvedParent =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(namespace));
    if (resolvedParent == null) {
      // Illegal state because the namespace should've already been in the static resolution set.
      throw new IllegalStateException(
          String.format("Failed to fetch resolved parent for semantic model '%s'", identifier));
    }
    List<PolarisEntity> catalogPath = resolvedParent.getRawFullPath();

    // Parse the document, then resolve its source tables before persisting. Duplicate names are
    // caught by createEntityIfNotExists below (ENTITY_ALREADY_EXISTS), so there is no separate
    // existence pre-check: the semantic-model path is not registered on this request's resolution
    // manifest, and probing it there would fail.
    JsonNode semanticModel = validateDocument(document);
    resolveAndValidateSources(semanticModel);

    SemanticModelEntity entity =
        new SemanticModelEntity.Builder(namespace, identifier.getName())
            .setCatalogId(catalogId)
            .setParentId(resolvedParent.getRawLeafEntity().getId())
            .setSpecVersion(document.getVersion())
            .setContent(document.getSemanticModel())
            .setId(
                metaStoreManager.generateNewEntityId(callContext.getPolarisCallContext()).getId())
            .setCreateTimestamp(System.currentTimeMillis())
            .build();

    EntityResult res =
        metaStoreManager.createEntityIfNotExists(
            callContext.getPolarisCallContext(), PolarisEntity.toCoreList(catalogPath), entity);
    if (!res.isSuccess()) {
      switch (res.getReturnStatus()) {
        case ENTITY_ALREADY_EXISTS ->
            throw new AlreadyExistsException(
                "Semantic model already exists: %s", identifier.getName());
        default ->
            throw new IllegalStateException(
                String.format(
                    "Unknown error status for identifier %s: %s with extraInfo: %s",
                    identifier, res.getReturnStatus(), res.getExtraInformation()));
      }
    }

    SemanticModelEntity result = SemanticModelEntity.of(res.getEntity());
    LOGGER.debug("Created semantic model entity {} with identifier {}", result, identifier);
    return toLoadResponse(result);
  }

  public ListSemanticModelsResponse listSemanticModels(Namespace namespace, PageToken pageToken) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(namespace));
    if (resolvedEntities == null) {
      throw new IllegalStateException(
          String.format("Failed to fetch resolved namespace '%s'", namespace));
    }
    List<PolarisEntity> catalogPath = resolvedEntities.getRawFullPath();

    ListEntitiesResult result =
        metaStoreManager.listEntities(
            callContext.getPolarisCallContext(),
            PolarisEntity.toCoreList(catalogPath),
            PolarisEntityType.SEMANTIC_MODEL,
            PolarisEntitySubType.NULL_SUBTYPE,
            pageToken);
    if (!result.isSuccess()) {
      throw new IllegalStateException("Failed to list semantic models in namespace: " + namespace);
    }

    // LinkedHashSet keeps the page order stable in the serialized response.
    Set<SemanticModelIdentifier> identifiers =
        result.getEntities().stream()
            .map(
                record ->
                    SemanticModelIdentifier.builder()
                        .setNamespace(List.of(namespace.levels()))
                        .setName(record.getName())
                        .build())
            .collect(Collectors.toCollection(LinkedHashSet::new));
    return ListSemanticModelsResponse.builder()
        .setIdentifiers(identifiers)
        .setNextPageToken(result.getPage().encodedResponseToken())
        .build();
  }

  public LoadSemanticModelResponse loadSemanticModel(SemanticModelIdentifier identifier) {
    return toLoadResponse(resolveModelOrThrow(identifier));
  }

  public LoadSemanticModelResponse updateSemanticModel(
      SemanticModelIdentifier identifier, SemanticModelDocument document, String expectedVersion) {
    PolarisResolvedPathWrapper resolvedPath = resolveModelPathOrThrow(identifier);
    SemanticModelEntity current = SemanticModelEntity.of(resolvedPath.getRawLeafEntity());

    String currentVersion = Integer.toString(current.getEntityVersion());
    if (!currentVersion.equals(expectedVersion)) {
      throw new SemanticModelVersionMismatchException(
          String.format(
              "Semantic model version mismatch. Given version is %s, current version is %s",
              expectedVersion, currentVersion));
    }

    JsonNode semanticModel = validateDocument(document);
    resolveAndValidateSources(semanticModel);

    SemanticModelEntity newEntity =
        new SemanticModelEntity.Builder(current)
            .setSpecVersion(document.getVersion())
            .setContent(document.getSemanticModel())
            .build();

    List<PolarisEntity> catalogPath = resolvedPath.getRawParentPath();
    SemanticModelEntity updated =
        Optional.ofNullable(
                metaStoreManager
                    .updateEntityPropertiesIfNotChanged(
                        callContext.getPolarisCallContext(),
                        PolarisEntity.toCoreList(catalogPath),
                        newEntity)
                    .getEntity())
            .map(SemanticModelEntity::of)
            .orElse(null);
    if (updated == null) {
      // Lost the optimistic-concurrency race with a concurrent writer.
      throw new SemanticModelVersionMismatchException(
          String.format(
              "Semantic model %s was modified concurrently; retry after reloading",
              identifier.getName()));
    }

    return toLoadResponse(updated);
  }

  public void dropSemanticModel(SemanticModelIdentifier identifier) {
    PolarisResolvedPathWrapper resolvedPath = resolveModelPathOrThrow(identifier);
    List<PolarisEntity> catalogPath = resolvedPath.getRawParentPath();
    PolarisBaseEntity entity = resolvedPath.getRawLeafEntity();

    var result =
        metaStoreManager.dropEntityIfExists(
            callContext.getPolarisCallContext(),
            PolarisEntity.toCoreList(catalogPath),
            entity,
            Map.of(),
            false);
    if (!result.isSuccess()) {
      throw new IllegalStateException(
          String.format(
              "Failed to drop semantic model %s error status: %s with extraInfo: %s",
              identifier, result.getReturnStatus(), result.getExtraInformation()));
    }
  }

  private PolarisResolvedPathWrapper resolveModelPathOrThrow(SemanticModelIdentifier identifier) {
    Namespace namespace = toNamespace(identifier);
    PolarisResolvedPathWrapper resolved =
        resolvedEntityView.getPassthroughResolvedPath(
            ResolvedPathKey.ofSemanticModel(namespace, identifier.getName()),
            PolarisEntitySubType.NULL_SUBTYPE);
    if (resolved == null || resolved.getRawLeafEntity() == null) {
      throw new NoSuchSemanticModelException(
          String.format("Semantic model does not exist: %s", identifier.getName()));
    }
    return resolved;
  }

  private SemanticModelEntity resolveModelOrThrow(SemanticModelIdentifier identifier) {
    return SemanticModelEntity.of(resolveModelPathOrThrow(identifier).getRawLeafEntity());
  }

  /**
   * Parses the Ossie document and returns the {@code semantic_model} body. Currently only checks
   * that the required body is present and well-formed JSON; full schema validation is deferred to a
   * {@link SemanticDocumentValidator} implementation.
   *
   * @return the parsed {@code semantic_model} node
   * @throws BadRequestException if the body is missing/blank or not valid JSON
   */
  private JsonNode validateDocument(SemanticModelDocument document) {
    String body = document.getSemanticModel();
    if (body == null || body.isBlank()) {
      throw new BadRequestException("Semantic model document must not be empty");
    }
    try {
      // TODO: delegate full Ossie JSON-schema validation to SemanticDocumentValidator.
      return MAPPER.readTree(body);
    } catch (JsonProcessingException e) {
      throw new BadRequestException(
          "Field 'semantic_model' is not valid JSON: %s", e.getOriginalMessage());
    }
  }

  /**
   * Resolves and validates every {@code dataset.source} in the parsed Ossie document against the
   * current catalog. Unlike engine-specific view SQL, {@code dataset.source} is a structured
   * catalog identifier, so validating it here prevents every client from persisting dangling
   * references. Every dataset must define a string {@code source}; a missing or non-string source,
   * or one that does not resolve to a {@code TABLE_LIKE} entity, fails with 400 and a JSON-Pointer
   * to the offending dataset.
   */
  private void resolveAndValidateSources(JsonNode semanticModel) {
    if (!semanticModel.isArray()) {
      throw new BadRequestException("Field 'semantic_model' must be a JSON array");
    }

    for (int modelIdx = 0; modelIdx < semanticModel.size(); modelIdx++) {
      JsonNode datasets = semanticModel.get(modelIdx).get("datasets");
      if (datasets == null || !datasets.isArray()) {
        continue;
      }
      for (int datasetIdx = 0; datasetIdx < datasets.size(); datasetIdx++) {
        String pointer =
            String.format("/semantic_model/%d/datasets/%d/source", modelIdx, datasetIdx);
        JsonNode source = datasets.get(datasetIdx).get("source");
        if (source == null || !source.isTextual()) {
          throw new BadRequestException(
              "Semantic model dataset at %s must define a string 'source'", pointer);
        }
        resolveSourceOrThrow(source.asText(), pointer);
      }
    }
  }

  private void resolveSourceOrThrow(String source, String pointer) {
    TableIdentifier tableIdentifier = parseSource(source, pointer);
    // Sources are parsed from the opaque document, so they cannot be pre-registered on this
    // request's resolution manifest. Resolve each one with a fresh single-use manifest that
    // registers the table path as an optional passthrough.
    PolarisResolutionManifest manifest =
        resolutionManifestFactory.createResolutionManifest(principal, catalogEntity.getName());
    manifest.addPassthroughPath(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(tableIdentifier),
            PolarisEntityType.TABLE_LIKE,
            true /* optional */));
    PolarisResolvedPathWrapper resolved =
        manifest.getPassthroughResolvedPath(
            ResolvedPathKey.ofTableLike(tableIdentifier), PolarisEntitySubType.ANY_SUBTYPE);
    if (resolved == null
        || resolved.getRawLeafEntity() == null
        || resolved.getRawLeafEntity().getType() != PolarisEntityType.TABLE_LIKE) {
      throw new BadRequestException(
          "Semantic model source '%s' at %s does not resolve to a table or view in catalog '%s'",
          source, pointer, catalogEntity.getName());
    }
  }

  private TableIdentifier parseSource(String source, String pointer) {
    List<String> parts = Splitter.on(SOURCE_SEPARATOR).splitToList(source);
    if (parts.size() < 2 || parts.stream().anyMatch(String::isEmpty)) {
      throw new BadRequestException(
          "Semantic model source '%s' at %s must be a namespace-qualified identifier "
              + "'<namespace-path>.<name>'",
          source, pointer);
    }
    String name = parts.get(parts.size() - 1);
    String[] levels = parts.subList(0, parts.size() - 1).toArray(new String[0]);
    return TableIdentifier.of(Namespace.of(levels), name);
  }

  private static Namespace toNamespace(SemanticModelIdentifier identifier) {
    return Namespace.of(identifier.getNamespace().toArray(new String[0]));
  }

  private static LoadSemanticModelResponse toLoadResponse(SemanticModelEntity entity) {
    SemanticModelDocument document =
        SemanticModelDocument.builder()
            .setVersion(entity.getSpecVersion())
            .setSemanticModel(entity.getContent())
            .build();
    return LoadSemanticModelResponse.builder()
        .setDocument(document)
        .setEntityVersion(Integer.toString(entity.getEntityVersion()))
        .build();
  }
}
