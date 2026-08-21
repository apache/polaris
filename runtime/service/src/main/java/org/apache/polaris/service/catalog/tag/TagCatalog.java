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
package org.apache.polaris.service.catalog.tag;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.exceptions.CommitConflictException;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.tag.TagEntity;
import org.apache.polaris.core.tag.TagValidation;
import org.apache.polaris.core.tag.TagVersionToken;
import org.apache.polaris.core.tag.exceptions.NoSuchTagException;
import org.apache.polaris.core.tag.exceptions.TagVersionMismatchException;
import org.apache.polaris.service.types.Tag;
import org.apache.polaris.service.types.TagIdentifier;
import org.apache.polaris.service.types.TargetType;
import org.apache.polaris.service.types.UpdateTagRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Business logic for tag definitions. Tags are children of the catalog, so every operation here
 * resolves the tag by name directly under the resolved reference catalog.
 */
public class TagCatalog {
  private static final Logger LOGGER = LoggerFactory.getLogger(TagCatalog.class);

  private final CallContext callContext;
  private final PolarisResolutionManifestCatalogView resolvedEntityView;
  private final CatalogEntity catalogEntity;
  private final long catalogId;
  private final PolarisMetaStoreManager metaStoreManager;

  public TagCatalog(
      PolarisMetaStoreManager metaStoreManager,
      CallContext callContext,
      PolarisResolutionManifestCatalogView resolvedEntityView) {
    this.callContext = callContext;
    this.resolvedEntityView = resolvedEntityView;
    this.catalogEntity = resolvedEntityView.getResolvedCatalogEntity();
    this.catalogId = catalogEntity.getId();
    this.metaStoreManager = metaStoreManager;
  }

  public Tag createTag(
      String tagName, String comment, List<String> allowedValues, List<TargetType> targetTypes) {
    PolarisResolvedPathWrapper resolvedTagEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            ResolvedPathKey.of(List.of(tagName), PolarisEntityType.TAG),
            PolarisEntitySubType.NULL_SUBTYPE);

    TagEntity existing =
        TagEntity.of(resolvedTagEntities == null ? null : resolvedTagEntities.getRawLeafEntity());
    if (existing != null) {
      throw new AlreadyExistsException("Tag already exists: %s", tagName);
    }

    List<String> allowedValueList = allowedValues;
    // Map to the wire vocabulary null-safely, then validate BEFORE anything consumes the lists:
    // a null member must surface as a 400, not a mapping NPE, and TagEntity.Builder's
    // Preconditions would surface a null list as a raw 500. The entity stores the wire strings
    // (for example "table-like"), so the stored form and the API vocabulary stay identical.
    List<String> targetTypeNames =
        targetTypes == null
            ? null
            : targetTypes.stream()
                .map(targetType -> targetType == null ? null : targetType.toString())
                .collect(Collectors.toList());
    TagValidation.validateAllowedValues(allowedValueList);
    TagValidation.validateTargetTypes(targetTypeNames);

    TagEntity entity =
        new TagEntity.Builder(tagName)
            .setCatalogId(catalogId)
            .setParentId(catalogId)
            .setComment(comment)
            .setAllowedValues(allowedValueList)
            .setTargetTypes(targetTypeNames)
            .setId(
                metaStoreManager.generateNewEntityId(callContext.getPolarisCallContext()).getId())
            .setCreateTimestamp(System.currentTimeMillis())
            .build();

    EntityResult res =
        metaStoreManager.createEntityIfNotExists(
            callContext.getPolarisCallContext(),
            PolarisEntity.toCoreList(List.of(PolarisEntity.of(catalogEntity))),
            entity);

    if (!res.isSuccess()) {
      switch (res.getReturnStatus()) {
        case ENTITY_ALREADY_EXISTS:
          throw new AlreadyExistsException("Tag already exists: %s", tagName);
        default:
          throw new IllegalStateException(
              String.format(
                  "Unknown error status for tag %s: %s with extraInfo: %s",
                  tagName, res.getReturnStatus(), res.getExtraInformation()));
      }
    }

    TagEntity resultEntity = TagEntity.of(res.getEntity());
    LOGGER.debug("Created tag entity {}", resultEntity);
    return constructTag(resultEntity);
  }

  public Page<TagIdentifier> listTags(PageToken pageToken) {
    ListEntitiesResult listEntitiesResult =
        metaStoreManager.listEntities(
            callContext.getPolarisCallContext(),
            PolarisEntity.toCoreList(List.of(PolarisEntity.of(catalogEntity))),
            PolarisEntityType.TAG,
            PolarisEntitySubType.NULL_SUBTYPE,
            pageToken);
    if (!listEntitiesResult.isSuccess()) {
      throw new IllegalStateException("Failed to list tags in the catalog");
    }
    return listEntitiesResult
        .getPage()
        .map(
            (EntityNameLookupRecord record) ->
                TagIdentifier.builder().setName(record.getName()).build());
  }

  public Tag loadTag(String tagName) {
    return constructTag(TagEntity.of(getResolvedPathWrapper(tagName).getRawLeafEntity()));
  }

  /**
   * Applies an update to a tag definition, rejecting one that is based on a stale read.
   *
   * <p>The token check and the change are atomic through the entity compare-and-swap, not through
   * their order here. The token carries the entity version this request read, and the write below
   * conditions on that same version: {@code updateEntityPropertiesIfNotChanged} for a field change
   * and {@code renameEntity} for a rename both refuse when the stored version has moved. A writer
   * that commits between the check and the write increments the version, the conditional write then
   * matches no row, and this reports a retryable conflict rather than overwriting that writer.
   *
   * <p>A request that would change nothing still has to present the current token, and returns that
   * same token without writing.
   */
  public Tag updateTag(String tagName, UpdateTagRequest request) {
    var resolvedTagPath = getResolvedPathWrapper(tagName);
    var tag = TagEntity.of(resolvedTagPath.getRawLeafEntity());

    // The request schema requires a non-empty current-tag-version, so a request that reaches the
    // handler over the wire has already been rejected with the shared validation response if the
    // token was missing, null or empty. This guard covers a direct in-process caller.
    if (request.getCurrentTagVersion() == null || request.getCurrentTagVersion().isEmpty()) {
      throw new BadRequestException("current-tag-version is required");
    }
    if (!TagVersionToken.decode(request.getCurrentTagVersion())
        .describes(tag.getId(), tag.getEntityVersion())) {
      throw new TagVersionMismatchException(
          String.format(
              "The supplied current-tag-version does not match the current version of tag '%s'",
              tagName));
    }

    // Null-tolerant update semantics: an omitted mutable field leaves the field unchanged, and
    // for the two nullable lists, allowed-values and target-types, an explicit JSON null does the
    // same; a present, non-null allowed-values list replaces the complete list. target-types is
    // create-only: any non-null value, including an empty list, is rejected.
    if (request.getTargetTypes() != null) {
      throw new BadRequestException("target-types is create-only and cannot be updated");
    }

    String newName = request.getName();
    boolean isRename = newName != null && !newName.equals(tag.getName());
    String newComment = request.getComment();
    List<String> newAllowedValues = request.getAllowedValues();
    if (newAllowedValues != null) {
      // Validate the raw wire list first: List.copyOf would turn a null member into a raw 500.
      TagValidation.validateAllowedValues(newAllowedValues);
      newAllowedValues = List.copyOf(newAllowedValues);
    }

    boolean commentUnchanged = newComment == null || Objects.equals(newComment, tag.getComment());
    boolean allowedValuesUnchanged =
        newAllowedValues == null || Objects.equals(newAllowedValues, tag.getAllowedValues());
    if (!isRename && commentUnchanged && allowedValuesUnchanged) {
      // Deliberate: a no-op update writes nothing and returns the tag with its current token.
      return constructTag(tag);
    }

    TagEntity.Builder newTagBuilder = new TagEntity.Builder(tag);
    if (isRename) {
      newTagBuilder.setName(newName);
    }
    newTagBuilder.setComment(newComment);
    if (newAllowedValues != null) {
      newTagBuilder.setAllowedValues(newAllowedValues);
    }
    TagEntity newTagEntity = newTagBuilder.build();

    List<PolarisEntity> catalogPath = resolvedTagPath.getRawParentPath();
    if (isRename) {
      EntityResult renameResult =
          metaStoreManager.renameEntity(
              callContext.getPolarisCallContext(),
              PolarisEntity.toCoreList(catalogPath),
              tag,
              PolarisEntity.toCoreList(catalogPath),
              newTagEntity);
      if (!renameResult.isSuccess()) {
        switch (renameResult.getReturnStatus()) {
          case ENTITY_ALREADY_EXISTS:
            throw new AlreadyExistsException("Tag already exists: %s", newName);
          case TARGET_ENTITY_CONCURRENTLY_MODIFIED:
          // The tag resolved and its version matched, so a source that can no longer be found or
          // resolved means another request modified or removed it after validation: the contract
          // defines that as a retryable conflict, not a server error.
          case ENTITY_NOT_FOUND:
          case ENTITY_CANNOT_BE_RESOLVED:
          case CATALOG_PATH_CANNOT_BE_RESOLVED:
            throw new CommitConflictException(
                "Concurrent modification on tag '%s'; retry later", tagName);
          default:
            throw new IllegalStateException(
                String.format(
                    "Failed to rename tag %s error status: %s with extraInfo: %s",
                    tagName, renameResult.getReturnStatus(), renameResult.getExtraInformation()));
        }
      }
      return constructTag(TagEntity.of(renameResult.getEntity()));
    }

    TagEntity updatedEntity =
        Optional.ofNullable(
                metaStoreManager
                    .updateEntityPropertiesIfNotChanged(
                        callContext.getPolarisCallContext(),
                        PolarisEntity.toCoreList(catalogPath),
                        newTagEntity)
                    .getEntity())
            .map(TagEntity::of)
            .orElse(null);

    if (updatedEntity == null) {
      // The compare-and-swap lost against another writer: a retryable 409, matching Policy.
      throw new CommitConflictException(
          "Concurrent modification on tag '%s'; retry later", tagName);
    }

    return constructTag(updatedEntity);
  }

  public boolean dropTag(String tagName) {
    var resolvedTagPath = getResolvedPathWrapper(tagName);
    var catalogPath = resolvedTagPath.getRawParentPath();
    var tagEntity = resolvedTagPath.getRawLeafEntity();

    var result =
        metaStoreManager.dropEntityIfExists(
            callContext.getPolarisCallContext(),
            PolarisEntity.toCoreList(catalogPath),
            tagEntity,
            Map.of(),
            false);

    if (!result.isSuccess()) {
      switch (result.getReturnStatus()) {
        // A concurrent request can drop the tag (or its catalog path) between this request's
        // resolution and the delete; the contract defines a missing definition as 404.
        case ENTITY_NOT_FOUND:
        case CATALOG_PATH_CANNOT_BE_RESOLVED:
          throw new NoSuchTagException(String.format("Tag does not exist: %s", tagName));
        default:
          throw new IllegalStateException(
              String.format(
                  "Failed to drop tag %s error status: %s with extraInfo: %s",
                  tagName, result.getReturnStatus(), result.getExtraInformation()));
      }
    }
    return true;
  }

  private PolarisResolvedPathWrapper getResolvedPathWrapper(String tagName) {
    var resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            ResolvedPathKey.of(List.of(tagName), PolarisEntityType.TAG),
            PolarisEntitySubType.NULL_SUBTYPE);
    if (resolvedEntities == null || resolvedEntities.getResolvedLeafEntity() == null) {
      throw new NoSuchTagException(String.format("Tag does not exist: %s", tagName));
    }
    return resolvedEntities;
  }

  private static Tag constructTag(TagEntity tagEntity) {
    return Tag.builder()
        .setName(tagEntity.getName())
        .setComment(tagEntity.getComment())
        .setAllowedValues(tagEntity.getAllowedValues())
        .setTargetTypes(
            tagEntity.getTargetTypes().stream()
                .map(TagCatalog::targetTypeFromStored)
                .collect(Collectors.toCollection(LinkedHashSet::new)))
        .setVersion(TagVersionToken.encode(tagEntity.getId(), tagEntity.getEntityVersion()))
        .build();
  }

  /**
   * Resolves a stored target-types member back to the enum by its wire value. A value that no
   * longer resolves is corrupt stored data, a server-side condition, so this throws
   * IllegalStateException rather than an exception the mappers would blame on the client.
   */
  static TargetType targetTypeFromStored(String stored) {
    for (TargetType targetType : TargetType.values()) {
      if (targetType.toString().equals(stored)) {
        return targetType;
      }
    }
    throw new IllegalStateException(
        String.format("Invalid stored target-types member: %s", stored));
  }
}
