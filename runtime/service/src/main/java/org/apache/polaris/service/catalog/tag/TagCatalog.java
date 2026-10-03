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

import java.time.Instant;
import java.util.HashMap;
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
import org.apache.polaris.service.idempotency.EntityIdempotency;
import org.apache.polaris.service.idempotency.IdempotencyRequestContext;
import org.apache.polaris.service.types.Tag;
import org.apache.polaris.service.types.TagIdentifier;
import org.apache.polaris.service.types.TargetType;
import org.apache.polaris.service.types.UpdateTagRequest;
import org.jspecify.annotations.Nullable;
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
  private final IdempotencyRequestContext idempotency;

  public TagCatalog(
      PolarisMetaStoreManager metaStoreManager,
      CallContext callContext,
      PolarisResolutionManifestCatalogView resolvedEntityView,
      IdempotencyRequestContext idempotency) {
    this.callContext = callContext;
    this.resolvedEntityView = resolvedEntityView;
    this.catalogEntity = resolvedEntityView.getResolvedCatalogEntity();
    this.catalogId = catalogEntity.getId();
    this.metaStoreManager = metaStoreManager;
    this.idempotency = idempotency;
  }

  /**
   * The internal properties to persist with a write, carrying the request's idempotency key when
   * the shared mechanism is active. The key is recorded by the same write as the change it belongs
   * to, because two metastore calls are two transactions here, and a key recorded on its own would
   * promise a change that may never have landed. A window that is already full refuses here, before
   * the write, so a rejection cannot leave a half-applied change behind.
   */
  private Map<String, String> internalPropertiesWithKey(TagEntity entity) {
    Map<String, String> internalProperties = new HashMap<>(entity.getInternalPropertiesAsMap());
    if (!idempotency.isActive()) {
      return internalProperties;
    }
    return EntityIdempotency.recordKey(
        internalProperties, idempotency.pendingKey(), idempotency.pendingExpiry(), Instant.now());
  }

  /** Whether this request's key is already recorded on the given definition. */
  private boolean hasLiveKey(TagEntity entity) {
    return idempotency.isActive()
        && EntityIdempotency.hasLiveKey(
            entity.getInternalPropertiesAsMap(), idempotency.pendingKey(), Instant.now());
  }

  /** Reads the definition that currently holds a name, or null. Always a fresh metastore read. */
  private @Nullable TagEntity passthroughRead(String tagName) {
    PolarisResolvedPathWrapper resolved =
        resolvedEntityView.getPassthroughResolvedPath(
            ResolvedPathKey.of(List.of(tagName), PolarisEntityType.TAG),
            PolarisEntitySubType.NULL_SUBTYPE);
    return TagEntity.of(resolved == null ? null : resolved.getRawLeafEntity());
  }

  /** Reads a definition by its entity id within this catalog, or null. */
  private @Nullable TagEntity readById(long definitionId) {
    EntityResult result =
        metaStoreManager.loadEntity(
            callContext.getPolarisCallContext(), catalogId, definitionId, PolarisEntityType.TAG);
    return result.isSuccess() ? TagEntity.of(result.getEntity()) : null;
  }

  public Tag createTag(
      String tagName, String description, List<String> values, List<TargetType> targetTypes) {
    PolarisResolvedPathWrapper resolvedTagEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            ResolvedPathKey.of(List.of(tagName), PolarisEntityType.TAG),
            PolarisEntitySubType.NULL_SUBTYPE);

    TagEntity existing =
        TagEntity.of(resolvedTagEntities == null ? null : resolvedTagEntities.getRawLeafEntity());
    if (existing != null) {
      // The name is taken. If this request already created it and lost the response, answer with
      // the definition as it stands now rather than a collision; the retry is the same logical
      // create, and reporting a conflict would make a completed create look like a failed one.
      // Anything else is an ordinary collision, whoever owns the name.
      if (hasLiveKey(existing)) {
        return constructTag(existing);
      }
      throw new AlreadyExistsException("Tag already exists: %s", tagName);
    }

    List<String> valueList = values;
    // Map to the wire vocabulary null-safely, then validate BEFORE anything consumes the lists:
    // a null member must surface as a 400, not a mapping NPE, and TagEntity.Builder's
    // Preconditions would surface a null list as a raw 500. The entity stores the wire strings
    // (for example "TABLE"), so the stored form and the API vocabulary stay identical.
    List<String> targetTypeNames =
        targetTypes == null
            ? null
            : targetTypes.stream()
                .map(targetType -> targetType == null ? null : targetType.toString())
                .collect(Collectors.toList());
    TagValidation.validateValues(valueList);
    TagValidation.validateTargetTypes(targetTypeNames);

    TagEntity entity =
        new TagEntity.Builder(tagName)
            .setCatalogId(catalogId)
            .setParentId(catalogId)
            .setDescription(normalizeDescription(description))
            .setValues(valueList)
            .setTargetTypes(targetTypeNames)
            .setId(
                metaStoreManager.generateNewEntityId(callContext.getPolarisCallContext()).getId())
            .setCreateTimestamp(System.currentTimeMillis())
            .build();
    entity =
        new TagEntity.Builder(entity)
            .setInternalProperties(internalPropertiesWithKey(entity))
            .build();

    EntityResult res =
        metaStoreManager.createEntityIfNotExists(
            callContext.getPolarisCallContext(),
            PolarisEntity.toCoreList(List.of(PolarisEntity.of(catalogEntity))),
            entity);

    if (!res.isSuccess()) {
      switch (res.getReturnStatus()) {
        case ENTITY_ALREADY_EXISTS:
          // Lost a race with a concurrent request. If that was this same request arriving twice,
          // the winner recorded the key with the definition it created, so a fresh read settles it.
          TagEntity winner = passthroughRead(tagName);
          if (winner != null && hasLiveKey(winner)) {
            return constructTag(winner);
          }
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
                TagIdentifier.builder()
                    .setId(Long.toString(record.getId()))
                    .setName(record.getName())
                    .build());
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

    // A request whose key is already recorded on this definition has run before, so answer with the
    // definition as it stands now. This precedes the version comparison deliberately: the retry
    // still carries the token from before the first attempt, so comparing versions first would
    // answer 409 to a request that already succeeded. It does not precede authorization, which the
    // handler has already applied to this same operation.
    if (hasLiveKey(tag)) {
      return constructTag(tag);
    }

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

    // Update replaces the whole editable definition: description and values are both present in a
    // valid request, and an omitted description never reaches here, because
    // Serializers.UpdateTagRequestDeserializer answers that first. Name and target-types are not
    // editable: renaming is its own operation and target-types are create-only.
    String newDescription = normalizeDescription(request.getDescription());
    List<String> newValues = request.getValues();
    // Validate the raw wire list first: List.copyOf would turn a null member into a raw 500.
    TagValidation.validateValues(newValues);
    newValues = List.copyOf(newValues);

    if (Objects.equals(newDescription, tag.getDescription())
        && Objects.equals(newValues, tag.getValues())) {
      // A request that asks for the state the definition already has: the token was checked, so the
      // caller is not overwriting someone else's write, and there is nothing to write. The version
      // does not advance, and no key is recorded, because no change happened for a retry to stand
      // for. A later retry of this same request can therefore see a newer version and answer 409,
      // which is the honest answer once the definition has moved on.
      return constructTag(tag);
    }

    TagEntity newTagEntity =
        new TagEntity.Builder(tag).setDescription(newDescription).setValues(newValues).build();
    newTagEntity =
        new TagEntity.Builder(newTagEntity)
            .setInternalProperties(internalPropertiesWithKey(newTagEntity))
            .build();

    List<PolarisEntity> catalogPath = resolvedTagPath.getRawParentPath();
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
      // The compare-and-swap lost against another writer. If the winner was this same request
      // arriving twice, it committed the key together with its change, so one fresh read by id
      // tells the two cases apart before answering a conflict.
      TagEntity fresh = readById(tag.getId());
      if (fresh != null && hasLiveKey(fresh)) {
        return constructTag(fresh);
      }
      throw new CommitConflictException(
          "Concurrent modification on tag '%s'; retry later", tagName);
    }

    return constructTag(updatedEntity);
  }

  /**
   * Whether this request already renamed the definition with the given id, which now holds {@code
   * currentName}. The read is fresh, and the id is re-checked against the definition the name
   * resolves to, so a rename that moved names around between the two reads falls back to the normal
   * path instead of answering for a different definition.
   */
  public boolean isRecognizedRename(String currentName, long definitionId) {
    TagEntity entity = passthroughRead(currentName);
    return entity != null && entity.getId() == definitionId && hasLiveKey(entity);
  }

  /**
   * Renames one definition. The definition keeps its id, its values and its target types; only the
   * name and the version token change.
   *
   * <p>The token names the definition as well as its revision: it carries the entity id and the
   * entity version, and both have to match. So a token taken from a definition since deleted and
   * recreated under this name, or from the definition this name was renamed away from, is refused
   * as a version mismatch even when the revision numbers happen to coincide. The write below
   * conditions on the same entity version through the metastore compare-and-swap, so the check and
   * the change are one step: a writer that commits in between moves the version, the write matches
   * no row, and this reports a conflict instead of renaming over it.
   */
  public void renameTag(String source, String destination, String currentTagVersion) {
    var resolvedTagPath = getResolvedPathWrapper(source);
    var tag = TagEntity.of(resolvedTagPath.getRawLeafEntity());

    if (currentTagVersion == null || currentTagVersion.isEmpty()) {
      throw new BadRequestException("current-tag-version is required");
    }
    if (!TagVersionToken.decode(currentTagVersion).describes(tag.getId(), tag.getEntityVersion())) {
      throw new TagVersionMismatchException(
          String.format(
              "The supplied current-tag-version does not match the current version of tag '%s'",
              source));
    }

    TagEntity renamed = new TagEntity.Builder(tag).setName(destination).build();
    renamed =
        new TagEntity.Builder(renamed)
            .setInternalProperties(internalPropertiesWithKey(renamed))
            .build();

    List<PolarisEntity> catalogPath = resolvedTagPath.getRawParentPath();
    EntityResult renameResult =
        metaStoreManager.renameEntity(
            callContext.getPolarisCallContext(),
            PolarisEntity.toCoreList(catalogPath),
            tag,
            PolarisEntity.toCoreList(catalogPath),
            renamed);
    if (renameResult.isSuccess()) {
      return;
    }

    // Either answer below can also be this same request arriving twice, so a fresh read by id
    // decides before a conflict is reported: the winner committed the key with the rename.
    TagEntity fresh = readById(tag.getId());
    if (fresh != null && hasLiveKey(fresh)) {
      return;
    }
    switch (renameResult.getReturnStatus()) {
      case ENTITY_ALREADY_EXISTS:
        throw new AlreadyExistsException("Tag already exists: %s", destination);
      case TARGET_ENTITY_CONCURRENTLY_MODIFIED:
      // The definition resolved and its version matched, so a source that can no longer be found
      // means another request changed or removed it after validation: the contract defines that as
      // a retryable conflict, not a server error.
      case ENTITY_NOT_FOUND:
      case ENTITY_CANNOT_BE_RESOLVED:
      case CATALOG_PATH_CANNOT_BE_RESOLVED:
        throw new CommitConflictException(
            "Concurrent modification on tag '%s'; retry later", source);
      default:
        throw new IllegalStateException(
            String.format(
                "Failed to rename tag %s error status: %s with extraInfo: %s",
                source, renameResult.getReturnStatus(), renameResult.getExtraInformation()));
    }
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
        .setId(Long.toString(tagEntity.getId()))
        .setName(tagEntity.getName())
        .setDescription(tagEntity.getDescription())
        .setValues(tagEntity.getValues())
        .setTargetTypes(
            tagEntity.getTargetTypes().stream()
                .map(TagCatalog::targetTypeFromStored)
                .collect(Collectors.toCollection(LinkedHashSet::new)))
        .setVersion(TagVersionToken.encode(tagEntity.getId(), tagEntity.getEntityVersion()))
        .build();
  }

  /**
   * The one stored form of "no description" is null.
   *
   * <p>A client can say it two ways, an explicit null or an empty string, and the request layer
   * folds the null into the empty string so the generated model's required-field check passes.
   * Collapsing the two here means the entity holds one representation instead of two, so a
   * definition that was never described and one whose description was cleared are the same state,
   * and the no-op check above compares them as equal rather than treating a clear as a change.
   */
  private static String normalizeDescription(String description) {
    return "".equals(description) ? null : description;
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
