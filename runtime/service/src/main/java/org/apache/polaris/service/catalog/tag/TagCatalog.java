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

import com.google.common.annotations.VisibleForTesting;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.table.GenericTableEntity;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.entity.table.TableLikeEntity;
import org.apache.polaris.core.exceptions.CommitConflictException;
import org.apache.polaris.core.exceptions.PolarisServiceUnavailableException;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.LoadTagAssignmentTargetsResult;
import org.apache.polaris.core.persistence.pagination.EntityIdToken;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.tag.CandidateBudget;
import org.apache.polaris.core.tag.ClassifiedAssignment;
import org.apache.polaris.core.tag.PolarisTagAssignmentManager.TargetLevel;
import org.apache.polaris.core.tag.TagAssignmentRecord;
import org.apache.polaris.core.tag.TagAssignmentTargetToken;
import org.apache.polaris.core.tag.TagEntity;
import org.apache.polaris.core.tag.TagValidation;
import org.apache.polaris.core.tag.TagVersionToken;
import org.apache.polaris.core.tag.exceptions.CandidateBudgetExceededException;
import org.apache.polaris.core.tag.exceptions.NoSuchAssignmentException;
import org.apache.polaris.core.tag.exceptions.NoSuchTagException;
import org.apache.polaris.core.tag.exceptions.NoSuchTargetException;
import org.apache.polaris.core.tag.exceptions.TagInUseException;
import org.apache.polaris.core.tag.exceptions.TagVersionMismatchException;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.idempotency.EntityIdempotency;
import org.apache.polaris.service.idempotency.IdempotencyRequestContext;
import org.apache.polaris.service.types.ObjectTag;
import org.apache.polaris.service.types.Tag;
import org.apache.polaris.service.types.TagAttachmentTarget;
import org.apache.polaris.service.types.TagIdentifier;
import org.apache.polaris.service.types.TaggedObject;
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
  private final StorageAccessConfigProvider storageAccessConfigProvider;
  private final FileIOFactory fileIOFactory;
  private final RealmConfig realmConfig;

  public TagCatalog(
      PolarisMetaStoreManager metaStoreManager,
      CallContext callContext,
      PolarisResolutionManifestCatalogView resolvedEntityView,
      IdempotencyRequestContext idempotency,
      StorageAccessConfigProvider storageAccessConfigProvider,
      FileIOFactory fileIOFactory,
      RealmConfig realmConfig) {
    this.callContext = callContext;
    this.resolvedEntityView = resolvedEntityView;
    this.catalogEntity = resolvedEntityView.getResolvedCatalogEntity();
    this.catalogId = catalogEntity.getId();
    this.metaStoreManager = metaStoreManager;
    this.idempotency = idempotency;
    this.storageAccessConfigProvider = storageAccessConfigProvider;
    this.fileIOFactory = fileIOFactory;
    this.realmConfig = realmConfig;
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

  /**
   * How many times a plain drop classifies the definition's assignment rows before it refuses. Each
   * attempt is lost only to an assignment write that committed while this request was deciding, so
   * a handful of attempts settles any real race; an attempt that keeps losing has to answer rather
   * than keep a caller waiting.
   */
  private static final int DROP_CLASSIFY_ATTEMPTS = 3;

  public boolean dropTag(String tagName, boolean detachAll) {
    var resolvedTagPath = getResolvedPathWrapper(tagName);
    var catalogPath = resolvedTagPath.getRawParentPath();
    var tagEntity = resolvedTagPath.getRawLeafEntity();

    // Deciding and deleting cannot be one step: judging a column row needs the table's current
    // schema, which is readable here and not below this layer. So the decision is taken here and
    // the delete is held to exactly the rows it was taken on. A row that appeared in between was
    // never judged and may be live, so that delete changes nothing and says so, and this loop
    // decides again on fresh state rather than remove a row no one judged.
    for (int attempt = 1; ; attempt++) {
      // Without detach-all, only an assignment whose target is still live blocks the delete. A row
      // whose target or column is gone is inert: nothing can read it and unassign cannot remove it,
      // so refusing the delete over it would leave the definition undeletable forever.
      Set<ClassifiedAssignment> inertAssignments = Set.of();
      if (!detachAll) {
        var remaining = classifyRemainingAssignments(tagName, tagEntity);
        if (remaining.hasLiveAssignment()) {
          throw new TagInUseException(
              "Tag %s is in use: assignments exist; retry with detach-all=true to remove them",
              tagName);
        }
        inertAssignments = remaining.inertAssignments();
      }

      DropEntityResult result;
      if (detachAll) {
        // detach-all removes every assignment whatever it names, so it carries no judgement and
        // checks none.
        result =
            metaStoreManager.dropEntityIfExists(
                callContext.getPolarisCallContext(),
                PolarisEntity.toCoreList(catalogPath),
                tagEntity,
                Map.of(),
                true);
      } else {
        // Every other drop goes through the delete that is held to what this attempt judged, the
        // empty set included. An empty set means "this definition must hold no assignment row at
        // all", which is the same question the plain delete asks, but asked inside one transaction
        // and under the definition-row lock. Asking it outside one is how a row written between the
        // question and the delete used to be deleted unseen.
        result =
            metaStoreManager.dropTagAndClassifiedAssignmentsIfExists(
                callContext.getPolarisCallContext(),
                PolarisEntity.toCoreList(catalogPath),
                tagEntity,
                inertAssignments);
        if (inertAssignments.isEmpty()
            && result.getReturnStatus() == BaseResult.ReturnStatus.TAG_ASSIGNMENTS_NOT_SUPPORTED) {
          // A backend that cannot remove a definition together with assignments cannot be holding
          // any either, so there is nothing for the guarded delete to protect and the plain delete
          // it does support is the right answer. Only reachable with an empty set: a non-empty one
          // is proof the backend holds rows.
          result =
              metaStoreManager.dropEntityIfExists(
                  callContext.getPolarisCallContext(),
                  PolarisEntity.toCoreList(catalogPath),
                  tagEntity,
                  Map.of(),
                  false);
        }
      }

      if (!result.isSuccess()) {
        if (result.getReturnStatus() == BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED
            && attempt < DROP_CLASSIFY_ATTEMPTS) {
          continue;
        }
        switch (result.getReturnStatus()) {
          // A concurrent request can drop the tag (or its catalog path) between this request's
          // resolution and the delete; the contract defines a missing definition as 404.
          case ENTITY_NOT_FOUND:
          case CATALOG_PATH_CANNOT_BE_RESOLVED:
            throw new NoSuchTagException(String.format("Tag does not exist: %s", tagName));
          case TAG_HAS_ASSIGNMENTS:
            // No drop from here reports this any more: a plain drop asks the guarded delete, which
            // answers a row it did not judge as a concurrent modification instead. The label stays
            // because the status is still part of the drop contract, and losing it would turn a
            // backend that does report it into a server error.
            throw new TagInUseException(
                "Tag %s is in use: assignments exist; retry with detach-all=true to remove them",
                tagName);
          case TARGET_ENTITY_CONCURRENTLY_MODIFIED:
            // Every attempt was decided against assignment rows that had already moved on. No row
            // was proved live, so this does not say one exists: it says the definition's
            // assignments
            // would not hold still long enough to judge them. The type is the one this operation
            // declares for a drop it will not perform.
            throw new TagInUseException(
                "Tag %s cannot be dropped: assignments on this definition changed concurrently"
                    + " during every attempt; retry the drop",
                tagName);
          case TAG_ASSIGNMENTS_NOT_SUPPORTED:
            if (detachAll) {
              // A backend without assignment support cannot remove the assignments this request
              // asks for, so it answers 501 and changes nothing; permissions were already checked.
              throw new WebApplicationException(
                  String.format(
                      "This implementation cannot guarantee the detach-all=true result; no visible"
                          + " change was made: %s",
                      result.getExtraInformation()),
                  Response.Status.NOT_IMPLEMENTED);
            }
            throw tagAssignmentsUnsupported("drop", tagName, result);
          default:
            throw new IllegalStateException(
                String.format(
                    "Failed to drop tag %s error status: %s with extraInfo: %s",
                    tagName, result.getReturnStatus(), result.getExtraInformation()));
        }
      }
      return true;
    }
  }

  /**
   * What the surviving assignment rows of a definition say about whether it can be deleted. When a
   * live row is found the others no longer matter, so {@code inertAssignments} is only complete
   * while {@code hasLiveAssignment} is false; that is the only case in which a delete follows.
   */
  private record RemainingAssignments(
      boolean hasLiveAssignment, Set<ClassifiedAssignment> inertAssignments) {}

  /**
   * Reads every assignment row of the definition and reports whether any of them is still live. A
   * row is live while its target resolves and, for a column row, while its field id still names a
   * top-level column of that table's current schema.
   */
  private RemainingAssignments classifyRemainingAssignments(
      String tagName, PolarisBaseEntity tagEntity) {
    var result =
        metaStoreManager.loadAllTargetsOnTagWithEntities(
            callContext.getPolarisCallContext(), tagEntity);
    if (!result.isSuccess()) {
      switch (result.getReturnStatus()) {
        case ENTITY_NOT_FOUND:
          throw new NoSuchTagException(String.format("Tag does not exist: %s", tagName));
        case TAG_ASSIGNMENTS_NOT_SUPPORTED:
          // A backend that cannot store an assignment cannot be holding one, so the answer is
          // definitively "none" rather than "unknown", and a plain drop keeps working there.
          return new RemainingAssignments(false, Set.of());
        default:
          throw new IllegalStateException(
              String.format(
                  "Failed to read the assignments of tag %s error status: %s with extraInfo: %s",
                  tagName, result.getReturnStatus(), result.getExtraInformation()));
      }
    }

    var records = result.getAssignments();
    if (records.isEmpty()) {
      return new RemainingAssignments(false, Set.of());
    }
    var targetsById = result.getTargetEntitiesAsMap();
    Map<Long, SchemaAt> schemaByTableId = new HashMap<>();
    Set<ClassifiedAssignment> inertAssignments = new LinkedHashSet<>();
    for (var record : records) {
      // Stopping at the first row, the way an existence probe does, cannot answer this question:
      // an inert row says nothing about the rows after it. Stopping at the first LIVE row is a
      // different matter, because one live assignment settles the answer on its own and reading
      // further table metadata could not change it.
      if (isLiveAssignment(
          tagName, record, targetsById.get(record.getTargetId()), schemaByTableId)) {
        return new RemainingAssignments(true, Set.of());
      }
      // The identity, not the record: the value is content, and a concurrent replacement of it
      // leaves the same row, which this judgement still covers. The target's version travels with
      // it, because the judgement rests on the target: a row is inert because of something about
      // the target, and the same row becomes live again if that target changes back. An absent
      // target carries no version, which is itself the state to re-check.
      var target = targetsById.get(record.getTargetId());
      inertAssignments.add(
          ClassifiedAssignment.of(
              record,
              target == null ? OptionalLong.empty() : OptionalLong.of(target.getEntityVersion())));
    }
    return new RemainingAssignments(false, inertAssignments);
  }

  private boolean isLiveAssignment(
      String tagName,
      TagAssignmentRecord record,
      @Nullable PolarisBaseEntity targetEntity,
      Map<Long, SchemaAt> schemaByTableId) {
    if (targetEntity == null || targetEntity.getDropTimestamp() != 0) {
      // The target id no longer resolves, or the target is soft-dropped. Ids are never reused, so a
      // row that has lost its target can never become live again, and a replacement created under
      // the same name is a different entity that does not inherit it. The soft-dropped half cannot
      // occur on any current backend, where dropping an entity removes its row outright; it is
      // written out because the answer is the same either way and a later lifecycle change should
      // not silently turn these rows back into blockers.
      return false;
    }
    if (record.getFieldId() == 0) {
      return true;
    }
    if (targetEntity.getType() != PolarisEntityType.TABLE_LIKE
        || targetEntity.getSubType() != PolarisEntitySubType.ICEBERG_TABLE) {
      // Only an Iceberg table has field ids, so a column row on anything else cannot name a live
      // column of it.
      return false;
    }
    var tableEntity = IcebergTableLikeEntity.of(targetEntity);
    if (tableEntity.getMetadataLocation() == null) {
      // Being unable to read a table's metadata does not establish that its column is gone, so the
      // row must not be judged inert on that basis: doing so would delete a definition that a live
      // assignment may still use. The request fails instead, having changed nothing.
      throw new IllegalStateException(
          String.format(
              "Cannot drop tag %s: table %s has no current metadata location, so the column"
                  + " assignment on it cannot be judged",
              tagName, tableEntity.getTableIdentifier()));
    }
    // This probe reads its rows and their targets from one snapshot, so every row of a table here
    // carries the same pointer and the id alone is a sufficient key; the pointer travels with the
    // cached value all the same, so the two users of this cache cannot drift apart.
    Schema schema =
        schemaByTableId
            .computeIfAbsent(
                targetEntity.getId(),
                id ->
                    new SchemaAt(
                        tableEntity.getMetadataLocation(),
                        TagCatalogUtils.loadCurrentSchema(
                            storageAccessConfigProvider,
                            fileIOFactory,
                            realmConfig,
                            catalogEntity,
                            tableEntity.getTableIdentifier(),
                            tableEntity,
                            resolvedEntityView.getResolvedReferenceCatalogEntity())))
            .schema();
    return TagCatalogUtils.findTopLevelColumnName(schema, record.getFieldId()) != null;
  }

  public void assignTag(String tagName, TagAttachmentTarget target, List<String> values) {
    var resolvedTagPath = getResolvedPathWrapper(tagName);
    var tag = TagEntity.of(resolvedTagPath.getRawLeafEntity());
    var tagCatalogPath = PolarisEntity.toCoreList(resolvedTagPath.getRawParentPath());

    // A selection that names no value is invalid: an explicit null is answered 400 BadRequest, as
    // is an empty list.
    if (values.isEmpty()) {
      throw new BadRequestException("values must not be empty");
    }
    if (values.size() > 1) {
      throw new BadRequestException("multiple selected values are not supported");
    }
    String value = values.get(0);
    if (value == null || value.isEmpty()) {
      throw new BadRequestException("values must not contain a null or empty member");
    }
    TagValidation.validateValueLength(value, "A selected value");

    // Resolve the target (path, subtype, column) before checking whether the definition allows
    // its kind: a target that does not exist answers the target-level 404 even when its kind
    // would have been rejected, and only an existing target of an excluded kind answers 400.
    // target-types is create-only, so checking it after resolution introduces no race; the
    // selected value is re-validated against the definition inside the persistence write.
    var resolvedTarget = resolveAssignmentTarget(target);
    int fieldId = resolveFieldId(target, resolvedTarget);
    if (!tag.getTargetTypes().contains(target.getType().toString())) {
      throw new BadRequestException(
          "Target type %s is not allowed by tag %s", target.getType(), tag.getName());
    }

    var result =
        metaStoreManager.assignTagToEntity(
            callContext.getPolarisCallContext(),
            PolarisEntity.toCoreList(resolvedTarget.getRawParentPath()),
            resolvedTarget.getRawLeafEntity(),
            fieldId,
            tagCatalogPath,
            tag,
            value);
    if (!result.isSuccess()) {
      switch (result.getReturnStatus()) {
        case ENTITY_NOT_FOUND:
          throw new NoSuchTagException(String.format("Tag no longer exists: %s", tagName));
        case ENTITY_CANNOT_BE_RESOLVED:
          throw new NoSuchTargetException("Target no longer exists for tag %s", tagName);
        case TAG_ASSIGNMENTS_NOT_SUPPORTED:
          throw tagAssignmentsUnsupported("assign", "tag " + tagName, result);
        default:
          throw new IllegalStateException(
              String.format(
                  "Failed to assign tag %s error status: %s with extraInfo: %s",
                  tagName, result.getReturnStatus(), result.getExtraInformation()));
      }
    }
  }

  public void unassignTag(String tagName, TagAttachmentTarget target) {
    var resolvedTagPath = getResolvedPathWrapper(tagName);
    var tag = TagEntity.of(resolvedTagPath.getRawLeafEntity());
    var tagCatalogPath = PolarisEntity.toCoreList(resolvedTagPath.getRawParentPath());

    // unassign removes an existing relationship; it does not re-check target-types.
    var resolvedTarget = resolveAssignmentTarget(target);
    int fieldId = resolveFieldId(target, resolvedTarget);

    var result =
        metaStoreManager.unassignTagFromEntity(
            callContext.getPolarisCallContext(),
            PolarisEntity.toCoreList(resolvedTarget.getRawParentPath()),
            resolvedTarget.getRawLeafEntity(),
            fieldId,
            tagCatalogPath,
            tag);
    if (!result.isSuccess()) {
      switch (result.getReturnStatus()) {
        case TAG_ASSIGNMENT_NOT_FOUND:
          throw new NoSuchAssignmentException(
              "Tag assignment does not exist for tag %s on the given target", tagName);
        case ENTITY_NOT_FOUND:
          throw new NoSuchTagException(String.format("Tag no longer exists: %s", tagName));
        case ENTITY_CANNOT_BE_RESOLVED:
          throw new NoSuchTargetException("Target no longer exists for tag %s", tagName);
        case TAG_ASSIGNMENTS_NOT_SUPPORTED:
          throw tagAssignmentsUnsupported("unassign", "tag " + tagName, result);
        default:
          throw new IllegalStateException(
              String.format(
                  "Failed to unassign tag %s error status: %s with extraInfo: %s",
                  tagName, result.getReturnStatus(), result.getExtraInformation()));
      }
    }
  }

  /**
   * The capability reject shares one shape across every tag-assignment operation, write or read:
   * the backend cannot perform tag-assignment operations, surfaced as a 400 with the manager's
   * explanation. The caller composes the subject, because a write names the tag it was operating on
   * while a read of one object's tags names the kind of object it could not read.
   */
  private static BadRequestException tagAssignmentsUnsupported(
      String action, String subject, BaseResult result) {
    return new BadRequestException(
        "Cannot %s %s: %s", action, subject, result.getExtraInformation());
  }

  /**
   * The refusal a tag read owes its caller when answering it would examine more candidate
   * assignments than the request may consume. It is a client error rather than a server fault: the
   * request asked for more work than this deployment performs in one response, and it names the
   * limit so the caller knows what it asked past.
   *
   * <p>The remedy it names is deliberately not "ask for a smaller page". A page bounds this read's
   * results but not its candidates, because the hierarchy walked is the same hierarchy whether one
   * tag or the whole set is asked for, so the page size does not move this budget in either
   * direction. Pointing a caller at the page size would send it round a loop that cannot succeed.
   */
  private static BadRequestException candidateBudgetExceeded(int candidateBudget) {
    return new BadRequestException(
        "Reading this target's tags would examine more candidate assignments than one request may"
            + " consume (limit %d); the page size does not change this budget, so retry as a"
            + " full-result read by setting pagination=false, or reduce the number of tags assigned"
            + " across this target's hierarchy",
        candidateBudget);
  }

  /**
   * The refusal a reverse lookup owes its caller when the budget ran out before a single candidate
   * could be returned or resumed from. The remedy names what actually moves the budget: it is
   * derived from the page size, up to the deployment's maximum page size, and there is no separate
   * setting for it. It does not suggest narrowing the value filter, because a filter that rejects
   * more rows examines more rows, not fewer.
   */
  private static BadRequestException candidateBudgetExhaustedWithoutProgress(int candidateBudget) {
    return new BadRequestException(
        "This request examined more candidate assignments than its work budget allows (limit %d)"
            + " without finding one it could return or resume from; the budget grows with the page"
            + " size, up to the deployment's maximum page size, and a client that sent a value"
            + " filter can instead page without it and apply the filter to the pages it receives",
        candidateBudget);
  }

  /** apply-method for a winning assignment stored on the queried target itself. */
  private static final String APPLY_METHOD_DIRECT = "DIRECT";

  /** apply-method for a winning assignment stored on a parent of the queried target. */
  private static final String APPLY_METHOD_INHERITED = "INHERITED";

  /**
   * Returns the direct or effective tags of one target. The effective view walks the target's
   * parent chain root-to-leaf keeping, per tag definition, the closest assignment that passes the
   * definition's target-types rules in both directions: an excluded kind is skipped as an
   * assignment source and omitted as a queried destination, but an excluded intermediate kind does
   * not stop the walk. The read returns the complete result or fails; it never degrades to a
   * partial hierarchy or falls back to the direct view.
   *
   * <p>The target is resolved once, by the caller, and that one resolution is what both the read
   * and any continuation the caller binds are built on. The read of the assignment rows is
   * attempted twice against it: the manager reports a concurrent write it could not read through as
   * an unavailable service, and rows or definitions that moved between two of its own reads are
   * exactly what a second attempt can find settled. What a second attempt cannot change is the
   * resolution itself -- it is fixed for the request by design, so a target whose schema moved
   * under the read fails rather than silently answering from a newer state. One retry, then the
   * failure stands: a request that cannot establish one state is a service failure, not something
   * to keep attempting.
   */
  public Page<ObjectTag> getObjectTags(
      ResolvedTarget resolved,
      TagAttachmentTarget target,
      boolean effective,
      PageToken pageToken,
      int candidateBudget) {
    try {
      return getObjectTagsOnce(resolved, target, effective, pageToken, candidateBudget);
    } catch (PolarisServiceUnavailableException firstAttempt) {
      return getObjectTagsOnce(resolved, target, effective, pageToken, candidateBudget);
    }
  }

  /**
   * {@link #getObjectTags(ResolvedTarget, TagAttachmentTarget, boolean, PageToken, int)} for a
   * caller that has no continuation to bind and so needs no resolution of its own.
   */
  public Page<ObjectTag> getObjectTags(
      TagAttachmentTarget target, boolean effective, PageToken pageToken, int candidateBudget) {
    return getObjectTags(resolveTarget(target), target, effective, pageToken, candidateBudget);
  }

  private Page<ObjectTag> getObjectTagsOnce(
      ResolvedTarget resolved,
      TagAttachmentTarget target,
      boolean effective,
      PageToken pageToken,
      int candidateBudget) {
    PolarisResolvedPathWrapper resolvedTarget = resolved.path();
    int fieldId = resolved.fieldId();
    String metadataLocation = resolved.metadataLocation();
    String queriedKind = target.getType().toString();

    if (!effective) {
      PolarisEntity leaf = resolvedTarget.getRawLeafEntity();
      Map<Long, ObjectTag> direct = new LinkedHashMap<>();
      var byLevel =
          loadVisibleAssignmentsByLevel(
              queriedKind,
              List.of(new TargetLevel(leaf, fieldId, metadataLocation)),
              candidateBudget);
      for (VisibleAssignment assignment : assignmentsAt(byLevel, leaf.getId(), fieldId)) {
        if (!assignment.tag().getTargetTypes().contains(queriedKind)) {
          continue;
        }
        direct.put(
            assignment.tag().getId(), constructObjectTag(assignment, APPLY_METHOD_DIRECT, target));
      }
      return pageOfWinners(direct, pageToken);
    }

    // One traversal level per path entity, root to leaf, plus a final column level for a column
    // query. A column can only carry a direct assignment at the queried level itself.
    record TraversalLevel(
        PolarisEntity entity, String kind, int fieldId, TagAttachmentTarget assignedAt) {}
    List<PolarisEntity> fullPath = resolvedTarget.getRawFullPath();
    List<TraversalLevel> levels = new ArrayList<>();
    List<String> pathNames = new ArrayList<>();
    for (PolarisEntity entity : fullPath) {
      if (entity.getType() == PolarisEntityType.CATALOG) {
        levels.add(
            new TraversalLevel(
                entity,
                TargetType.CATALOG.toString(),
                0,
                TagAttachmentTarget.builder(TargetType.CATALOG).build()));
      } else {
        pathNames.add(entity.getName());
        TargetType kind = traversalKindOf(entity);
        levels.add(
            new TraversalLevel(
                entity,
                kind.toString(),
                0,
                TagAttachmentTarget.builder(kind).setPath(List.copyOf(pathNames)).build()));
      }
    }
    if (target.getType() == TargetType.COLUMN) {
      levels.add(
          new TraversalLevel(
              fullPath.get(fullPath.size() - 1), TargetType.COLUMN.toString(), fieldId, target));
    }

    // Load every level's assignments in one call: the whole traversal describes one state that
    // actually existed, not results stitched from independent reads taken at different times.
    List<TargetLevel> requestedLevels =
        levels.stream()
            .map(
                level ->
                    new TargetLevel(
                        level.entity(),
                        level.fieldId(),
                        // Only a field level resolved a schema, so only it carries the pointer that
                        // schema came from; a whole-object level has nothing of the kind to check.
                        level.fieldId() == 0 ? null : metadataLocation))
            .collect(Collectors.toList());
    var byLevel = loadVisibleAssignmentsByLevel(queriedKind, requestedLevels, candidateBudget);

    // Closest wins per definition: root-to-leaf overwrite keyed by the definition's id (never its
    // name, so a recreated same-name definition can never inherit an old assignment).
    Map<Long, ObjectTag> winners = new LinkedHashMap<>();
    Set<Long> omittedDefinitions = new HashSet<>();
    for (int i = 0; i < levels.size(); i++) {
      TraversalLevel level = levels.get(i);
      boolean isQueriedLevel = i == levels.size() - 1;
      for (VisibleAssignment assignment :
          assignmentsAt(byLevel, level.entity().getId(), level.fieldId())) {
        long tagId = assignment.tag().getId();
        if (omittedDefinitions.contains(tagId)) {
          continue;
        }
        List<String> targetTypes = assignment.tag().getTargetTypes();
        if (!targetTypes.contains(queriedKind)) {
          // the queried target kind is excluded by this definition: omit it entirely
          omittedDefinitions.add(tagId);
          continue;
        }
        if (!targetTypes.contains(level.kind())) {
          // excluded assignment source: ignore this level but keep walking
          continue;
        }
        winners.put(
            tagId,
            constructObjectTag(
                assignment,
                isQueriedLevel ? APPLY_METHOD_DIRECT : APPLY_METHOD_INHERITED,
                level.assignedAt()));
      }
    }
    return pageOfWinners(winners, pageToken);
  }

  /**
   * One page of a target's tags, keyed and ordered by tag definition id.
   *
   * <p>The hierarchy walk above is never cut short: a page bounds what is returned, not what was
   * considered, so an effective result still describes the complete hierarchy for the tags on it.
   * The definition id is the continuation key because it is the one identity the result is unique
   * on and it outlives a rename, so a page boundary cannot drop or repeat a definition when one is
   * renamed between requests.
   */
  private Page<ObjectTag> pageOfWinners(Map<Long, ObjectTag> winners, PageToken pageToken) {
    List<Long> ordered = new ArrayList<>(winners.keySet());
    Collections.sort(ordered);
    long after =
        pageToken.valueAs(EntityIdToken.class).map(EntityIdToken::entityId).orElse(Long.MIN_VALUE);
    Stream<Long> remaining =
        pageToken.value().isPresent()
            ? ordered.stream().filter(id -> id > after)
            : ordered.stream();
    // Page.mapped hands the token builder null once the stream is exhausted, which is how it says
    // "complete"; an unboxing method reference would fail on exactly that call.
    return Page.mapped(
        pageToken,
        remaining,
        winners::get,
        last -> last == null ? null : EntityIdToken.fromEntityId(last));
  }

  /**
   * Reverse lookup: one page of the targets carrying a direct assignment of one definition,
   * optionally filtered by an exact, case-sensitive selected value. Only direct assignments are
   * returned, soft-dropped and orphaned targets are hidden, and a column result is hidden when its
   * field id no longer names a top-level column of the current table schema.
   *
   * <p>The definition arrives already resolved, by the caller, once. A name is not an identity: a
   * definition dropped and recreated under the same name is a different definition inheriting none
   * of the old assignments, so resolving the name a second time inside the request would let the
   * rows be read from one definition while the request was authorized, and its continuation bound,
   * against another. Being handed the identity makes those the same value rather than two values
   * that would have to be compared, and a definition that is gone by the time its rows are read is
   * reported missing by the store's own existence check on that id.
   *
   * <p>{@code readable} decides, per candidate, whether the caller may read that target. A target
   * it refuses is left out of the result rather than reported, and is a consumed candidate: the
   * continuation advances past it. It never turns a refusal into an error, and it never swallows
   * one: a failure to reach a decision propagates.
   *
   * <p>{@code candidateBudget} bounds the work one request may do, which a page size alone cannot,
   * because filtering may examine many candidates without returning any. It is one budget for the
   * whole request: every persistence read below charges the rows it examined to the same instance,
   * so a later read sees what the earlier ones spent. When the budget runs out after a candidate
   * has been consumed, the page is returned short, or empty, with a token that resumes strictly
   * after that candidate, so nothing examined-but-unreturned is lost. When it runs out before any
   * candidate was consumed there is no position to resume from, and the request is refused rather
   * than answered with an empty page that would read as the end of the definition.
   */
  public Page<TaggedObject> listObjectsByTag(
      TagEntity tag,
      String valueFilter,
      PageToken pageToken,
      Predicate<TagAttachmentTarget> readable,
      int candidateBudget) {
    List<TaggedObject> objects = new ArrayList<>();
    // One metadata read per distinct table state, reused for every column row that arrives from
    // that
    // same state, including across the persistence reads below. A row whose target carries a newer
    // pointer reads again rather than reusing what an earlier read returned.
    Map<Long, SchemaAt> schemaByTableId = new HashMap<>();
    PageToken currentPageToken = pageToken;
    int requested = pageToken.pageSize().orElse(Integer.MAX_VALUE);
    CandidateBudget budget = CandidateBudget.of(candidateBudget);
    TagAssignmentRecord lastConsumed = null;
    boolean storeExhausted = false;

    while (true) {
      LoadTagAssignmentTargetsResult result;
      try {
        result =
            metaStoreManager.loadTargetsOnTag(
                callContext.getPolarisCallContext(), tag, valueFilter, currentPageToken, budget);
      } catch (CandidateBudgetExceededException rowsRemain) {
        // The read had rows left to examine and no budget to examine them with. A request that has
        // consumed a candidate resumes after it: the rows that read looked at sit past that
        // candidate and are examined again by the next page, so nothing is skipped. A request that
        // has consumed none has no position to resume from and is refused, never answered with an
        // empty page that would read as the end of the definition.
        if (lastConsumed == null) {
          throw candidateBudgetExhaustedWithoutProgress(candidateBudget);
        }
        break;
      }
      if (!result.isSuccess()) {
        switch (result.getReturnStatus()) {
          case ENTITY_NOT_FOUND:
            // The definition this request resolved is no longer there. A definition recreated under
            // the same name is a different definition, so this is a miss and never a substitution.
            throw new NoSuchTagException(String.format("Tag does not exist: %s", tag.getName()));
          case TAG_ASSIGNMENTS_NOT_SUPPORTED:
            // Reads reject the same way writes do: an incapable store must never silently report
            // "nothing to list" for a read it cannot actually perform.
            throw tagAssignmentsUnsupported("list objects by", "tag " + tag.getName(), result);
          default:
            throw new IllegalStateException(
                String.format(
                    "Failed to list objects by tag %s error status: %s with extraInfo: %s",
                    tag.getName(), result.getReturnStatus(), result.getExtraInformation()));
        }
      }

      Map<Long, PolarisBaseEntity> targetsById = result.getTargetEntitiesAsMap();
      boolean pageFull = false;
      for (var record : result.getAssignments().items()) {
        if (objects.size() >= requested) {
          // Stop before touching this row: it stays fetched-but-unconsumed and the token below
          // resumes at it, so no candidate is skipped.
          pageFull = true;
          break;
        }
        lastConsumed = record;
        PolarisBaseEntity targetEntity = targetsById.get(record.getTargetId());
        if (targetEntity == null || targetEntity.getDropTimestamp() != 0) {
          // purged or soft-dropped target: the row is orphaned or inactive and stays hidden
          continue;
        }
        TagAttachmentTarget itemTarget =
            constructAssignmentTarget(record.getFieldId(), targetEntity, schemaByTableId);
        if (itemTarget == null) {
          continue;
        }
        if (!tag.getTargetTypes().contains(itemTarget.getType().toString())) {
          // belt: only target kinds listed by the definition may appear
          continue;
        }
        if (!readable.test(itemTarget)) {
          // The caller may read the definition but not this target. Reading the definition is not
          // authority over the objects carrying it, so the row is left out rather than reported.
          continue;
        }
        objects.add(
            TaggedObject.builder()
                .setTarget(itemTarget)
                .setValues(new LinkedHashSet<>(List.of(record.getValue())))
                .setApplyMethod(APPLY_METHOD_DIRECT)
                .build());
      }

      String storeToken = result.getAssignments().encodedResponseToken();
      if (pageFull) {
        break;
      }
      if (storeToken == null) {
        storeExhausted = true;
        break;
      }
      if (objects.size() >= requested) {
        break;
      }
      if (budget.isExhausted()) {
        // Rows remain and the budget does not. Every row read so far was consumed, so the page is
        // answered as it stands, with a continuation after the last of them.
        break;
      }
      // This persistence page was entirely hidden or filtered. Pull the next one rather than
      // answering an empty page while data remains. What stops this loop is the budget: it is
      // finite on both the paged and the full-result path, and only an operator who switches the
      // unpaginated result limit off leaves it unbounded.
      currentPageToken =
          PageToken.build(storeToken, null, 0 /* keep the size the token carries */, () -> true);
    }

    if (!pageToken.paginationRequested()) {
      // A full-result request is answered in full or not at all; the handler's own limit probe
      // decides that, so there is no continuation to hand back here. The handler reaches this
      // branch
      // only where it has no limit to probe with, having been told to answer every result, and it
      // passes an unbounded budget with it, so there is no bound here left to report either.
      return Page.fromItems(objects);
    }
    return Page.page(
        pageToken,
        objects,
        storeExhausted ? null : TagAssignmentTargetToken.fromRecord(lastConsumed));
  }

  /** One assignment row whose tag definition still resolves, paired with that definition. */
  private record VisibleAssignment(TagAssignmentRecord record, TagEntity tag) {}

  /**
   * Loads the assignments stored on every requested (target, fieldId) level in one call, and hides
   * every row whose definition no longer resolves or is soft-dropped: physical rows left behind by
   * a failed cleanup must never surface as results.
   *
   * <p>The read is bounded: the levels say how far to walk, but one level may carry any number of
   * assignments, so the request also states how many candidate rows it may consume. A read that
   * needs more is refused rather than trimmed, because an effective result must describe the whole
   * hierarchy and a trimmed one silently would not.
   *
   * @param queriedKind the wire target-type of the object being read, used only to name what could
   *     not be read if the backend rejects this call
   * @param levels the (target, fieldId) levels to load, in no particular order
   * @param candidateBudget the greatest number of assignment rows this read may consume across
   *     every level
   * @return the visible assignments grouped by target entity id, then by field id. A caller reads
   *     one level's assignments with {@link #assignmentsAt}.
   */
  private Map<Long, Map<Integer, List<VisibleAssignment>>> loadVisibleAssignmentsByLevel(
      String queriedKind, List<TargetLevel> levels, int candidateBudget) {
    var result =
        metaStoreManager.loadTagsOnEntities(
            callContext.getPolarisCallContext(), levels, candidateBudget);
    if (!result.isSuccess()) {
      switch (result.getReturnStatus()) {
        case TAG_ASSIGNMENTS_NOT_SUPPORTED:
          // Reads reject the same way writes do: an incapable store must never silently report
          // "no assignments" for a read it cannot actually perform.
          throw tagAssignmentsUnsupported("read tags on", queriedKind, result);
        case ENTITY_NOT_FOUND:
        case ENTITY_CANNOT_BE_RESOLVED:
          // a requested target was purged between resolution and this load; the write path
          // answers the same miss 404.
          throw new NoSuchTargetException("Target no longer exists");
        default:
          throw new IllegalStateException(
              String.format(
                  "Failed to load tags on entities error status: %s with extraInfo: %s",
                  result.getReturnStatus(), result.getExtraInformation()));
      }
    }
    if (result.getTagAssignmentRecords().size() > candidateBudget) {
      // The read came back with more rows than the request was allowed to consume, which is how a
      // bounded read reports that it was cut short. There is no honest short answer available: an
      // effective read that stopped part way through a level would report a hierarchy that was
      // never whole, and a client cannot tell that from a complete one.
      throw candidateBudgetExceeded(candidateBudget);
    }
    Map<Long, TagEntity> definitionsById = new HashMap<>();
    for (PolarisBaseEntity definition : result.getEntities()) {
      if (definition != null && definition.getDropTimestamp() == 0) {
        definitionsById.put(definition.getId(), TagEntity.of(definition));
      }
    }
    Map<Long, Map<Integer, List<VisibleAssignment>>> byLevel = new HashMap<>();
    for (var record : result.getTagAssignmentRecords()) {
      TagEntity definition = definitionsById.get(record.getTagId());
      if (definition == null) {
        continue;
      }
      byLevel
          .computeIfAbsent(record.getTargetId(), unused -> new HashMap<>())
          .computeIfAbsent(record.getFieldId(), unused -> new ArrayList<>())
          .add(new VisibleAssignment(record, definition));
    }
    return byLevel;
  }

  /** The visible assignments of one requested level, or an empty list if it has none. */
  private static List<VisibleAssignment> assignmentsAt(
      Map<Long, Map<Integer, List<VisibleAssignment>>> byLevel, long targetId, int fieldId) {
    return byLevel.getOrDefault(targetId, Map.of()).getOrDefault(fieldId, List.of());
  }

  private static ObjectTag constructObjectTag(
      VisibleAssignment assignment, String applyMethod, TagAttachmentTarget assignedAt) {
    return ObjectTag.builder()
        .setTag(
            TagIdentifier.builder()
                .setId(Long.toString(assignment.tag().getId()))
                .setName(assignment.tag().getName())
                .build())
        .setValues(new LinkedHashSet<>(List.of(assignment.record().getValue())))
        .setApplyMethod(applyMethod)
        .setAssignedAt(assignedAt)
        .build();
  }

  /**
   * A table's schema together with the Iceberg metadata pointer it was read from. The pointer is
   * part of the cached value, not just of the read that produced it, because a later row of the
   * same table may arrive from a newer state: what makes a cached schema reusable is not that it is
   * the same table, it is that it is the same state of that table.
   */
  record SchemaAt(String metadataLocation, Schema schema) {}

  /**
   * Whether a schema already read for a table may render a row whose own target carries {@code
   * metadataLocation}. Only if it was read from that same pointer: rows of one table can arrive
   * across several persistence reads inside one request, and a later read may bring a newer
   * pointer, so a schema memoized from the earlier one would name a column as of a state that row's
   * assignment never belonged to. A cached entry with no pointer recorded, or a row whose target
   * has none, cannot be shown to describe the same state and so does not count as a hit.
   */
  @VisibleForTesting
  static boolean stillDescribes(@Nullable SchemaAt cached, @Nullable String metadataLocation) {
    return cached != null
        && cached.metadataLocation() != null
        && metadataLocation != null
        && cached.metadataLocation().equals(metadataLocation);
  }

  /**
   * Rebuilds the API target a reverse-lookup row addresses, or null when the row must stay hidden
   * (an unsupported entity kind, a table that no longer resolves, or a removed column).
   */
  private TagAttachmentTarget constructAssignmentTarget(
      int fieldId, PolarisBaseEntity targetEntity, Map<Long, SchemaAt> schemaByTableId) {
    if (fieldId != 0) {
      if (targetEntity.getType() != PolarisEntityType.TABLE_LIKE
          || targetEntity.getSubType() != PolarisEntitySubType.ICEBERG_TABLE) {
        return null;
      }
      var tableEntity = IcebergTableLikeEntity.of(targetEntity);
      TableIdentifier tableIdentifier = tableEntity.getTableIdentifier();
      SchemaAt cached = schemaByTableId.get(targetEntity.getId());
      Schema schema =
          stillDescribes(cached, tableEntity.getMetadataLocation()) ? cached.schema() : null;
      if (schema == null) {
        if (tableEntity.getMetadataLocation() == null) {
          // Being unable to read a table's metadata does not establish that its column is gone, so
          // the row cannot be hidden on that basis: hiding it would answer the lookup as if the
          // assignment did not exist. A read that cannot establish what it was asked for fails,
          // the same way any other failure to load metadata already does here.
          throw new IllegalStateException(
              String.format(
                  "Cannot list objects by tag: table %s has no current metadata location, so the"
                      + " column assignment on it cannot be validated",
                  tableIdentifier));
        }
        // the target was looked up by id and soft-drop-filtered above; read its current schema
        // once for all of its column rows, validating storage against the resolved catalog
        schema =
            TagCatalogUtils.loadCurrentSchema(
                storageAccessConfigProvider,
                fileIOFactory,
                realmConfig,
                catalogEntity,
                tableIdentifier,
                tableEntity,
                resolvedEntityView.getResolvedReferenceCatalogEntity());
        schemaByTableId.put(
            targetEntity.getId(), new SchemaAt(tableEntity.getMetadataLocation(), schema));
      }
      String columnName = TagCatalogUtils.findTopLevelColumnName(schema, fieldId);
      if (columnName == null) {
        // the field id no longer names a top-level column: the row is orphaned and stays hidden
        return null;
      }
      return TagAttachmentTarget.builder(TargetType.COLUMN)
          .setPath(tablePathOf(tableIdentifier))
          .setColumn(List.of(columnName))
          .build();
    }
    switch (targetEntity.getType()) {
      case CATALOG:
        return TagAttachmentTarget.builder(TargetType.CATALOG).build();
      case NAMESPACE:
        return TagAttachmentTarget.builder(TargetType.NAMESPACE)
            .setPath(Arrays.asList(NamespaceEntity.of(targetEntity).asNamespace().levels()))
            .build();
      case TABLE_LIKE:
        {
          // Resolve the identifier through the shared table-like accessor, not the Iceberg
          // subclass: a whole-object assignment on a GENERIC_TABLE is a target the write path
          // accepts, and IcebergTableLikeEntity's constructor rejects every subtype but Iceberg
          // table and view, so building it that way would fail this whole page over one row the
          // lookup is required to return.
          TableLikeEntity tableLike = tableLikeTargetOf(targetEntity);
          if (tableLike == null) {
            return null;
          }
          return TagAttachmentTarget.builder(tableLikeKindOf(targetEntity))
              .setPath(tablePathOf(tableLike.getTableIdentifier()))
              .build();
        }
      default:
        return null;
    }
  }

  /**
   * The table-like wrapper for a reverse-lookup row's target, or null when the row's subtype is not
   * one this API returns as a whole-object target. Each resolves its identifier through {@link
   * TableLikeEntity}, which derives it from the stored parent namespace and name without caring
   * which table format the entity is.
   */
  private static TableLikeEntity tableLikeTargetOf(PolarisBaseEntity targetEntity) {
    return switch (targetEntity.getSubType()) {
      case ICEBERG_TABLE, ICEBERG_VIEW -> IcebergTableLikeEntity.of(targetEntity);
      case GENERIC_TABLE -> GenericTableEntity.of(targetEntity);
      default -> null;
    };
  }

  /**
   * The API kind of a table-like entity. A namespace and a name identify either a table or a view,
   * and the two are separate target kinds a definition may list independently, so the subtype is
   * what decides and never the path.
   */
  private static TargetType tableLikeKindOf(PolarisBaseEntity targetEntity) {
    return targetEntity.getSubType() == PolarisEntitySubType.ICEBERG_VIEW
        ? TargetType.VIEW
        : TargetType.TABLE;
  }

  /**
   * The kind one level of the effective-read walk reports. The queried target's own kind is the
   * request's, but a parent level is whatever the resolved entity is, and a view level has to say
   * so: the target-types test runs against this kind, so reporting a view as a table would let a
   * definition that lists only TABLE reach it.
   */
  private static TargetType traversalKindOf(PolarisEntity entity) {
    if (entity.getType() == PolarisEntityType.NAMESPACE) {
      return TargetType.NAMESPACE;
    }
    return tableLikeKindOf(entity);
  }

  /** The namespace-levels-plus-name path form the API uses for a table-like target. */
  private static List<String> tablePathOf(TableIdentifier tableIdentifier) {
    List<String> tablePath = new ArrayList<>(Arrays.asList(tableIdentifier.namespace().levels()));
    tablePath.add(tableIdentifier.name());
    return tablePath;
  }

  private PolarisResolvedPathWrapper resolveAssignmentTarget(TagAttachmentTarget target) {
    var resolvedTarget = TagCatalogUtils.getResolvedTargetWrapper(resolvedEntityView, target);
    PolarisEntitySubType subType = resolvedTarget.getRawLeafEntity().getSubType();
    if (target.getType() == TargetType.COLUMN) {
      // v1 supports columns of Iceberg tables only: generic tables define no stable column id,
      // and view column ids are not stable across replaces.
      if (subType != PolarisEntitySubType.ICEBERG_TABLE) {
        throw new BadRequestException(
            "Column targets are supported only on Iceberg tables; %s is not", subType);
      }
      if (target.getColumn() == null
          || target.getColumn().size() != 1
          || target.getColumn().get(0) == null
          || target.getColumn().get(0).isBlank()) {
        throw new BadRequestException("column must contain exactly one top-level column name");
      }
    } else if (target.getType() == TargetType.TABLE
        && subType != PolarisEntitySubType.ICEBERG_TABLE
        && subType != PolarisEntitySubType.GENERIC_TABLE) {
      // TagCatalogUtils.getResolvedTargetWrapper already excludes a view here, so this is a
      // defensive assertion that a table target always resolves to one of the two subtypes it
      // recognizes.
      throw new BadRequestException(
          "Table targets require an Iceberg or generic table; %s is not", subType);
    } else if (target.getColumn() != null && !target.getColumn().isEmpty()) {
      throw new BadRequestException("column is only valid for column targets");
    }
    return resolvedTarget;
  }

  private int resolveFieldId(
      TagAttachmentTarget target, PolarisResolvedPathWrapper resolvedTarget) {
    return resolveField(target, resolvedTarget).fieldId();
  }

  /**
   * What one request's target resolved to: the path it named, the Iceberg field id a column target
   * addresses, and the metadata pointer the schema that field was read from came from.
   *
   * <p>The three travel together because they are one fact, established once. The pointer belongs
   * with the field because a read that reports a column has used that schema to decide the column
   * exists, and the assignment read has to be able to establish that they still describe the state
   * it returns. The path belongs with them because the field id is meaningless without the table it
   * was resolved against, and a caller that has to bind a continuation to the object being read
   * needs the whole identity, not the half of it a name can express.
   */
  record ResolvedTarget(
      PolarisResolvedPathWrapper path, int fieldId, @Nullable String metadataLocation) {}

  /**
   * Resolves and validates a tag-read target once, for the whole request.
   *
   * <p>A column's identity is its containing table's id together with its Iceberg field id, and the
   * field id comes from the table's current schema. Resolving it twice in one request is therefore
   * two different questions asked of two different moments, so the caller that binds a page token
   * to the object being read and the read that answers the page are given the same resolution
   * rather than each taking their own.
   */
  ResolvedTarget resolveTarget(TagAttachmentTarget target) {
    return resolveField(target, resolveAssignmentTarget(target));
  }

  private ResolvedTarget resolveField(
      TagAttachmentTarget target, PolarisResolvedPathWrapper resolvedTarget) {
    if (target.getType() != TargetType.COLUMN) {
      return new ResolvedTarget(resolvedTarget, 0, null);
    }
    TableIdentifier tableIdentifier = TableIdentifier.of(target.getPath().toArray(new String[0]));
    Schema schema =
        TagCatalogUtils.loadCurrentSchema(
            storageAccessConfigProvider,
            fileIOFactory,
            realmConfig,
            catalogEntity,
            resolvedEntityView,
            tableIdentifier,
            resolvedTarget);
    return new ResolvedTarget(
        resolvedTarget,
        TagCatalogUtils.resolveTopLevelFieldId(schema, target.getColumn().get(0)),
        IcebergTableLikeEntity.of(resolvedTarget.getRawLeafEntity()).getMetadataLocation());
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
