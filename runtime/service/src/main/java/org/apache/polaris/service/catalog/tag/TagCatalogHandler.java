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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisSecurable;
import org.apache.polaris.core.auth.RenameAuthorizationIntent;
import org.apache.polaris.core.auth.SingleTargetAuthorizationIntent;
import org.apache.polaris.core.auth.TagAttachmentAuthorizationIntent;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfiguration;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.tag.TagEntity;
import org.apache.polaris.core.tag.TagValidation;
import org.apache.polaris.core.tag.TagVersionToken;
import org.apache.polaris.core.tag.exceptions.NoSuchCatalogException;
import org.apache.polaris.core.tag.exceptions.NoSuchTagException;
import org.apache.polaris.core.tag.exceptions.NoSuchTargetException;
import org.apache.polaris.core.tag.exceptions.TagVersionMismatchException;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.catalog.common.PolarisSecurableMapper;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.idempotency.IdempotencyRequestContext;
import org.apache.polaris.service.types.CreateTagRequest;
import org.apache.polaris.service.types.GetObjectTagsResponse;
import org.apache.polaris.service.types.ListObjectsByTagResponse;
import org.apache.polaris.service.types.ListTagsResponse;
import org.apache.polaris.service.types.ObjectTag;
import org.apache.polaris.service.types.RenameTagRequest;
import org.apache.polaris.service.types.Tag;
import org.apache.polaris.service.types.TagAttachmentTarget;
import org.apache.polaris.service.types.TagIdentifier;
import org.apache.polaris.service.types.TaggedObject;
import org.apache.polaris.service.types.TargetType;
import org.apache.polaris.service.types.UpdateTagRequest;
import org.immutables.value.Value;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;

@PolarisImmutable
@SuppressWarnings("immutables:incompat")
public abstract class TagCatalogHandler extends CatalogHandler {

  /**
   * How many candidates one tag read may consume per result it is allowed to return. The factor,
   * not the absolute number, is the choice: it keeps the bound proportional to the page a
   * deployment already sized, and it is large enough that an ordinary page of mostly-readable
   * targets, or of a hierarchy holding a handful of assignments per level, never hits it.
   */
  private static final int CANDIDATES_PER_RESULT_BUDGET = 20;

  private TagCatalog tagCatalog;

  /**
   * The request's idempotency context. A handler built outside CDI, as the tests do, gets the
   * disabled instance, which is the same state a deployment with the shared mechanism switched off
   * presents: no pending key, so every operation takes its ordinary path.
   */
  @Value.Default
  public IdempotencyRequestContext idempotencyRequestContext() {
    return IdempotencyRequestContext.DISABLED;
  }

  protected abstract StorageAccessConfigProvider storageAccessConfigProvider();

  protected abstract FileIOFactory fileIOFactory();

  @Override
  protected void initializeCatalog() {
    this.tagCatalog =
        new TagCatalog(
            metaStoreManager(),
            callContext(),
            this.resolutionManifest,
            idempotencyRequestContext(),
            storageAccessConfigProvider(),
            fileIOFactory(),
            realmConfig());
  }

  /**
   * Creates one definition and returns it directly: the definition's fields and its version token
   * are the response, so a client reads the token it will send back as current-tag-version from the
   * same object it reads the definition from. loadTag and updateTag answer the same shape.
   */
  public Tag createTag(CreateTagRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_TAG;
    // The tag itself does not exist yet; the optional passthrough path lets the catalog detect a
    // name collision, the same way policy creation passes the not-yet-existing policy through.
    authorizeCatalogScopedTagOperationOrThrow(op, request.getName());

    return tagCatalog.createTag(
        request.getName(), request.getDescription(), request.getValues(), request.getTargetTypes());
  }

  public ListTagsResponse listTags(
      boolean paged, @Nullable String pageToken, @Nullable Integer pageSize) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_TAG;
    authorizeCatalogScopedTagOperationOrThrow(op, null);

    requirePositivePageSize(pageSize);

    // Paging is what a request gets unless it asks for the whole collection, so an omitted or empty
    // pageToken is the first page rather than everything, and a pageSize bounds that page. The
    // adapter has already refused a full-result request that carried either parameter, so the mode
    // arriving here is consistent with the parameters arriving with it.
    Page<TagIdentifier> page = paged ? listOneTagPage(pageToken, pageSize) : listEveryTag();
    // The cursor says where to resume and nothing about which listing it came from, so what goes
    // out carries the realm and the catalog too. A complete result has no cursor and so has nothing
    // to carry.
    String cursor = page.encodedResponseToken();
    return ListTagsResponse.builder()
        .setIdentifiers(new LinkedHashSet<>(page.items()))
        .setNextPageToken(
            cursor == null
                ? null
                : TagPageToken.encode(resolvedRealmId(), resolvedCatalogId(), cursor))
        .build();
  }

  /**
   * A page size is an input to validate whether or not it is used, so this runs in both modes on
   * every listing and every target read. The shared pagination helper validates only {@code >= 0},
   * and only while pagination is enabled, so a zero or a negative would otherwise pass unremarked
   * and the request would answer an empty page as though it had succeeded.
   */
  private static void requirePositivePageSize(@Nullable Integer pageSize) {
    if (pageSize != null && pageSize <= 0) {
      throw new BadRequestException("pageSize must be a positive integer");
    }
  }

  /**
   * Lists every definition in one response, or refuses. A full result is complete or it is an
   * error: truncating it, or quietly turning it into a page, would look like a complete result to a
   * client that cannot tell the difference. One extra row is requested so the limit is known before
   * a response is committed.
   */
  private Page<TagIdentifier> listEveryTag() {
    int limit = listConfig(FeatureConfiguration.LIST_PAGINATION_UNPAGINATED_MAX_RESULTS);
    if (limit <= 0) {
      return tagCatalog.listTags(PageToken.readEverything());
    }
    Page<TagIdentifier> page = tagCatalog.listTags(PageToken.fromLimit(limit + 1));
    if (page.items().size() > limit) {
      throw new BadRequestException(
          "The tag listing is larger than this deployment answers in one response (limit %d);"
              + " retry in paged mode by removing pagination=false or setting it to true, keeping"
              + " the same target and filters",
          limit);
    }
    // The request asked for the whole collection, so the response carries no continuation either.
    return Page.fromItems(page.items());
  }

  /**
   * Reads one of the listing size settings. They all go through here so that adopting the shared
   * configuration is a change in one place, and so that a catalog override reaches every one of
   * them: each of these settings declares a catalog-level property, and only the catalog-scoped
   * read consults it. The catalog is resolved by the time a listing runs, because authorization
   * refuses the operation without one.
   */
  private int listConfig(PolarisConfiguration<Integer> config) {
    return realmConfig()
        .getConfig(
            config,
            requireNonNull(
                resolutionManifest.getResolvedCatalogEntity(), "No resolved catalog entity"));
  }

  /**
   * The catalog this listing is bound to, by entity id rather than by name. The id is what the
   * listing itself narrows by, and unlike a name it does not move: a catalog dropped and recreated
   * under the same name is a different catalog, and a token from the first one does not belong to
   * the second. Authorization has already resolved the catalog by the time a listing runs.
   */
  private long resolvedCatalogId() {
    return requireNonNull(
            resolutionManifest.getResolvedCatalogEntity(), "No resolved catalog entity")
        .getId();
  }

  /**
   * The realm this listing is bound to. A catalog id is only unique inside one realm, because a
   * metastore that numbers entities per realm gives the first catalog of every realm the same id,
   * so the catalog alone does not say which listing a token came from. The realm is resolved before
   * any request reaches a handler, so an absent one is a server fault rather than a bad request.
   */
  private String resolvedRealmId() {
    return requireNonNull(realmContext().getRealmIdentifier(), "No realm identifier");
  }

  /**
   * The largest page this deployment will return. The shared maximum decides wherever it is set;
   * with it unset, which is its own default, the tag ceiling still bounds a page, because a listing
   * that is bounded only when an operator remembers to bound it is not bounded. This specializes
   * the shared hook rather than reading the shared setting directly, so the one name keeps one
   * meaning across handlers; the shared rejection of an unpaged listing that overflows does not
   * apply here, because tag listings page by default and a first page is the answer, not an error.
   */
  @Override
  protected int maxPageSize() {
    int sharedMax = listConfig(FeatureConfiguration.LIST_PAGINATION_MAX_PAGE_SIZE);
    return sharedMax > 0
        ? sharedMax
        : listConfig(FeatureConfiguration.LIST_PAGINATION_MAX_PAGE_SIZE_CEILING);
  }

  /**
   * Lists one page. The size the client asked for is a bound, capped by the deployment maximum;
   * without one, the configured default.
   */
  private Page<TagIdentifier> listOneTagPage(
      @Nullable String pageToken, @Nullable Integer pageSize) {
    int size =
        pageSize != null
            ? pageSize
            : listConfig(FeatureConfiguration.LIST_PAGINATION_DEFAULT_PAGE_SIZE);
    PageToken token;
    try {
      // Paging on tag routes turns on the token decode itself: the request asked for a page, so the
      // token has to be read. An omitted or empty token means "start", which decodes to a plain
      // size
      // limit. Only the decode sits inside this try: a failure from the listing itself is a server
      // fault, and reporting that as a client error would hide it.
      // A token is read for the listing running now, the catalog and the realm that contains it:
      // one
      // minted elsewhere is refused here, before a query runs, rather than accepted as a position
      // that means nothing in this listing.
      String position =
          pageToken == null || pageToken.isEmpty()
              ? null
              : TagPageToken.cursorFor(resolvedRealmId(), resolvedCatalogId(), pageToken);
      token = PageToken.build(position, size, maxPageSize(), () -> true);
    } catch (IllegalArgumentException | IllegalStateException | JacksonException e) {
      // A token this server did not issue is a client error. The shared decode already turns any
      // failure into IllegalArgumentException, which the shared mapper answers 400, so this catch
      // is not what keeps the response off 500. It is what makes the error type BadRequest: a 400
      // raised anywhere else reaches the client as the generic schema-validation type.
      throw new BadRequestException("Invalid page token");
    }
    return tagCatalog.listTags(token);
  }

  public Tag loadTag(String tagName) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_TAG;
    authorizeBasicTagOperationOrThrow(op, tagName);

    return tagCatalog.loadTag(tagName);
  }

  public Tag updateTag(String tagName, UpdateTagRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_TAG;
    authorizeBasicTagOperationOrThrow(op, tagName);

    return tagCatalog.updateTag(tagName, request);
  }

  /**
   * Renames one definition, and answers the same 204 whether the rename ran now or had already run
   * under this key.
   *
   * <p>A retry of a rename that succeeded cannot be found by its source name, which the first
   * attempt freed, and need not still hold its destination name, because the definition may have
   * been renamed again since. So the definition is located by the entity id inside the version
   * token the retry still carries, and authorization then runs against the name that definition
   * holds now, through the ordinary name-based path. The id is a lookup, not a permission: the
   * two-sided check applies to the recognized retry exactly as it does to a first attempt, and the
   * token itself stays opaque to clients.
   */
  public void renameTag(RenameTagRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.RENAME_TAG;
    TagValidation.validateName(request.getSource());
    TagValidation.validateName(request.getDestination());
    if (request.getSource().equals(request.getDestination())) {
      throw new BadRequestException("source and destination must differ");
    }

    if (idempotencyRequestContext().isActive()) {
      Long definitionId = definitionIdOf(request.getCurrentTagVersion());
      if (definitionId != null) {
        String currentName = currentNameOf(resolveCatalogId(), definitionId);
        // Recognition is tried even when the source name resolves, because after a successful
        // rename that name may belong to a different definition, and answering for that one would
        // be wrong in both directions: it is not the definition this request renamed, and it must
        // not be touched on its behalf.
        if (currentName != null
            && authorizeRenameTagOrThrow(op, currentName, false /* requireResolved */)
            && tagCatalog.isRecognizedRename(currentName, definitionId)) {
          return;
        }
      }
    }

    authorizeRenameTagOrThrow(op, request.getSource(), true /* requireResolved */);
    tagCatalog.renameTag(
        request.getSource(), request.getDestination(), request.getCurrentTagVersion());
  }

  public boolean dropTag(String tagName, boolean detachAll) {
    // detach-all removes every assignment of the definition and then the definition itself, so it
    // requires both TAG_DROP and TAG_DETACH on the definition (a distinct operation); a plain drop
    // requires only TAG_DROP.
    PolarisAuthorizableOperation op =
        detachAll
            ? PolarisAuthorizableOperation.DROP_TAG_DETACH_ALL
            : PolarisAuthorizableOperation.DROP_TAG;
    authorizeBasicTagOperationOrThrow(op, tagName);

    return tagCatalog.dropTag(tagName, detachAll);
  }

  public void assignTag(String tagName, TagAttachmentTarget target, List<String> values) {
    authorizeTagAssignmentOperationOrThrow(tagName, target, true);
    tagCatalog.assignTag(tagName, target, values);
  }

  public void unassignTag(String tagName, TagAttachmentTarget target) {
    authorizeTagAssignmentOperationOrThrow(tagName, target, false);
    tagCatalog.unassignTag(tagName, target);
  }

  /**
   * Reads a target's direct or effective tags. Authorization checks the queried target only, with
   * the target's ordinary read-properties privilege; a column read is authorized against its
   * containing table. Once the queried target is authorized, inherited results are not filtered by
   * parent permissions.
   *
   * <p>A paged effective read still walks the complete hierarchy for the tags it returns: the page
   * bounds the results, never the walk.
   */
  public GetObjectTagsResponse getObjectTags(
      TagAttachmentTarget target,
      boolean effective,
      boolean paged,
      @Nullable String pageToken,
      @Nullable Integer pageSize) {
    requirePositivePageSize(pageSize);
    switch (target.getType()) {
      case CATALOG ->
          authorizeCatalogScopedTagOperationOrThrow(
              PolarisAuthorizableOperation.GET_OBJECT_TAGS_ON_CATALOG, null);
      case NAMESPACE -> {
        if (target.getPath() == null || target.getPath().isEmpty()) {
          throw new BadRequestException("Namespace target path must not be empty");
        }
        requireValidPathMembers(target.getPath());
        authorizeObjectTagsTargetOrThrow(
            PolarisAuthorizableOperation.GET_OBJECT_TAGS_ON_NAMESPACE, target);
      }
      // A view is its own target kind with its own privilege; a column read is authorized against
      // the containing table.
      case TABLE, COLUMN -> {
        requireTableLikePath(target);
        authorizeObjectTagsTargetOrThrow(
            PolarisAuthorizableOperation.GET_OBJECT_TAGS_ON_TABLE, target);
      }
      case VIEW -> {
        requireTableLikePath(target);
        authorizeObjectTagsTargetOrThrow(
            PolarisAuthorizableOperation.GET_OBJECT_TAGS_ON_VIEW, target);
      }
    }
    // Paging is what a read gets unless it asks for the target's whole set, so an omitted or empty
    // pageToken is the first page rather than everything. The adapter has already refused a
    // full-result request that carried a token or a size, so the mode arriving here agrees with the
    // parameters arriving with it.
    // One resolution for the request: the same value scopes the token and answers the read, so a
    // continuation cannot be validated against one state of the target and applied to another.
    TagCatalog.ResolvedTarget resolved = tagCatalog.resolveTarget(target);
    Page<ObjectTag> page =
        paged
            ? readOneObjectTagPage(resolved, target, effective, pageToken, pageSize)
            : readEveryObjectTag(resolved, target, effective);
    String cursor = page.encodedResponseToken();
    return GetObjectTagsResponse.builder()
        .setObjectTags(new LinkedHashSet<>(page.items()))
        .setNextPageToken(
            cursor == null
                ? null
                : TagQueryPageToken.encode(
                    TagQueryPageToken.OBJECT_TAGS_PREFIX,
                    objectTagsScope(resolved, target, effective),
                    cursor))
        .build();
  }

  private void requireTableLikePath(TagAttachmentTarget target) {
    if (target.getPath() == null || target.getPath().size() < 2) {
      throw new BadRequestException("Table-like target path must name a namespace and table");
    }
    requireValidPathMembers(target.getPath());
  }

  /**
   * Answers the whole result, or refuses. A full result is complete or it is an error: truncating
   * it, or quietly turning it into a page, would look like a complete set of a target's tags to a
   * client that cannot tell the difference. One extra result is requested so the limit is known
   * before a response is committed.
   *
   * <p>It is bounded twice, because a result bound alone does not bound the work: an effective read
   * walks every level of the target's hierarchy and each level may hold any number of assignments,
   * so the rows examined are not bounded by the tags returned. There being no page here, the work
   * budget comes from the same result limit, so a deployment configures one number rather than two.
   */
  private Page<ObjectTag> readEveryObjectTag(
      TagCatalog.ResolvedTarget resolved, TagAttachmentTarget target, boolean effective) {
    int limit = listConfig(FeatureConfiguration.LIST_PAGINATION_UNPAGINATED_MAX_RESULTS);
    if (limit <= 0) {
      // The operator has switched the unpaginated result limit off. There is no limit to derive a
      // work bound from, and none is invented here: this is the same answer the reverse lookup
      // gives for the same setting, and it takes an explicit override to reach.
      return tagCatalog.getObjectTags(
          resolved, target, effective, PageToken.readEverything(), Integer.MAX_VALUE);
    }
    Page<ObjectTag> page =
        tagCatalog.getObjectTags(
            resolved, target, effective, PageToken.fromLimit(limit + 1), candidateBudget(limit));
    if (page.items().size() > limit) {
      throw new BadRequestException(
          "The tags of this target are more than this deployment answers in one response (limit %d);"
              + " retry in paged mode by removing pagination=false or setting it to true, keeping"
              + " the same target and filters",
          limit);
    }
    // The request asked for no page, so the response carries no continuation either.
    return Page.fromItems(page.items());
  }

  /**
   * Reads one page. The size the client asked for is a bound, capped by the deployment maximum;
   * without one, the configured default, on a continuation as much as on a first page.
   *
   * <p>The work this read may do is <em>not</em> derived from that size. A page bounds this read's
   * results but not its candidates: the hierarchy walked is the same hierarchy whether one tag or
   * the whole set is asked for, so the rows examined are a property of the target. Deriving the
   * budget from the page would therefore bound nothing about the work and would instead make the
   * same target readable at one page size and refused at another. The largest page this deployment
   * would ever hand out is the bound, so every page of a target gets the same budget.
   */
  private Page<ObjectTag> readOneObjectTagPage(
      TagCatalog.ResolvedTarget resolved,
      TagAttachmentTarget target,
      boolean effective,
      @Nullable String pageToken,
      @Nullable Integer pageSize) {
    int size = effectivePageSize(pageSize);
    PageToken token;
    try {
      // The request asked for a page, so the token has to be read whatever the shared pagination
      // flag says. An omitted or empty token means "start", which decodes to a plain size limit.
      // Only the decode sits inside this try: a failure from the read itself is a server fault, and
      // reporting that as a client error would hide it.
      String position =
          pageToken == null || pageToken.isEmpty()
              ? null
              : TagQueryPageToken.cursorFor(
                  TagQueryPageToken.OBJECT_TAGS_PREFIX,
                  objectTagsScope(resolved, target, effective),
                  pageToken);
      token = PageToken.build(position, size, maxPageSize(), () -> true);
    } catch (IllegalArgumentException | IllegalStateException | JacksonException e) {
      throw new BadRequestException("Invalid page token");
    }
    return tagCatalog.getObjectTags(
        resolved, target, effective, token, candidateBudget(Math.max(size, maxPageSize())));
  }

  /**
   * Resolves and authorizes a getObjectTags namespace, table, view or column target as a required
   * path, the way {@link #authorizeTagAssignmentOperationOrThrow} does for the write path, instead
   * of the base CatalogHandler's passthrough form, so a missing namespace or table answers 404
   * NoSuchTargetException rather than the base CatalogHandler's Iceberg exception types.
   */
  private void authorizeObjectTagsTargetOrThrow(
      PolarisAuthorizableOperation op, TagAttachmentTarget target) {
    resolutionManifest = newResolutionManifest();
    switch (target.getType()) {
      case NAMESPACE -> {
        Namespace targetNamespace = Namespace.of(target.getPath().toArray(new String[0]));
        resolutionManifest.addPath(
            new ResolverPath(Arrays.asList(targetNamespace.levels()), PolarisEntityType.NAMESPACE));
      }
      case TABLE, VIEW, COLUMN -> {
        TableIdentifier targetIdentifier =
            TableIdentifier.of(target.getPath().toArray(new String[0]));
        resolutionManifest.addPath(
            new ResolverPath(
                PolarisCatalogHelpers.tableIdentifierToList(targetIdentifier),
                PolarisEntityType.TABLE_LIKE));
      }
      default -> throw new BadRequestException("Unsupported target type: %s", target.getType());
    }

    AuthorizationState authorizationState = new AuthorizationState(resolutionManifest);
    AuthorizationRequest authorizationRequest =
        new AuthorizationRequest(
            polarisPrincipal(),
            List.of(
                new SingleTargetAuthorizationIntent(
                    op,
                    target.getType() == TargetType.NAMESPACE
                        ? PolarisSecurableMapper.namespace(
                            catalogName(), Namespace.of(target.getPath().toArray(new String[0])))
                        : PolarisSecurableMapper.tableLike(
                            catalogName(),
                            TableIdentifier.of(target.getPath().toArray(new String[0]))))));
    authorizer().resolveAuthorizationInputs(authorizationState, authorizationRequest);

    // Classify a failed required path the same way authorizeTagAssignmentOperationOrThrow does: a
    // prefix that names no catalog is the contract's NoSuchCatalog, and a target inside a catalog
    // that did resolve is NoSuchTarget naming the entity that is actually missing.
    ResolverStatus status = resolutionManifest.getPrimaryResolverStatusOrThrow();
    throwIfCatalogMissing(status);
    if (status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED) {
      List<String> failedPath = status.getFailedToResolvePath().entityNames();
      switch (status.getFailedToResolvePath().lastEntityType()) {
        case NAMESPACE ->
            throw new NoSuchTargetException(
                "Namespace does not exist: %s", Namespace.of(failedPath.toArray(new String[0])));
        case TABLE_LIKE ->
            throw new NoSuchTargetException(
                "Table does not exist: %s", TableIdentifier.of(failedPath.toArray(new String[0])));
        default ->
            throw new IllegalStateException(
                "Unexpected unresolved path type: " + status.getFailedToResolvePath());
      }
    }

    // Fetched for its own 404 classification, and for the subtype discrimination a VIEW or TABLE
    // target needs: a target that fails either check throws NoSuchTargetException before the
    // authorizer ever sees it.
    TagCatalogUtils.getResolvedTargetWrapper(resolutionManifest, target);

    authorizer().authorize(authorizationState, authorizationRequest).throwIfDenied();

    initializeCatalog();
  }

  /**
   * Reverse lookup from one definition to the targets carrying its assignments. The operation is
   * authorized on the definition alone, the way loadTag is; it confers no catalog-wide authority.
   * Each target the response would report is judged separately, by that target's own
   * read-properties privilege, while the page is built, and a target the caller may not read is
   * left out rather than reported.
   */
  public ListObjectsByTagResponse listObjectsByTag(
      String tagName,
      @Nullable String value,
      boolean paged,
      @Nullable String pageToken,
      @Nullable Integer pageSize) {
    requirePositivePageSize(pageSize);
    authorizeBasicTagOperationOrThrow(PolarisAuthorizableOperation.LIST_OBJECTS_BY_TAG, tagName);
    // One resolution for the request: the definition the operation was authorized on is the
    // definition whose rows are read and the definition a continuation is bound to.
    TagEntity tag = resolvedTag(tagName);
    // Paging is what a reverse lookup gets unless it asks for the complete result, so an omitted or
    // empty pageToken is the first page. The adapter has already refused a full-result request that
    // carried a token or a size.
    Page<TaggedObject> page =
        paged
            ? listOneTaggedObjectPage(tag, value, pageToken, pageSize)
            : listEveryTaggedObject(tag, value);
    String cursor = page.encodedResponseToken();
    return ListObjectsByTagResponse.builder()
        .setObjects(new LinkedHashSet<>(page.items()))
        .setNextPageToken(
            cursor == null
                ? null
                : TagQueryPageToken.encode(
                    TagQueryPageToken.TAGGED_OBJECTS_PREFIX,
                    taggedObjectsScope(tag, value),
                    cursor))
        .build();
  }

  /**
   * The full-result reverse lookup: the whole result or an error, never a silent truncation.
   *
   * <p>It is bounded twice, because a result bound alone does not bound the work: this operation
   * filters candidates by target existence and by the caller's permission, so a definition whose
   * assignments are mostly hidden can be scanned at length while returning almost nothing. One
   * extra result is requested so the result limit is known before a response is committed, and the
   * work budget is derived from that same limit so a deployment configures one number rather than
   * two.
   */
  private Page<TaggedObject> listEveryTaggedObject(TagEntity tag, @Nullable String value) {
    int limit = listConfig(FeatureConfiguration.LIST_PAGINATION_UNPAGINATED_MAX_RESULTS);
    if (limit <= 0) {
      // The operator has switched the unpaginated result limit off, so there is no limit to derive
      // a
      // work bound from and none is invented here. This is the same answer listTags gives for the
      // same setting, and it takes an explicit override to reach.
      return tagCatalog.listObjectsByTag(
          tag, value, PageToken.readEverything(), this::targetReadable, Integer.MAX_VALUE);
    }
    Page<TaggedObject> page =
        tagCatalog.listObjectsByTag(
            tag,
            value,
            PageToken.fromLimit(limit + 1),
            this::targetReadable,
            candidateBudget(limit));
    if (page.items().size() > limit) {
      throw new BadRequestException(
          "The assignments of this tag are more than this deployment answers in one response (limit"
              + " %d); retry in paged mode by removing pagination=false or setting it to true,"
              + " keeping the same target and filters",
          limit);
    }
    if (page.encodedResponseToken() != null) {
      // The scan stopped while rows remained. It cannot have been the result bound, because fewer
      // results than the limit came back, so the work budget ran out. A full result is
      // complete or it is an error: returning what was collected would look like the whole set to a
      // client that cannot tell the difference.
      throw new BadRequestException(
          "Reading every assignment of this tag would examine more candidates than this deployment"
              + " answers in one response (limit %d); retry in paged mode by removing"
              + " pagination=false or setting it to true, keeping the same target and filters",
          limit);
    }
    return Page.fromItems(page.items());
  }

  /** One page of the reverse lookup, bounded by the client's size, the maximum, or the default. */
  private Page<TaggedObject> listOneTaggedObjectPage(
      TagEntity tag,
      @Nullable String value,
      @Nullable String pageToken,
      @Nullable Integer pageSize) {
    int size = effectivePageSize(pageSize);
    PageToken token;
    try {
      String position =
          pageToken == null || pageToken.isEmpty()
              ? null
              : TagQueryPageToken.cursorFor(
                  TagQueryPageToken.TAGGED_OBJECTS_PREFIX,
                  taggedObjectsScope(tag, value),
                  pageToken);
      token = PageToken.build(position, size, maxPageSize(), () -> true);
    } catch (IllegalArgumentException | IllegalStateException | JacksonException e) {
      throw new BadRequestException("Invalid page token");
    }
    return tagCatalog.listObjectsByTag(
        tag, value, token, this::targetReadable, candidateBudget(size));
  }

  /**
   * The size a page is actually built to: what the request asked for, or the configured default,
   * and then the deployment maximum where there is one. The maximum is applied here and not left to
   * the shared decode alone because the candidate budget is derived from this number, and a budget
   * taken from the requested size would let a request ask for far more scanning than the page it
   * receives can carry. A maximum of zero or less means no maximum, which is the rule the shared
   * bound applies, so it is reproduced rather than reinterpreted.
   */
  private int effectivePageSize(@Nullable Integer pageSize) {
    int requested =
        pageSize != null
            ? pageSize
            : listConfig(FeatureConfiguration.LIST_PAGINATION_DEFAULT_PAGE_SIZE);
    int max = maxPageSize();
    return max > 0 ? Math.min(requested, max) : requested;
  }

  /**
   * How many candidate assignments one request may consume, per result the reader it serves may
   * return. A result bound is not a work bound: filtering by target existence and by the caller's
   * read permission can examine many candidates without returning any. Callers derive the size they
   * pass from what bounds their own candidates -- the reverse lookup from the page it walks, a tag
   * read from the largest page this deployment hands out -- so a deployment that tunes its page
   * sizes tunes both with them, and neither needs a setting of its own.
   */
  private static int candidateBudget(int pageSize) {
    long budget = (long) Math.max(pageSize, 1) * CANDIDATES_PER_RESULT_BUDGET;
    return (int) Math.min(budget, Integer.MAX_VALUE);
  }

  /**
   * The query one object-tag page belongs to: the catalog it resolved in, the target it addresses
   * and the view it asked for. Those are exactly the inputs that decide which tags the read
   * returns, so a cursor is only meaningful against the same three.
   *
   * <p>The path and column contribute element by element, with their sizes, rather than as printed
   * lists: a table {@code ["a", "b"]} with no column and a table {@code ["a"]} with column {@code
   * ["b"]} are different targets whose printed forms could otherwise flatten into one scope.
   *
   * <p>The identity of the object being read is the resolved entity id and, where the target is a
   * column, the Iceberg field id that entity's schema resolved the column name to. The names alone
   * are not that identity: a column dropped and recreated under the same name is a different column
   * of the same table, carrying none of the old column's assignments, and every other term of this
   * scope -- the table's own id included -- is unchanged by that replacement. The field id is added
   * for every kind rather than only for a column, because it is zero wherever a target has no field
   * and one shape is easier to reason about than a conditional one.
   */
  private String objectTagsScope(
      TagCatalog.ResolvedTarget resolved, TagAttachmentTarget target, boolean effective) {
    List<Object> parts = new ArrayList<>();
    parts.add(scopeRealm());
    parts.add(resolvedCatalogId());
    parts.add(resolved.path().getRawLeafEntity().getId());
    parts.add(resolved.fieldId());
    parts.add(target.getType());
    parts.add(effective);
    List<String> path = target.getPath() == null ? List.of() : target.getPath();
    parts.add(path.size());
    parts.addAll(path);
    List<String> column = target.getColumn() == null ? List.of() : target.getColumn();
    parts.add(column.size());
    parts.addAll(column);
    return TagQueryPageToken.scope(parts.toArray());
  }

  /**
   * The query one reverse-lookup page belongs to: the catalog, the definition being looked up and
   * the value filter narrowing it. An omitted filter is its own scope, distinct from any value a
   * client could send, because it narrows differently.
   */
  private String taggedObjectsScope(TagEntity tag, @Nullable String value) {
    return TagQueryPageToken.scope(
        scopeRealm(), resolvedCatalogId(), tag.getId(), tag.getName(), value);
  }

  /**
   * The realm a token was minted in. A catalog id is only unique inside its realm -- the in-memory
   * store hands them out from a per-realm sequence, so the same number names a different catalog in
   * another realm -- and a scope built from that number alone would let a token cross realms and
   * resume against whatever happened to share its id.
   */
  private String scopeRealm() {
    return realmContext().getRealmIdentifier();
  }

  /**
   * The definition a reverse lookup is reading, as resolved once for this request. The name is not
   * that identity: a definition deleted and recreated under the same name is a different definition
   * that inherits none of the old assignments, and a cursor is a position in one definition's
   * assignments. Both the scope a token is bound to and the read itself are given this one value,
   * so neither can be answered from a definition the other never saw.
   */
  private TagEntity resolvedTag(String tagName) {
    return TagEntity.of(
        requireNonNull(
                resolutionManifest.getResolvedPath(
                    ResolvedPathKey.of(List.of(tagName), PolarisEntityType.TAG), true),
                "No resolved tag entity")
            .getRawLeafEntity());
  }

  /**
   * Whether the caller may read one reverse-lookup candidate, using that target's own
   * read-properties privilege; a column is judged through the table that contains it, which is what
   * the resolved path for a COLUMN target already names.
   *
   * <p>A denial hides the candidate, because reading a definition is not authority over the objects
   * carrying it. A target that no longer resolves is likewise hidden, the same way an orphaned row
   * is. Anything else propagates: a failure to reach a decision is a service error, never a denial
   * and never an omission.
   */
  private boolean targetReadable(TagAttachmentTarget target) {
    PolarisResolutionManifest candidateManifest = newResolutionManifest();
    PolarisAuthorizableOperation op;
    PolarisSecurable securable;
    switch (target.getType()) {
      case CATALOG -> {
        op = PolarisAuthorizableOperation.GET_OBJECT_TAGS_ON_CATALOG;
        securable = PolarisSecurableMapper.catalog(catalogName());
      }
      case NAMESPACE -> {
        Namespace namespace = Namespace.of(target.getPath().toArray(new String[0]));
        candidateManifest.addPath(
            new ResolverPath(Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE));
        op = PolarisAuthorizableOperation.GET_OBJECT_TAGS_ON_NAMESPACE;
        securable = PolarisSecurableMapper.namespace(catalogName(), namespace);
      }
      case TABLE, VIEW, COLUMN -> {
        TableIdentifier identifier = TableIdentifier.of(target.getPath().toArray(new String[0]));
        candidateManifest.addPath(
            new ResolverPath(
                PolarisCatalogHelpers.tableIdentifierToList(identifier),
                PolarisEntityType.TABLE_LIKE));
        op =
            target.getType() == TargetType.VIEW
                ? PolarisAuthorizableOperation.GET_OBJECT_TAGS_ON_VIEW
                : PolarisAuthorizableOperation.GET_OBJECT_TAGS_ON_TABLE;
        securable = PolarisSecurableMapper.tableLike(catalogName(), identifier);
      }
      default -> throw new IllegalStateException("Unsupported target type: " + target.getType());
    }

    AuthorizationState candidateState = new AuthorizationState(candidateManifest);
    AuthorizationRequest candidateRequest =
        new AuthorizationRequest(
            polarisPrincipal(), List.of(new SingleTargetAuthorizationIntent(op, securable)));
    authorizer().resolveAuthorizationInputs(candidateState, candidateRequest);
    ResolverStatus status = candidateManifest.getPrimaryResolverStatusOrThrow();
    if (status.getStatus() != ResolverStatus.StatusEnum.SUCCESS) {
      // The target is gone between the assignment read and this check: an orphan, hidden, not an
      // error. A missing catalog cannot reach here, because the definition resolved inside one.
      return false;
    }
    return authorizer().authorize(candidateState, candidateRequest).isAllowed();
  }

  private void authorizeTagAssignmentOperationOrThrow(
      String tagName, TagAttachmentTarget target, boolean isAssign) {
    if (target == null || target.getType() == null) {
      throw new BadRequestException("Assignment target is required");
    }
    resolutionManifest = newResolutionManifest();
    resolutionManifest.addPassthroughPath(
        new ResolverPath(List.of(tagName), PolarisEntityType.TAG, true /* optional */));

    switch (target.getType()) {
      case CATALOG -> {
        if (target.getPath() != null && !target.getPath().isEmpty()) {
          throw new BadRequestException("A catalog target must not carry a path");
        }
      }
      case NAMESPACE -> {
        if (target.getPath() == null || target.getPath().isEmpty()) {
          throw new BadRequestException("Namespace target path must not be empty");
        }
        requireValidPathMembers(target.getPath());
        Namespace targetNamespace = Namespace.of(target.getPath().toArray(new String[0]));
        resolutionManifest.addPath(
            new ResolverPath(Arrays.asList(targetNamespace.levels()), PolarisEntityType.NAMESPACE));
      }
      case TABLE, VIEW, COLUMN -> {
        if (target.getPath() == null || target.getPath().size() < 2) {
          throw new BadRequestException("Table-like target path must name a namespace and table");
        }
        requireValidPathMembers(target.getPath());
        TableIdentifier targetIdentifier =
            TableIdentifier.of(target.getPath().toArray(new String[0]));
        resolutionManifest.addPath(
            new ResolverPath(
                PolarisCatalogHelpers.tableIdentifierToList(targetIdentifier),
                PolarisEntityType.TABLE_LIKE));
      }
      default -> throw new BadRequestException("Unsupported target type: %s", target.getType());
    }

    PolarisAuthorizableOperation op = determineTagAssignmentOperation(target, isAssign);
    AuthorizationState authorizationState = new AuthorizationState(resolutionManifest);
    AuthorizationRequest authorizationRequest =
        new AuthorizationRequest(
            polarisPrincipal(),
            List.of(
                new TagAttachmentAuthorizationIntent(
                    op,
                    PolarisSecurableMapper.tag(catalogName(), tagName),
                    PolarisSecurableMapper.tagAttachmentTarget(catalogName(), target))));
    authorizer().resolveAuthorizationInputs(authorizationState, authorizationRequest);

    // A failed required path fails the whole manifest, so every getResolvedPath below would
    // return null and a missing target would surface as the tag-level 404. Classify the failed
    // path first, the way policy attachment does, so the response names the entity that is
    // actually missing. The tag path is registered optional and cannot fail the manifest, so a
    // failure here is always the target's.
    ResolverStatus status = resolutionManifest.getPrimaryResolverStatusOrThrow();
    throwIfCatalogMissing(status);
    if (status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED) {
      List<String> failedPath = status.getFailedToResolvePath().entityNames();
      switch (status.getFailedToResolvePath().lastEntityType()) {
        case NAMESPACE ->
            throw new NoSuchTargetException(
                "Namespace does not exist: %s", Namespace.of(failedPath.toArray(new String[0])));
        case TABLE_LIKE ->
            throw new NoSuchTargetException(
                "Table does not exist: %s", TableIdentifier.of(failedPath.toArray(new String[0])));
        default ->
            throw new IllegalStateException(
                "Unexpected unresolved path type: " + status.getFailedToResolvePath());
      }
    }

    PolarisResolvedPathWrapper tagWrapper =
        resolutionManifest.getResolvedPath(
            ResolvedPathKey.of(List.of(tagName), PolarisEntityType.TAG), true);
    if (tagWrapper == null) {
      throw new NoSuchTagException(String.format("Tag does not exist: %s", tagName));
    }

    // Fetched for its own 404 classification: a target that fails to resolve here throws
    // NoSuchTargetException before the authorizer ever sees it.
    TagCatalogUtils.getResolvedTargetWrapper(resolutionManifest, target);

    authorizer().authorize(authorizationState, authorizationRequest).throwIfDenied();

    initializeCatalog();
  }

  /**
   * Rejects a null, blank, or U+001F-bearing path member before it reaches an Iceberg identifier
   * constructor. Namespace.of and TableIdentifier.of treat a null member as a bug
   * (NullPointerException, mapped to a 500) rather than a malformed request, and U+001F is the
   * namespace level separator in query encoding, so a name carrying it could never round-trip.
   */
  private static void requireValidPathMembers(List<String> path) {
    for (String member : path) {
      if (member == null || member.isBlank() || member.indexOf('\u001F') >= 0) {
        throw new BadRequestException(
            "Target path must not contain a null, empty, or U+001F segment");
      }
    }
  }

  private PolarisAuthorizableOperation determineTagAssignmentOperation(
      TagAttachmentTarget target, boolean isAssign) {
    return switch (target.getType()) {
      case CATALOG ->
          isAssign
              ? PolarisAuthorizableOperation.ASSIGN_TAG_TO_CATALOG
              : PolarisAuthorizableOperation.UNASSIGN_TAG_FROM_CATALOG;
      case NAMESPACE ->
          isAssign
              ? PolarisAuthorizableOperation.ASSIGN_TAG_TO_NAMESPACE
              : PolarisAuthorizableOperation.UNASSIGN_TAG_FROM_NAMESPACE;
      // A column target is authorized against its containing table.
      case TABLE, COLUMN ->
          isAssign
              ? PolarisAuthorizableOperation.ASSIGN_TAG_TO_TABLE
              : PolarisAuthorizableOperation.UNASSIGN_TAG_FROM_TABLE;
      case VIEW ->
          isAssign
              ? PolarisAuthorizableOperation.ASSIGN_TAG_TO_VIEW
              : PolarisAuthorizableOperation.UNASSIGN_TAG_FROM_VIEW;
      default -> throw new BadRequestException("Unsupported target type: %s", target.getType());
    };
  }

  /**
   * The entity id the version token names, or null when the token is not one this server issued. An
   * unreadable token is not reported here: the normal path reports it, so that a request with a bad
   * token gets the same answer whether or not it also carries an idempotency key.
   */
  private @Nullable Long definitionIdOf(@Nullable String currentTagVersion) {
    if (currentTagVersion == null || currentTagVersion.isEmpty()) {
      return null;
    }
    try {
      return TagVersionToken.decode(currentTagVersion).definitionId();
    } catch (TagVersionMismatchException e) {
      return null;
    }
  }

  /** The name a definition holds now, read by id within this catalog, or null if it is gone. */
  private @Nullable String currentNameOf(long catalogId, long definitionId) {
    EntityResult result =
        metaStoreManager()
            .loadEntity(
                callContext().getPolarisCallContext(),
                catalogId,
                definitionId,
                PolarisEntityType.TAG);
    return result.isSuccess() && result.getEntity() != null ? result.getEntity().getName() : null;
  }

  /**
   * Resolves the catalog this request addresses and returns its id, so a definition can be read by
   * id within it. This pass resolves the catalog only and asks for no authorization decision: the
   * decision belongs to the name the definition turns out to hold, which is not known until after
   * this read.
   */
  private long resolveCatalogId() {
    resolutionManifest = newResolutionManifest();
    throwIfCatalogMissing(resolutionManifest.resolveAll());
    PolarisResolvedPathWrapper catalog = resolutionManifest.getResolvedReferenceCatalogEntity();
    initializeCatalog();
    return catalog.getRawLeafEntity().getId();
  }

  /**
   * Resolves what an authorization decision needs, and answers a prefix that names no catalog with
   * NoSuchCatalog rather than a server fault.
   *
   * <p>This resolves through the manifest instead of {@code
   * PolarisAuthorizer.resolveAuthorizationInputs}, whose whole body is the same {@code
   * resolutionManifest.resolveAll()} call with the returned status dropped. The status cannot be
   * recovered afterwards: a resolver may be run only once, and every getter on it requires a
   * successful run, so asking the manifest for the reference catalog after a failure raises a
   * server fault instead of returning null. The status is the only thing that names the entity that
   * was missing, which is what the contract needs to tell a missing catalog from a missing
   * definition inside one that resolves.
   */
  private void resolveOrThrowNoSuchCatalog() {
    throwIfCatalogMissing(resolutionManifest.resolveAll());
  }

  /** A missing or dropped reference catalog is the contract's NoSuchCatalog, on every operation. */
  private void throwIfCatalogMissing(ResolverStatus status) {
    if (status.getStatus() == ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED
        && status.getFailedToResolvedEntityType() == PolarisEntityType.CATALOG) {
      throw new NoSuchCatalogException(
          "Catalog not found: " + status.getFailedToResolvedEntityName());
    }
  }

  /**
   * Authorizes a rename on both sides, the way Polaris authorizes its table and view renames: the
   * definition being renamed carries the drop privilege, and the catalog that will hold the new
   * name carries the create privilege. Returns false, rather than reporting the definition missing,
   * when the caller asked not to require it, which is how the recognition path steps aside for the
   * ordinary one when a concurrent rename has moved the name it was about to authorize.
   */
  private boolean authorizeRenameTagOrThrow(
      PolarisAuthorizableOperation op, String tagName, boolean requireResolved) {
    resolutionManifest = newResolutionManifest();
    resolutionManifest.addPassthroughPath(
        new ResolverPath(List.of(tagName), PolarisEntityType.TAG, true /* optional */));
    AuthorizationRequest authorizationRequest =
        new AuthorizationRequest(
            polarisPrincipal(),
            List.of(
                new RenameAuthorizationIntent(
                    op,
                    PolarisSecurableMapper.tag(catalogName(), tagName),
                    PolarisSecurableMapper.catalog(catalogName()))));
    AuthorizationState authorizationState = new AuthorizationState(resolutionManifest);
    resolveOrThrowNoSuchCatalog();

    PolarisResolvedPathWrapper target =
        resolutionManifest.getResolvedPath(
            ResolvedPathKey.of(List.of(tagName), PolarisEntityType.TAG), true);
    if (target == null) {
      if (requireResolved) {
        throw new NoSuchTagException(String.format("Tag does not exist: %s", tagName));
      }
      return false;
    }

    authorizer().authorize(authorizationState, authorizationRequest).throwIfDenied();

    initializeCatalog();
    return true;
  }

  private void authorizeBasicTagOperationOrThrow(PolarisAuthorizableOperation op, String tagName) {
    resolutionManifest = newResolutionManifest();
    resolutionManifest.addPassthroughPath(
        new ResolverPath(List.of(tagName), PolarisEntityType.TAG, true /* optional */));
    AuthorizationRequest authorizationRequest =
        new AuthorizationRequest(
            polarisPrincipal(),
            List.of(
                new SingleTargetAuthorizationIntent(
                    op, PolarisSecurableMapper.tag(catalogName(), tagName))));
    AuthorizationState authorizationState = new AuthorizationState(resolutionManifest);
    resolveOrThrowNoSuchCatalog();

    // The resolved path decides existence before the decision is asked for, so a caller without
    // the privilege on a definition that does not exist still learns only that it does not exist.
    PolarisResolvedPathWrapper target =
        resolutionManifest.getResolvedPath(
            ResolvedPathKey.of(List.of(tagName), PolarisEntityType.TAG), true);
    if (target == null) {
      throw new NoSuchTagException(String.format("Tag does not exist: %s", tagName));
    }

    authorizer().authorize(authorizationState, authorizationRequest).throwIfDenied();

    initializeCatalog();
  }

  private void authorizeCatalogScopedTagOperationOrThrow(
      PolarisAuthorizableOperation op, @Nullable String tagName) {
    resolutionManifest = newResolutionManifest();
    if (tagName != null) {
      resolutionManifest.addPassthroughPath(
          new ResolverPath(List.of(tagName), PolarisEntityType.TAG, true /* optional */));
    }
    AuthorizationRequest authorizationRequest =
        new AuthorizationRequest(
            polarisPrincipal(),
            List.of(
                new SingleTargetAuthorizationIntent(
                    op, PolarisSecurableMapper.catalog(catalogName()))));
    AuthorizationState authorizationState = new AuthorizationState(resolutionManifest);
    resolveOrThrowNoSuchCatalog();

    authorizer().authorize(authorizationState, authorizationRequest).throwIfDenied();

    initializeCatalog();
  }
}
