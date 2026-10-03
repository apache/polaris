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

import java.util.LinkedHashSet;
import java.util.List;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.RenameAuthorizationIntent;
import org.apache.polaris.core.auth.SingleTargetAuthorizationIntent;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfiguration;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.tag.TagValidation;
import org.apache.polaris.core.tag.TagVersionToken;
import org.apache.polaris.core.tag.exceptions.NoSuchCatalogException;
import org.apache.polaris.core.tag.exceptions.NoSuchTagException;
import org.apache.polaris.core.tag.exceptions.TagVersionMismatchException;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.catalog.common.PolarisSecurableMapper;
import org.apache.polaris.service.idempotency.IdempotencyRequestContext;
import org.apache.polaris.service.types.CreateTagRequest;
import org.apache.polaris.service.types.ListTagsResponse;
import org.apache.polaris.service.types.RenameTagRequest;
import org.apache.polaris.service.types.Tag;
import org.apache.polaris.service.types.TagIdentifier;
import org.apache.polaris.service.types.UpdateTagRequest;
import org.immutables.value.Value;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;

@PolarisImmutable
@SuppressWarnings("immutables:incompat")
public abstract class TagCatalogHandler extends CatalogHandler {

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

  @Override
  protected void initializeCatalog() {
    this.tagCatalog =
        new TagCatalog(
            metaStoreManager(),
            callContext(),
            this.resolutionManifest,
            idempotencyRequestContext());
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

    // A page size is an input, so it is validated on the way in rather than where it is used. The
    // shared helper validates only >= 0, and only while pagination is enabled, so a zero would
    // otherwise pass unremarked.
    if (pageSize != null && pageSize <= 0) {
      throw new BadRequestException("pageSize must be a positive integer");
    }

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

  public boolean dropTag(String tagName, boolean detachAll) {
    // detach-all removes every assignment of the definition and then the definition itself, so it
    // requires both TAG_DROP and TAG_DETACH on the definition, even while no assignment can exist
    // yet;
    // a plain drop requires only TAG_DROP.
    PolarisAuthorizableOperation op =
        detachAll
            ? PolarisAuthorizableOperation.DROP_TAG_DETACH_ALL
            : PolarisAuthorizableOperation.DROP_TAG;
    authorizeBasicTagOperationOrThrow(op, tagName);

    // detach-all promises that the definition and every assignment of it are gone together. No
    // assignment can exist yet, so that promise is already kept by deleting the definition and the
    // parameter changes nothing here. The assignment change adds the cleanup, and the separate
    // privilege the wider operation needs, at the same time.
    return tagCatalog.dropTag(tagName);
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
