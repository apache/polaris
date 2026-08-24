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

import static org.apache.polaris.core.config.FeatureConfiguration.LIST_PAGINATION_ENABLED;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.SingleTargetAuthorizationIntent;
import org.apache.polaris.core.auth.TagAttachmentAuthorizationIntent;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.tag.exceptions.NoSuchTagException;
import org.apache.polaris.core.tag.exceptions.NoSuchTargetException;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.catalog.common.PolarisSecurableMapper;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.types.AssignTagRequest;
import org.apache.polaris.service.types.CreateTagRequest;
import org.apache.polaris.service.types.ListTagsResponse;
import org.apache.polaris.service.types.LoadTagResponse;
import org.apache.polaris.service.types.TagAttachmentTarget;
import org.apache.polaris.service.types.TagIdentifier;
import org.apache.polaris.service.types.UnassignTagRequest;
import org.apache.polaris.service.types.UpdateTagRequest;
import org.jspecify.annotations.Nullable;

@PolarisImmutable
@SuppressWarnings("immutables:incompat")
public abstract class TagCatalogHandler extends CatalogHandler {

  private TagCatalog tagCatalog;

  protected abstract StorageAccessConfigProvider storageAccessConfigProvider();

  protected abstract FileIOFactory fileIOFactory();

  @Override
  protected void initializeCatalog() {
    this.tagCatalog =
        new TagCatalog(
            metaStoreManager(),
            callContext(),
            this.resolutionManifest,
            storageAccessConfigProvider(),
            fileIOFactory(),
            realmConfig());
  }

  public LoadTagResponse createTag(CreateTagRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_TAG;
    // The tag itself does not exist yet; the optional passthrough path lets the catalog detect a
    // name collision, the same way policy creation passes the not-yet-existing policy through.
    authorizeCatalogScopedTagOperationOrThrow(op, request.getName());

    return LoadTagResponse.builder()
        .setTag(
            tagCatalog.createTag(
                request.getName(),
                request.getComment(),
                request.getAllowedValues(),
                request.getTargetTypes()))
        .build();
  }

  public ListTagsResponse listTags(@Nullable String pageToken, @Nullable Integer pageSize) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_TAG;
    authorizeCatalogScopedTagOperationOrThrow(op, null);

    PageToken pageRequest = PageToken.build(pageToken, pageSize, this::shouldDecodeToken);
    Page<TagIdentifier> page = tagCatalog.listTags(pageRequest);
    return ListTagsResponse.builder()
        .setIdentifiers(new LinkedHashSet<>(page.items()))
        .setNextPageToken(page.encodedResponseToken())
        .build();
  }

  public LoadTagResponse loadTag(String tagName) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_TAG;
    authorizeBasicTagOperationOrThrow(op, tagName);

    return LoadTagResponse.builder().setTag(tagCatalog.loadTag(tagName)).build();
  }

  public LoadTagResponse updateTag(String tagName, UpdateTagRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_TAG;
    authorizeBasicTagOperationOrThrow(op, tagName);

    return LoadTagResponse.builder().setTag(tagCatalog.updateTag(tagName, request)).build();
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

  public void assignTag(String tagName, AssignTagRequest request) {
    authorizeTagAssignmentOperationOrThrow(tagName, request.getTarget(), true);
    tagCatalog.assignTag(tagName, request.getTarget(), request.getValues());
  }

  public void unassignTag(String tagName, UnassignTagRequest request) {
    authorizeTagAssignmentOperationOrThrow(tagName, request.getTarget(), false);
    tagCatalog.unassignTag(tagName, request.getTarget());
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
      case TABLE_LIKE, COLUMN -> {
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
    authorizer()
        .resolveAuthorizationInputs(
            authorizationState,
            new AuthorizationRequest(
                polarisPrincipal(),
                List.of(
                    new TagAttachmentAuthorizationIntent(
                        op,
                        PolarisSecurableMapper.tag(catalogName(), tagName),
                        PolarisSecurableMapper.tagAttachmentTarget(catalogName(), target)))));

    // A failed required path fails the whole manifest, so every getResolvedPath below would
    // return null and a missing target would surface as the tag-level 404. Classify the failed
    // path first, the way policy attachment does, so the response names the entity that is
    // actually missing. The tag path is registered optional and cannot fail the manifest, so a
    // failure here is always the target's.
    ResolverStatus status = resolutionManifest.getPrimaryResolverStatusOrThrow();
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

    PolarisResolvedPathWrapper targetWrapper =
        TagCatalogUtils.getResolvedTargetWrapper(resolutionManifest, target);

    authorizer()
        .authorizeOrThrow(
            polarisPrincipal(),
            resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
            op,
            tagWrapper,
            targetWrapper);

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
      case TABLE_LIKE, COLUMN ->
          isAssign
              ? PolarisAuthorizableOperation.ASSIGN_TAG_TO_TABLE
              : PolarisAuthorizableOperation.UNASSIGN_TAG_FROM_TABLE;
      default -> throw new BadRequestException("Unsupported target type: %s", target.getType());
    };
  }

  private boolean shouldDecodeToken() {
    return realmConfig()
        .getConfig(LIST_PAGINATION_ENABLED, resolutionManifest.getResolvedCatalogEntity());
  }

  private void authorizeBasicTagOperationOrThrow(PolarisAuthorizableOperation op, String tagName) {
    resolutionManifest = newResolutionManifest();
    resolutionManifest.addPassthroughPath(
        new ResolverPath(List.of(tagName), PolarisEntityType.TAG, true /* optional */));
    AuthorizationState authorizationState = new AuthorizationState(resolutionManifest);
    authorizer()
        .resolveAuthorizationInputs(
            authorizationState,
            new AuthorizationRequest(
                polarisPrincipal(),
                List.of(
                    new SingleTargetAuthorizationIntent(
                        op, PolarisSecurableMapper.tag(catalogName(), tagName)))));

    PolarisResolvedPathWrapper target =
        resolutionManifest.getResolvedPath(
            ResolvedPathKey.of(List.of(tagName), PolarisEntityType.TAG), true);
    if (target == null) {
      throw new NoSuchTagException(String.format("Tag does not exist: %s", tagName));
    }

    authorizer()
        .authorizeOrThrow(
            polarisPrincipal(),
            resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
            op,
            target,
            null /* secondary */);

    initializeCatalog();
  }

  private void authorizeCatalogScopedTagOperationOrThrow(
      PolarisAuthorizableOperation op, @Nullable String tagName) {
    resolutionManifest = newResolutionManifest();
    if (tagName != null) {
      resolutionManifest.addPassthroughPath(
          new ResolverPath(List.of(tagName), PolarisEntityType.TAG, true /* optional */));
    }
    AuthorizationState authorizationState = new AuthorizationState(resolutionManifest);
    authorizer()
        .resolveAuthorizationInputs(
            authorizationState,
            new AuthorizationRequest(
                polarisPrincipal(),
                List.of(
                    new SingleTargetAuthorizationIntent(
                        op, PolarisSecurableMapper.catalog(catalogName())))));

    PolarisResolvedPathWrapper targetCatalog =
        resolutionManifest.getResolvedReferenceCatalogEntity();
    if (targetCatalog == null) {
      throw new NotFoundException("Catalog not found");
    }
    authorizer()
        .authorizeOrThrow(
            polarisPrincipal(),
            resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
            op,
            targetCatalog,
            null /* secondary */);

    initializeCatalog();
  }
}
