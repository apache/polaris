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

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashSet;
import java.util.List;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.SingleTargetAuthorizationIntent;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.tag.exceptions.NoSuchTagException;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.catalog.common.PolarisSecurableMapper;
import org.apache.polaris.service.types.CreateTagRequest;
import org.apache.polaris.service.types.ListTagsResponse;
import org.apache.polaris.service.types.LoadTagResponse;
import org.apache.polaris.service.types.TagIdentifier;
import org.apache.polaris.service.types.UpdateTagRequest;
import org.jspecify.annotations.Nullable;

@PolarisImmutable
@SuppressWarnings("immutables:incompat")
public abstract class TagCatalogHandler extends CatalogHandler {

  private TagCatalog tagCatalog;

  @Override
  protected void initializeCatalog() {
    this.tagCatalog = new TagCatalog(metaStoreManager(), callContext(), this.resolutionManifest);
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
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_TAG;
    authorizeBasicTagOperationOrThrow(op, tagName);

    if (detachAll) {
      // Runs after authorization: the parameter is documented, its implementation arrives with
      // tag assignments.
      throw new WebApplicationException(
          "detach-all is implemented together with tag assignments",
          Response.Status.NOT_IMPLEMENTED);
    }
    return tagCatalog.dropTag(tagName);
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
