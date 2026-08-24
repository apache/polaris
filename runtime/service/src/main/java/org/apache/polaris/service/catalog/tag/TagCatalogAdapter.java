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

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.UriInfo;
import java.util.List;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.api.PolarisCatalogTagApiService;
import org.apache.polaris.service.catalog.common.CatalogAdapter;
import org.apache.polaris.service.types.AssignTagRequest;
import org.apache.polaris.service.types.CreateTagRequest;
import org.apache.polaris.service.types.ListTagsResponse;
import org.apache.polaris.service.types.RenameTagRequest;
import org.apache.polaris.service.types.Tag;
import org.apache.polaris.service.types.TagAttachmentTarget;
import org.apache.polaris.service.types.TargetType;
import org.apache.polaris.service.types.UpdateTagRequest;

@RequestScoped
public class TagCatalogAdapter implements PolarisCatalogTagApiService, CatalogAdapter {

  /**
   * Carries the query string as the request sent it. The bound pagination parameters cannot answer
   * what the contract asks of them: {@code pagination} is valid only as the literal {@code true} or
   * {@code false}, yet the binding converts any other value to a boolean without complaint, and a
   * full-result request must reject a {@code pageToken} or {@code pageSize} that is present even
   * when it is empty, which the binding hands over as null. The query map keeps both differences,
   * so {@link #requestedPagedMode} reads it from here.
   */
  @Context UriInfo uriInfo;

  private final CatalogPrefixParser prefixParser;
  private final TagCatalogHandlerFactory handlerFactory;
  private final RealmConfig realmConfig;
  private final PolarisMetaStoreManager metaStoreManager;

  @Inject
  public TagCatalogAdapter(
      CatalogPrefixParser prefixParser,
      TagCatalogHandlerFactory handlerFactory,
      RealmConfig realmConfig,
      PolarisMetaStoreManager metaStoreManager) {
    this.prefixParser = prefixParser;
    this.handlerFactory = handlerFactory;
    this.realmConfig = realmConfig;
    this.metaStoreManager = metaStoreManager;
  }

  private TagCatalogHandler newHandler(SecurityContext securityContext, String prefix) {
    FeatureConfiguration.enforceFeatureEnabledOrThrow(
        realmConfig, FeatureConfiguration.ENABLE_TAG_STORE);
    if (!metaStoreManager.supportsEntityType(PolarisEntityType.TAG)) {
      // The flag is a realm setting and cannot see which metastore is configured, so an operator
      // can enable tags on a metastore that has no storage for them. Refuse here, before
      // anything is resolved, instead of letting the request fail inside persistence.
      throw new UnsupportedOperationException("Tags are not supported by the configured metastore");
    }
    PolarisPrincipal principal = validatePrincipal(securityContext);
    // The prefix identifies the catalog, the parser decides how that segment maps to one, and its
    // default mapping is identity. Renaming the placeholder changes no mapping, so the parser
    // stays.
    String resolvedCatalogName = prefixParser.prefixToCatalogName(prefix);
    return handlerFactory.createHandler(resolvedCatalogName, principal);
  }

  /**
   * The generated signatures carry the {@code Idempotency-Key} value because the operations declare
   * that header, but the handler reads it from the request-scoped idempotency context instead,
   * which is where the shared filter puts it after validating it and where its expiry is computed.
   * Taking it from two places would let them disagree.
   */
  @Override
  public Response createTag(
      String prefix,
      CreateTagRequest createTagRequest,
      String idempotencyKey,
      RealmContext realmContext,
      SecurityContext securityContext) {
    TagCatalogHandler handler = newHandler(securityContext, prefix);
    Tag response = handler.createTag(createTagRequest);
    return Response.ok(response).build();
  }

  /**
   * The bound {@code pagination} value is unused for the same kind of reason {@code idempotencyKey}
   * is: it cannot carry the answer. The parameter declares a default, so an absent flag arrives as
   * {@code TRUE} and cannot be told from an explicit one, and the conversion to a boolean never
   * fails, so an empty or misspelled value arrives as {@code FALSE}, which is a request for the
   * whole collection. Only the two literals are valid, and only the raw query value says which one
   * the client sent.
   */
  @Override
  public Response listTags(
      String prefix,
      Boolean pagination,
      String pageToken,
      Integer pageSize,
      RealmContext realmContext,
      SecurityContext securityContext) {
    TagCatalogHandler handler = newHandler(securityContext, prefix);
    rejectRepeatedQueryParameters("pagination", "pageToken", "pageSize");
    boolean paged = requestedPagedMode();
    rejectUnusablePageSize(pageSize);
    ListTagsResponse response = handler.listTags(paged, pageToken, pageSize);
    return Response.ok(response).build();
  }

  /**
   * Refuses a page size the request sent but the binding could not use. An empty value, and a value
   * that is not a number, arrive as null, which by the time it reaches the handler is
   * indistinguishable from a parameter that was never sent: the request would be answered as though
   * it had asked for no particular size. The contract makes such a value invalid input rather than
   * a missing one, so presence decides this here, where presence is still visible.
   */
  private void rejectUnusablePageSize(Integer boundPageSize) {
    if (boundPageSize == null && uriInfo.getQueryParameters().containsKey("pageSize")) {
      throw new BadRequestException("Query parameter pageSize must be a positive integer");
    }
  }

  /**
   * Refuses a query parameter that was sent more than once. Such a request has no single value to
   * act on, and picking one silently would answer a question the client did not ask.
   */
  private void rejectRepeatedQueryParameters(String... names) {
    MultivaluedMap<String, String> query = uriInfo.getQueryParameters();
    for (String name : names) {
      List<String> values = query.get(name);
      if (values != null && values.size() > 1) {
        throw new BadRequestException("Query parameter %s was supplied more than once", name);
      }
    }
  }

  /**
   * Decides which mode the request asked for. Paging is the default, so a request that sends no
   * flag is a paged one, and an omitted or empty page token asks for its first page. {@code
   * pagination=false} asks for the complete result instead, and has to arrive alone: a page token
   * or a page size says nothing inside a request that is not paged, so carrying one is a
   * contradiction rather than a hint. Presence is read from the query map, which is what lets an
   * empty value of either count as present.
   */
  private boolean requestedPagedMode() {
    MultivaluedMap<String, String> query = uriInfo.getQueryParameters();
    if (!query.containsKey("pagination")) {
      return true;
    }
    String requested = query.getFirst("pagination");
    if ("true".equals(requested)) {
      return true;
    }
    if (!"false".equals(requested)) {
      throw new BadRequestException("Query parameter pagination accepts only true or false");
    }
    if (query.containsKey("pageToken") || query.containsKey("pageSize")) {
      throw new BadRequestException(
          "Query parameter pagination=false cannot be combined with pageToken or pageSize");
    }
    return false;
  }

  @Override
  public Response loadTag(
      String prefix, String tagName, RealmContext realmContext, SecurityContext securityContext) {
    TagCatalogHandler handler = newHandler(securityContext, prefix);
    Tag response = handler.loadTag(tagName);
    return Response.ok(response).build();
  }

  @Override
  public Response updateTag(
      String prefix,
      String tagName,
      UpdateTagRequest updateTagRequest,
      String idempotencyKey,
      RealmContext realmContext,
      SecurityContext securityContext) {
    TagCatalogHandler handler = newHandler(securityContext, prefix);
    Tag response = handler.updateTag(tagName, updateTagRequest);
    return Response.ok(response).build();
  }

  @Override
  public Response renameTag(
      String prefix,
      RenameTagRequest renameTagRequest,
      String idempotencyKey,
      RealmContext realmContext,
      SecurityContext securityContext) {
    TagCatalogHandler handler = newHandler(securityContext, prefix);
    handler.renameTag(renameTagRequest);
    return Response.noContent().build();
  }

  @Override
  public Response dropTag(
      String prefix,
      String tagName,
      Boolean detachAll,
      RealmContext realmContext,
      SecurityContext securityContext) {
    TagCatalogHandler handler = newHandler(securityContext, prefix);
    handler.dropTag(tagName, detachAll != null && detachAll);
    return Response.noContent().build();
  }

  @Override
  public Response assignTag(
      String prefix,
      String tagName,
      TargetType targetType,
      AssignTagRequest assignTagRequest,
      String namespace,
      String targetName,
      String column,
      RealmContext realmContext,
      SecurityContext securityContext) {
    TagCatalogHandler handler = newHandler(securityContext, prefix);
    rejectRepeatedQueryParameters("target-type", "namespace", "target-name", "column");
    TagAttachmentTarget target =
        TagCatalogUtils.targetFromQuery(targetType, namespace, targetName, column);
    handler.assignTag(tagName, target, assignTagRequest.getValues());
    return Response.noContent().build();
  }

  @Override
  public Response unassignTag(
      String prefix,
      String tagName,
      TargetType targetType,
      String namespace,
      String targetName,
      String column,
      RealmContext realmContext,
      SecurityContext securityContext) {
    TagCatalogHandler handler = newHandler(securityContext, prefix);
    rejectRepeatedQueryParameters("target-type", "namespace", "target-name", "column");
    TagAttachmentTarget target =
        TagCatalogUtils.targetFromQuery(targetType, namespace, targetName, column);
    handler.unassignTag(tagName, target);
    return Response.noContent().build();
  }

  // getObjectTags and listObjectsByTag are not implemented yet; the default
  // PolarisCatalogTagApiService methods return 501.
}
