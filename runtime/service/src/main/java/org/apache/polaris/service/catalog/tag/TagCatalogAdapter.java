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
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.api.PolarisCatalogTagApiService;
import org.apache.polaris.service.catalog.common.CatalogAdapter;
import org.apache.polaris.service.types.CreateTagRequest;
import org.apache.polaris.service.types.ListTagsResponse;
import org.apache.polaris.service.types.LoadTagResponse;
import org.apache.polaris.service.types.UpdateTagRequest;

@RequestScoped
public class TagCatalogAdapter implements PolarisCatalogTagApiService, CatalogAdapter {

  private final CatalogPrefixParser prefixParser;
  private final TagCatalogHandlerFactory handlerFactory;
  private final RealmConfig realmConfig;

  @Inject
  public TagCatalogAdapter(
      CatalogPrefixParser prefixParser,
      TagCatalogHandlerFactory handlerFactory,
      RealmConfig realmConfig) {
    this.prefixParser = prefixParser;
    this.handlerFactory = handlerFactory;
    this.realmConfig = realmConfig;
  }

  private TagCatalogHandler newHandler(SecurityContext securityContext, String prefix) {
    FeatureConfiguration.enforceFeatureEnabledOrThrow(
        realmConfig, FeatureConfiguration.ENABLE_TAG_STORE);
    PolarisPrincipal principal = validatePrincipal(securityContext);
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    return handlerFactory.createHandler(catalogName, principal);
  }

  @Override
  public Response createTag(
      String prefix,
      CreateTagRequest createTagRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    TagCatalogHandler handler = newHandler(securityContext, prefix);
    LoadTagResponse response = handler.createTag(createTagRequest);
    return Response.ok(response).build();
  }

  @Override
  public Response listTags(
      String prefix,
      String pageToken,
      Integer pageSize,
      RealmContext realmContext,
      SecurityContext securityContext) {
    TagCatalogHandler handler = newHandler(securityContext, prefix);
    ListTagsResponse response = handler.listTags(pageToken, pageSize);
    return Response.ok(response).build();
  }

  @Override
  public Response loadTag(
      String prefix, String tagName, RealmContext realmContext, SecurityContext securityContext) {
    TagCatalogHandler handler = newHandler(securityContext, prefix);
    LoadTagResponse response = handler.loadTag(tagName);
    return Response.ok(response).build();
  }

  @Override
  public Response updateTag(
      String prefix,
      String tagName,
      UpdateTagRequest updateTagRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    TagCatalogHandler handler = newHandler(securityContext, prefix);
    LoadTagResponse response = handler.updateTag(tagName, updateTagRequest);
    return Response.ok(response).build();
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

  // assignTag, unassignTag, getObjectTags, and listObjectsByTag are not implemented yet; the
  // default PolarisCatalogTagApiService methods return 501.
}
