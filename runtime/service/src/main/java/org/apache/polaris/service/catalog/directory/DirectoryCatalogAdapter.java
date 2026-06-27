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
package org.apache.polaris.service.catalog.directory;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.rest.NamespaceUtils;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.api.PolarisCatalogDirectoryApiService;
import org.apache.polaris.service.catalog.common.CatalogAdapter;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.types.CreateDirectoryRequest;
import org.apache.polaris.service.types.DirectoryFilter;
import org.apache.polaris.service.types.ListDirectoriesResponse;
import org.apache.polaris.service.types.LoadDirectoryResponse;

@RequestScoped
public class DirectoryCatalogAdapter implements PolarisCatalogDirectoryApiService, CatalogAdapter {

  private final RealmConfig realmConfig;
  private final ReservedProperties reservedProperties;
  private final CatalogPrefixParser prefixParser;
  private final DirectoryCatalogHandlerFactory handlerFactory;

  @Inject
  public DirectoryCatalogAdapter(
      CallContext callContext,
      CatalogPrefixParser prefixParser,
      ReservedProperties reservedProperties,
      DirectoryCatalogHandlerFactory handlerFactory) {
    this.realmConfig = callContext.getRealmConfig();
    this.prefixParser = prefixParser;
    this.reservedProperties = reservedProperties;
    this.handlerFactory = handlerFactory;
  }

  private DirectoryCatalogHandler newHandler(SecurityContext securityContext, String prefix) {
    FeatureConfiguration.enforceFeatureEnabledOrThrow(
        realmConfig, FeatureConfiguration.ENABLE_DIRECTORIES);
    PolarisPrincipal principal = validatePrincipal(securityContext);
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    return handlerFactory.createHandler(catalogName, principal);
  }

  @Override
  public Response createDirectory(
      String prefix,
      String namespace,
      CreateDirectoryRequest createDirectoryRequest,
      String polarisDirectoryAccessDelegation,
      RealmContext realmContext,
      SecurityContext securityContext) {
    DirectoryCatalogHandler handler = newHandler(securityContext, prefix);

    DirectoryFilter filter = createDirectoryRequest.getFilter();
    String filterInclude =
        filter != null ? DirectoryCatalogHandler.toJsonArray(filter.getInclude()) : null;
    String filterExclude =
        filter != null ? DirectoryCatalogHandler.toJsonArray(filter.getExclude()) : null;

    LoadDirectoryResponse response =
        handler.createDirectory(
            TableIdentifier.of(NamespaceUtils.splitNamespace(namespace, NamespaceUtils.DEFAULT_NAMESPACE_SEPARATOR), createDirectoryRequest.getName()),
            createDirectoryRequest.getBaseLocation(),
            filterInclude,
            filterExclude,
            DirectoryCatalogHandler.scanScheduleToJson(createDirectoryRequest.getScanSchedule()),
            reservedProperties.removeReservedProperties(createDirectoryRequest.getProperties()));

    return Response.ok(response).build();
  }

  @Override
  public Response dropDirectory(
      String prefix,
      String namespace,
      String directory,
      RealmContext realmContext,
      SecurityContext securityContext) {
    DirectoryCatalogHandler handler = newHandler(securityContext, prefix);
    handler.dropDirectory(TableIdentifier.of(NamespaceUtils.splitNamespace(namespace, NamespaceUtils.DEFAULT_NAMESPACE_SEPARATOR), directory));
    return Response.noContent().build();
  }

  @Override
  public Response listDirectories(
      String prefix,
      String namespace,
      String pageToken,
      Integer pageSize,
      RealmContext realmContext,
      SecurityContext securityContext) {
    DirectoryCatalogHandler handler = newHandler(securityContext, prefix);
    ListDirectoriesResponse response = handler.listDirectories(NamespaceUtils.splitNamespace(namespace, NamespaceUtils.DEFAULT_NAMESPACE_SEPARATOR));
    return Response.ok(response).build();
  }

  @Override
  public Response loadDirectory(
      String prefix,
      String namespace,
      String directory,
      String polarisDirectoryAccessDelegation,
      RealmContext realmContext,
      SecurityContext securityContext) {
    DirectoryCatalogHandler handler = newHandler(securityContext, prefix);
    LoadDirectoryResponse response =
        handler.loadDirectory(TableIdentifier.of(NamespaceUtils.splitNamespace(namespace, NamespaceUtils.DEFAULT_NAMESPACE_SEPARATOR), directory));
    return Response.ok(response).build();
  }
}
