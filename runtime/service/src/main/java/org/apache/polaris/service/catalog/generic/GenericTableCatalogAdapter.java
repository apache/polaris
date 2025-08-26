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
package org.apache.polaris.service.catalog.generic;

import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.service.admin.MainService;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.api.PolarisCatalogGenericTableApiService;
import org.apache.polaris.service.catalog.common.CatalogAdapter;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.types.CreateGenericTableRequest;
import org.apache.polaris.service.types.ListGenericTablesResponse;
import org.apache.polaris.service.types.LoadGenericTableResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
@MainService
public class GenericTableCatalogAdapter
    implements PolarisCatalogGenericTableApiService, CatalogAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenericTableCatalogAdapter.class);

  private final RealmContext realmContext;
  private final RealmConfig realmConfig;
  private final CallContext callContext;
  private final ResolutionManifestFactory resolutionManifestFactory;
  private final PolarisMetaStoreManager metaStoreManager;
  private final PolarisAuthorizer polarisAuthorizer;
  private final ReservedProperties reservedProperties;
  private final CatalogPrefixParser prefixParser;
  private final UserSecretsManager userSecretsManager;
  private final Instance<ExternalCatalogFactory> externalCatalogFactories;

  @Inject
  public GenericTableCatalogAdapter(
      RealmContext realmContext,
      CallContext callContext,
      ResolutionManifestFactory resolutionManifestFactory,
      PolarisMetaStoreManager metaStoreManager,
      PolarisAuthorizer polarisAuthorizer,
      CatalogPrefixParser prefixParser,
      ReservedProperties reservedProperties,
      UserSecretsManager userSecretsManager,
      @Any Instance<ExternalCatalogFactory> externalCatalogFactories) {
    this.realmContext = realmContext;
    this.callContext = callContext;
    this.realmConfig = callContext.getRealmConfig();
    this.resolutionManifestFactory = resolutionManifestFactory;
    this.metaStoreManager = metaStoreManager;
    this.polarisAuthorizer = polarisAuthorizer;
    this.prefixParser = prefixParser;
    this.reservedProperties = reservedProperties;
    this.userSecretsManager = userSecretsManager;
    this.externalCatalogFactories = externalCatalogFactories;
  }

  private GenericTableCatalogHandler newHandlerWrapper(
      SecurityContext securityContext, String prefix) {
    FeatureConfiguration.enforceFeatureEnabledOrThrow(
        realmConfig, FeatureConfiguration.ENABLE_GENERIC_TABLES);
    validatePrincipal(securityContext);

    return new GenericTableCatalogHandler(
        callContext,
        resolutionManifestFactory,
        metaStoreManager,
        securityContext,
        prefixParser.prefixToCatalogName(realmContext, prefix),
        polarisAuthorizer,
        userSecretsManager,
        externalCatalogFactories);
  }

  @Override
  public Response createGenericTable(
      String prefix,
      String namespace,
      CreateGenericTableRequest createGenericTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    GenericTableCatalogHandler handler = newHandlerWrapper(securityContext, prefix);
    LoadGenericTableResponse response =
        handler.createGenericTable(
            TableIdentifier.of(decodeNamespace(namespace), createGenericTableRequest.getName()),
            createGenericTableRequest.getFormat(),
            createGenericTableRequest.getBaseLocation(),
            createGenericTableRequest.getDoc(),
            reservedProperties.removeReservedProperties(createGenericTableRequest.getProperties()));

    return Response.ok(response).build();
  }

  @Override
  public Response dropGenericTable(
      String prefix,
      String namespace,
      String genericTable,
      RealmContext realmContext,
      SecurityContext securityContext) {
    GenericTableCatalogHandler handler = newHandlerWrapper(securityContext, prefix);
    handler.dropGenericTable(TableIdentifier.of(decodeNamespace(namespace), genericTable));
    return Response.noContent().build();
  }

  @Override
  public Response listGenericTables(
      String prefix,
      String namespace,
      String pageToken,
      Integer pageSize,
      RealmContext realmContext,
      SecurityContext securityContext) {
    GenericTableCatalogHandler handler = newHandlerWrapper(securityContext, prefix);
    ListGenericTablesResponse response = handler.listGenericTables(decodeNamespace(namespace));
    return Response.ok(response).build();
  }

  @Override
  public Response loadGenericTable(
      String prefix,
      String namespace,
      String genericTable,
      RealmContext realmContext,
      SecurityContext securityContext) {
    GenericTableCatalogHandler handler = newHandlerWrapper(securityContext, prefix);
    LoadGenericTableResponse response =
        handler.loadGenericTable(TableIdentifier.of(decodeNamespace(namespace), genericTable));
    return Response.ok(response).build();
  }
}
