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
package org.apache.polaris.service.catalog.iceberg;

import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.context.catalog.CallContextCatalogFactory;
import org.apache.polaris.service.events.EventAttributeMap;

@RequestScoped
public class IcebergCatalogHandlerFactory {

  @Inject PolarisDiagnostics diagnostics;
  @Inject CallContext callContext;
  @Inject CatalogPrefixParser prefixParser;
  @Inject ResolverFactory resolverFactory;
  @Inject ResolutionManifestFactory resolutionManifestFactory;
  @Inject PolarisMetaStoreManager metaStoreManager;
  @Inject PolarisCredentialManager credentialManager;
  @Inject CallContextCatalogFactory catalogFactory;
  @Inject PolarisAuthorizer authorizer;
  @Inject ReservedProperties reservedProperties;
  @Inject CatalogHandlerUtils catalogHandlerUtils;
  @Inject @Any Instance<ExternalCatalogFactory> externalCatalogFactories;
  @Inject StorageAccessConfigProvider storageAccessConfigProvider;
  @Inject EventAttributeMap eventAttributeMap;

  public IcebergCatalogHandler createHandler(String catalogName, PolarisPrincipal principal) {
    return ImmutableIcebergCatalogHandler.builder()
        .catalogName(catalogName)
        .polarisPrincipal(principal)
        .diagnostics(diagnostics)
        .callContext(callContext)
        .prefixParser(prefixParser)
        .resolverFactory(resolverFactory)
        .resolutionManifestFactory(resolutionManifestFactory)
        .metaStoreManager(metaStoreManager)
        .credentialManager(credentialManager)
        .catalogFactory(catalogFactory)
        .authorizer(authorizer)
        .reservedProperties(reservedProperties)
        .catalogHandlerUtils(catalogHandlerUtils)
        .externalCatalogFactories(externalCatalogFactories)
        .storageAccessConfigProvider(storageAccessConfigProvider)
        .eventAttributeMap(eventAttributeMap)
        .build();
  }
}
