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

import jakarta.annotation.PostConstruct;
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

/**
 * Factory for creating {@link GenericTableCatalogHandler} instances.
 *
 * <p>This factory holds all the shared dependencies needed to create handler instances, allowing
 * callers to create handlers by providing only the per-request values (principal and catalog name).
 */
@RequestScoped
public class GenericTableCatalogHandlerFactory {

  @Inject PolarisDiagnostics diagnostics;
  @Inject CallContext callContext;
  @Inject ResolutionManifestFactory resolutionManifestFactory;
  @Inject PolarisMetaStoreManager metaStoreManager;
  @Inject PolarisAuthorizer authorizer;
  @Inject PolarisCredentialManager credentialManager;
  @Inject @Any Instance<ExternalCatalogFactory> externalCatalogFactories;

  private GenericTableCatalogHandlerRuntime runtime;

  @PostConstruct
  public void init() {
    runtime =
        GenericTableCatalogHandlerRuntime.builder()
            .diagnostics(diagnostics)
            .callContext(callContext)
            .resolutionManifestFactory(resolutionManifestFactory)
            .metaStoreManager(metaStoreManager)
            .authorizer(authorizer)
            .credentialManager(credentialManager)
            .externalCatalogFactories(externalCatalogFactories)
            .build();
  }

  /**
   * Creates a new {@link GenericTableCatalogHandler} instance for the given principal and catalog.
   */
  public GenericTableCatalogHandler createHandler(String catalogName, PolarisPrincipal principal) {
    return new GenericTableCatalogHandler(catalogName, principal, runtime);
  }
}
