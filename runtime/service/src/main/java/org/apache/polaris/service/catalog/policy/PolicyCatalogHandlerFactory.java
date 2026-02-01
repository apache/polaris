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
package org.apache.polaris.service.catalog.policy;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;

/**
 * Factory for creating {@link PolicyCatalogHandler} instances.
 *
 * <p>This factory holds all the shared dependencies needed to create handler instances, allowing
 * callers to create handlers by providing only the per-request values (principal and catalog name).
 */
@RequestScoped
public class PolicyCatalogHandlerFactory {

  @Inject PolarisDiagnostics diagnostics;
  @Inject CallContext callContext;
  @Inject ResolutionManifestFactory resolutionManifestFactory;
  @Inject PolarisMetaStoreManager metaStoreManager;
  @Inject PolarisAuthorizer authorizer;
  @Inject PolarisCredentialManager credentialManager;

  private PolicyCatalogHandlerRuntime runtime;

  @PostConstruct
  public void init() {
    runtime =
        PolicyCatalogHandlerRuntime.builder()
            .diagnostics(diagnostics)
            .callContext(callContext)
            .resolutionManifestFactory(resolutionManifestFactory)
            .metaStoreManager(metaStoreManager)
            .authorizer(authorizer)
            .credentialManager(credentialManager)
            .build();
  }

  /** Creates a new {@link PolicyCatalogHandler} instance for the given principal and catalog. */
  public PolicyCatalogHandler createHandler(String catalogName, PolarisPrincipal principal) {
    return new PolicyCatalogHandler(catalogName, principal, runtime);
  }
}
