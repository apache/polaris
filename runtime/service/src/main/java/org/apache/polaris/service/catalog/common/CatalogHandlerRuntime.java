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
package org.apache.polaris.service.catalog.common;

import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.immutables.value.Value;

/**
 * Immutable runtime configuration for {@link CatalogHandler}.
 *
 * <p>This interface encapsulates all the immutable dependencies that are shared across handler
 * instances. It separates the immutable "runtime" state from the per-request mutable state in the
 * handler itself.
 */
public interface CatalogHandlerRuntime {

  PolarisDiagnostics diagnostics();

  CallContext callContext();

  @Value.Derived
  default RealmConfig realmConfig() {
    return callContext().getRealmConfig();
  }

  @Value.Derived
  default RealmContext realmContext() {
    return callContext().getRealmContext();
  }

  PolarisMetaStoreManager metaStoreManager();

  ResolutionManifestFactory resolutionManifestFactory();

  PolarisAuthorizer authorizer();

  PolarisCredentialManager credentialManager();
}
