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
package org.apache.polaris.service.admin;

import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.service.config.ReservedProperties;

public final class PolarisAdminServiceTestSupport {
  private PolarisAdminServiceTestSupport() {}

  public static PolarisAdminService newAdminService(
      CallContext callContext,
      ResolutionManifestFactory resolutionManifestFactory,
      PolarisMetaStoreManager metaStoreManager,
      UserSecretsManager userSecretsManager,
      ServiceIdentityProvider serviceIdentityProvider,
      PolarisPrincipal principal,
      PolarisAuthorizer authorizer,
      ReservedProperties reservedProperties) {
    // Mirror request-scoped service behavior: each test service gets a fresh mutable
    // AuthorizationState so one admin call cannot retain resolution state for the next call.
    return new PolarisAdminService(
        callContext,
        resolutionManifestFactory,
        metaStoreManager,
        new AuthorizationState(),
        userSecretsManager,
        serviceIdentityProvider,
        principal,
        authorizer,
        reservedProperties);
  }
}
