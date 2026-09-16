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

import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.core.storage.aws.S3CredentialVendingMechanism;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.storage.DefaultCredentialVendingMechanism;
import org.apache.polaris.service.storage.S3CredentialVendingMechanisms;
import org.apache.polaris.service.storage.StsCredentialVendingMechanism;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;

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
    S3CredentialVendingMechanisms vendingMechanisms =
        new S3CredentialVendingMechanisms(
            Map.of(
                S3CredentialVendingMechanism.STS,
                new StsCredentialVendingMechanism(
                    destination -> Mockito.mock(StsClient.class), Optional.empty(), null),
                S3CredentialVendingMechanism.DEFAULT,
                new DefaultCredentialVendingMechanism(
                    destination -> Mockito.mock(StsClient.class), Optional.empty(), null)));
    return new PolarisAdminService(
        callContext,
        resolutionManifestFactory,
        metaStoreManager,
        userSecretsManager,
        serviceIdentityProvider,
        principal,
        authorizer,
        reservedProperties,
        vendingMechanisms);
  }
}
