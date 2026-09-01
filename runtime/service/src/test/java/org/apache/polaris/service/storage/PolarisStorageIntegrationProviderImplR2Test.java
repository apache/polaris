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
package org.apache.polaris.service.storage;

import static org.apache.polaris.core.config.RealmConfigurationSource.EMPTY_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.LocationGrant;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.core.storage.r2.R2CredentialsStorageIntegration;
import org.apache.polaris.core.storage.r2.R2ParentToken;
import org.apache.polaris.core.storage.r2.R2ParentTokenResolver;
import org.apache.polaris.core.storage.r2.R2StorageConfigurationInfo;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;

class PolarisStorageIntegrationProviderImplR2Test {

  private static final RealmConfig REALM_CONFIG = new RealmConfigImpl(EMPTY_CONFIG, () -> "realm");

  @Test
  void r2CatalogGetsAnR2Integration() {
    R2ParentTokenResolver resolver = n -> Optional.of(new R2ParentToken("k", "s"));
    PolarisStorageIntegrationProviderImpl provider =
        new PolarisStorageIntegrationProviderImpl(
            destination -> Mockito.mock(StsClient.class),
            Optional.empty(),
            () -> null,
            resolver,
            Clock.systemUTC(),
            new StorageCredentialCache(() -> 100),
            REALM_CONFIG,
            new PolarisDefaultDiagServiceImpl());
    R2StorageConfigurationInfo config =
        R2StorageConfigurationInfo.builder()
            .accountId("0123456789abcdef0123456789abcdef")
            .addAllowedLocations("s3://bucket/")
            .build();
    PolarisEntity catalog =
        new CatalogEntity.Builder()
            .setName("r2cat")
            .setInternalProperties(
                Map.of(
                    PolarisEntityConstants.getStorageConfigInfoPropertyName(), config.serialize()))
            .build();
    PolarisStorageIntegration integration = provider.getStorageIntegration(List.of(catalog));
    assertThat(integration).isInstanceOf(R2CredentialsStorageIntegration.class);

    StorageAccessConfig accessConfig =
        integration.getStorageAccessConfig(
            List.of(
                new LocationGrant(Set.of("s3://bucket/x/"), Set.of(PolarisStorageActions.READ))),
            Optional.empty(),
            CredentialVendingContext.empty());
    assertThat(accessConfig.credentials())
        .containsEntry(StorageAccessProperty.R2_KEY_ID.getPropertyName(), "k");
  }
}
