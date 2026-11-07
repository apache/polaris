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

import static org.apache.polaris.core.config.FeatureConfiguration.GCS_PRINCIPAL_ATTRIBUTION_ENABLED;
import static org.apache.polaris.core.config.FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import java.time.Clock;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.gcp.GcpCredentialsStorageIntegration;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.junit.jupiter.api.Test;

/** How {@link PolarisStorageIntegrationProviderImpl} selects GCP credentials for a storage. */
class GcpNamedCredentialsProviderTest {

  private static final RealmContext REALM = () -> "test-realm";

  private final AtomicInteger defaultLoads = new AtomicInteger();
  private final AtomicInteger teamALoads = new AtomicInteger();

  private static Supplier<GoogleCredentials> counting(AtomicInteger loads) {
    return () -> {
      loads.incrementAndGet();
      return GoogleCredentials.create(new AccessToken("fake", new Date()));
    };
  }

  private PolarisStorageIntegrationProviderImpl provider(Map<String, Object> flags) {
    StorageConfiguration storageConfiguration = mock(StorageConfiguration.class);
    Clock clock = Clock.systemUTC();
    when(storageConfiguration.gcpCredentialsSupplier(clock)).thenReturn(counting(defaultLoads));
    when(storageConfiguration.gcpNamedCredentialsSuppliers())
        .thenReturn(Map.of("team-a", counting(teamALoads)));
    RealmConfig realmConfig = new RealmConfigImpl((rc, name) -> flags.get(name), REALM);
    return new PolarisStorageIntegrationProviderImpl(
        storageConfiguration,
        new S3CredentialVendingMechanisms(Map.of()),
        realmConfig,
        clock,
        null);
  }

  private static List<PolarisEntity> entityPath(String storageName) {
    GcpStorageConfigurationInfo config =
        GcpStorageConfigurationInfo.builder()
            .addAllowedLocation("gs://bucket/path/")
            .storageName(storageName)
            .build();
    return List.of(
        new PolarisEntity.Builder()
            .setType(PolarisEntityType.CATALOG)
            .setName("entity")
            .setInternalProperties(
                Map.of(
                    PolarisEntityConstants.getStorageConfigInfoPropertyName(), config.serialize()))
            .build());
  }

  private static Map<String, Object> resolveByName() {
    return Map.of(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), true);
  }

  @Test
  void flagOnWithNameUsesOnlyTheNamedCredentials() {
    PolarisStorageIntegration integration =
        provider(resolveByName()).getStorageIntegration(entityPath("team-a"));

    assertThat(integration).isInstanceOf(GcpCredentialsStorageIntegration.class);
    assertThat(teamALoads).hasValue(1);
    assertThat(defaultLoads).hasValue(0);
  }

  @Test
  void flagOnWithoutNameUsesTheDefault() {
    provider(resolveByName()).getStorageIntegration(entityPath(null));

    assertThat(defaultLoads).hasValue(1);
    assertThat(teamALoads).hasValue(0);
  }

  @Test
  void flagOffIgnoresTheName() {
    provider(Map.of(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), false))
        .getStorageIntegration(entityPath("team-a"));
    provider(Map.of()).getStorageIntegration(entityPath("unknown"));

    assertThat(defaultLoads).hasValue(2);
    assertThat(teamALoads).hasValue(0);
  }

  @Test
  void flagOnWithUnknownNameFailsWithoutFallback() {
    PolarisStorageIntegrationProviderImpl provider = provider(resolveByName());

    assertThatThrownBy(() -> provider.getStorageIntegration(entityPath("unknown")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unknown")
        .hasMessageContaining("not configured");
    assertThat(defaultLoads).hasValue(0);
  }

  @Test
  void storageNameIsRefusedWithPrincipalAttribution() {
    PolarisStorageIntegrationProviderImpl provider =
        provider(
            Map.of(
                RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(),
                true,
                GCS_PRINCIPAL_ATTRIBUTION_ENABLED.key(),
                true));

    assertThatThrownBy(() -> provider.getStorageIntegration(entityPath("team-a")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("team-a")
        .hasMessageContaining(GCS_PRINCIPAL_ATTRIBUTION_ENABLED.key());
    assertThat(defaultLoads).hasValue(0);
    assertThat(teamALoads).hasValue(0);
  }
}
