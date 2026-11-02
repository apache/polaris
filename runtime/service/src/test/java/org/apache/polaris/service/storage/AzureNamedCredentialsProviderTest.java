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

import static org.apache.polaris.core.config.FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import java.time.Clock;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.azure.AzureCredentialsStorageIntegration;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.service.storage.StorageConfiguration.AzureNamedCredential;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Answers;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

/** How {@link PolarisStorageIntegrationProviderImpl} selects Azure credentials for a storage. */
class AzureNamedCredentialsProviderTest {

  private static final RealmContext REALM = () -> "test-realm";
  private static final String TENANT = "aaaaaaaa-1111-1111-1111-111111111111";
  private static final String OTHER_TENANT = "22222222-2222-2222-2222-222222222222";

  private final AtomicInteger teamALoads = new AtomicInteger();

  private PolarisStorageIntegrationProviderImpl provider(Map<String, Object> flags) {
    StorageConfiguration storageConfiguration = mock(StorageConfiguration.class);
    when(storageConfiguration.azureNamedCredentials())
        .thenReturn(
            Map.of(
                "team-a",
                new AzureNamedCredential(
                    TENANT,
                    () -> {
                      teamALoads.incrementAndGet();
                      return mock(TokenCredential.class);
                    })));
    RealmConfig realmConfig = new RealmConfigImpl((rc, name) -> flags.get(name), REALM);
    return new PolarisStorageIntegrationProviderImpl(
        storageConfiguration,
        new S3CredentialVendingMechanisms(Map.of()),
        realmConfig,
        Clock.systemUTC(),
        null);
  }

  /** A catalog, or a cleanup task: both carry the configuration in their internal properties. */
  private static List<PolarisEntity> entityPath(
      PolarisEntityType type, String storageName, String tenantId) {
    AzureStorageConfigurationInfo config =
        AzureStorageConfigurationInfo.builder()
            .addAllowedLocation("abfss://container@account.dfs.core.windows.net/path/")
            .tenantId(tenantId)
            .storageName(storageName)
            .build();
    return List.of(
        new PolarisEntity.Builder()
            .setType(type)
            .setName("entity")
            .setInternalProperties(
                Map.of(
                    PolarisEntityConstants.getStorageConfigInfoPropertyName(), config.serialize()))
            .build());
  }

  private static Map<String, Object> resolveByName() {
    return Map.of(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), true);
  }

  /** Counts how many times the default credential gets built while {@code action} runs. */
  private static int defaultCredentialBuilds(Runnable action) {
    try (MockedConstruction<DefaultAzureCredentialBuilder> builders =
        Mockito.mockConstruction(
            DefaultAzureCredentialBuilder.class,
            Mockito.withSettings().defaultAnswer(Answers.RETURNS_SELF))) {
      action.run();
      return builders.constructed().size();
    }
  }

  @ParameterizedTest
  @CsvSource({"CATALOG", "TASK"})
  void flagOnWithNameUsesOnlyTheNamedCredentials(PolarisEntityType type) {
    PolarisStorageIntegrationProviderImpl provider = provider(resolveByName());

    int defaultBuilds =
        defaultCredentialBuilds(
            () -> {
              PolarisStorageIntegration integration =
                  provider.getStorageIntegration(entityPath(type, "team-a", TENANT));
              assertThat(integration).isInstanceOf(AzureCredentialsStorageIntegration.class);
            });

    assertThat(teamALoads).hasValue(1);
    assertThat(defaultBuilds).isZero();
  }

  @Test
  void flagOnWithoutNameUsesTheDefault() {
    PolarisStorageIntegrationProviderImpl provider = provider(resolveByName());

    int defaultBuilds =
        defaultCredentialBuilds(
            () ->
                provider.getStorageIntegration(
                    entityPath(PolarisEntityType.CATALOG, null, TENANT)));

    assertThat(defaultBuilds).isEqualTo(1);
    assertThat(teamALoads).hasValue(0);
  }

  @Test
  void flagOffIgnoresTheName() {
    PolarisStorageIntegrationProviderImpl disabled =
        provider(Map.of(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), false));
    PolarisStorageIntegrationProviderImpl unset = provider(Map.of());

    int defaultBuilds =
        defaultCredentialBuilds(
            () -> {
              disabled.getStorageIntegration(
                  entityPath(PolarisEntityType.CATALOG, "team-a", TENANT));
              unset.getStorageIntegration(entityPath(PolarisEntityType.CATALOG, "unknown", TENANT));
              // The tenant check belongs to named credentials, so it does not apply here either.
              unset.getStorageIntegration(
                  entityPath(PolarisEntityType.CATALOG, "team-a", OTHER_TENANT));
            });

    assertThat(defaultBuilds).isEqualTo(3);
    assertThat(teamALoads).hasValue(0);
  }

  @ParameterizedTest
  @CsvSource({"CATALOG", "TASK"})
  void flagOnWithUnknownNameFailsWithoutFallback(PolarisEntityType type) {
    PolarisStorageIntegrationProviderImpl provider = provider(resolveByName());

    int defaultBuilds =
        defaultCredentialBuilds(
            () ->
                assertThatThrownBy(
                        () -> provider.getStorageIntegration(entityPath(type, "unknown", TENANT)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("unknown")
                    .hasMessageContaining("not configured"));

    assertThat(defaultBuilds).isZero();
  }

  @ParameterizedTest
  @ValueSource(strings = {OTHER_TENANT, "contoso.onmicrosoft.com"})
  void tenantMismatchFailsWithoutRevealingTheConfiguredTenant(String catalogTenant) {
    PolarisStorageIntegrationProviderImpl provider = provider(resolveByName());

    int defaultBuilds =
        defaultCredentialBuilds(
            () ->
                assertThatThrownBy(
                        () ->
                            provider.getStorageIntegration(
                                entityPath(PolarisEntityType.CATALOG, "team-a", catalogTenant)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("team-a")
                    .hasMessageContaining("tenant")
                    .hasMessageNotContaining(TENANT));

    assertThat(teamALoads).hasValue(0);
    assertThat(defaultBuilds).isZero();
  }

  @Test
  void tenantComparisonIgnoresCase() {
    PolarisStorageIntegration integration =
        provider(resolveByName())
            .getStorageIntegration(
                entityPath(PolarisEntityType.CATALOG, "team-a", TENANT.toUpperCase(Locale.ROOT)));

    assertThat(integration).isInstanceOf(AzureCredentialsStorageIntegration.class);
    assertThat(teamALoads).hasValue(1);
  }
}
