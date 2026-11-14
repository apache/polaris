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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.time.Clock;
import java.util.HashMap;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

/** How {@link PolarisStorageIntegrationProviderImpl} selects Azure credentials for a storage. */
class AzureNamedCredentialsProviderTest {

  private static final RealmContext REALM = () -> "test-realm";
  private static final String TENANT = "aaaaaaaa-1111-1111-1111-111111111111";
  private static final String OTHER_TENANT = "22222222-2222-2222-2222-222222222222";

  private static final String UNNAMED_CLIENT_ID = "app-unnamed";

  private final AtomicInteger builds = new AtomicInteger();
  private MockedConstruction<ClientSecretCredentialBuilder> builders;

  @BeforeEach
  void mockCredentialBuilds() {
    // The real selection and checks run; only the credential build is replaced.
    builders =
        Mockito.mockConstruction(
            ClientSecretCredentialBuilder.class,
            Mockito.withSettings().defaultAnswer(Answers.RETURNS_SELF),
            (builder, context) ->
                when(builder.build())
                    .thenAnswer(invocation -> {
                      builds.incrementAndGet();
                      return mock(ClientSecretCredential.class);
                    }));
  }

  @AfterEach
  void closeBuilders() {
    builders.close();
  }

  private static Map<String, String> teamA() {
    return Map.of(
        "polaris.storage.azure.team-a.tenant-id", TENANT,
        "polaris.storage.azure.team-a.client-id", "app-team-a",
        "polaris.storage.azure.team-a.client-secret", "secret-a");
  }

  private static Map<String, String> teamAAndUnnamed() {
    Map<String, String> properties = new HashMap<>(teamA());
    properties.put("polaris.storage.azure.tenant-id", TENANT);
    properties.put("polaris.storage.azure.client-id", UNNAMED_CLIENT_ID);
    properties.put("polaris.storage.azure.client-secret", "secret-unnamed");
    return properties;
  }

  private PolarisStorageIntegrationProviderImpl provider(Map<String, Object> flags) {
    return provider(flags, teamA());
  }

  private PolarisStorageIntegrationProviderImpl provider(
      Map<String, Object> flags, Map<String, String> properties) {
    StorageConfiguration storageConfiguration =
        new SmallRyeConfigBuilder()
            .withMapping(StorageConfiguration.class)
            .withSources(new PropertiesConfigSource(properties, "test", 100))
            .build()
            .getConfigMapping(StorageConfiguration.class);
    RealmConfig realmConfig = new RealmConfigImpl((rc, name) -> flags.get(name), REALM);
    return new PolarisStorageIntegrationProviderImpl(
        storageConfiguration,
        new S3CredentialVendingMechanisms(Map.of()),
        realmConfig,
        Clock.systemUTC(),
        null,
        new AzureCredentialsFactory(storageConfiguration, realmConfig));
  }

  /** The client IDs of the credentials built so far, in build order. */
  private List<String> builtClientIds() {
    return builders.constructed().stream()
        .map(builder -> {
          ArgumentCaptor<String> clientId = ArgumentCaptor.forClass(String.class);
          verify(builder).clientId(clientId.capture());
          return clientId.getValue();
        })
        .toList();
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
  @EnumSource(
      value = PolarisEntityType.class,
      names = {"CATALOG", "TASK"})
  void flagOnWithNameUsesOnlyTheNamedCredentials(PolarisEntityType type) {
    PolarisStorageIntegrationProviderImpl provider = provider(resolveByName());

    int defaultBuilds =
        defaultCredentialBuilds(() -> {
          PolarisStorageIntegration integration =
              provider.getStorageIntegration(entityPath(type, "team-a", TENANT));
          assertThat(integration).isInstanceOf(AzureCredentialsStorageIntegration.class);
        });

    assertThat(builds).hasValue(1);
    assertThat(defaultBuilds).isZero();
  }

  @Test
  void flagOnWithoutNameUsesTheDefault() {
    PolarisStorageIntegrationProviderImpl provider = provider(resolveByName());

    int defaultBuilds =
        defaultCredentialBuilds(() ->
            provider.getStorageIntegration(entityPath(PolarisEntityType.CATALOG, null, TENANT)));

    assertThat(defaultBuilds).isEqualTo(1);
    assertThat(builds).hasValue(0);
  }

  @Test
  void flagOffIgnoresTheName() {
    PolarisStorageIntegrationProviderImpl disabled =
        provider(Map.of(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), false));
    PolarisStorageIntegrationProviderImpl unset = provider(Map.of());

    int defaultBuilds =
        defaultCredentialBuilds(() -> {
          disabled.getStorageIntegration(entityPath(PolarisEntityType.CATALOG, "team-a", TENANT));
          unset.getStorageIntegration(entityPath(PolarisEntityType.CATALOG, "unknown", TENANT));
          // The tenant check belongs to named credentials, so it does not apply here either.
          unset.getStorageIntegration(
              entityPath(PolarisEntityType.CATALOG, "team-a", OTHER_TENANT));
        });

    assertThat(defaultBuilds).isEqualTo(3);
    assertThat(builds).hasValue(0);
  }

  @ParameterizedTest
  @EnumSource(
      value = PolarisEntityType.class,
      names = {"CATALOG", "TASK"})
  void flagOnWithUnknownNameFailsWithoutFallback(PolarisEntityType type) {
    PolarisStorageIntegrationProviderImpl provider = provider(resolveByName());

    int defaultBuilds =
        defaultCredentialBuilds(() ->
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
        defaultCredentialBuilds(() ->
            assertThatThrownBy(() ->
                    provider.getStorageIntegration(
                        entityPath(PolarisEntityType.CATALOG, "team-a", catalogTenant)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("team-a")
                .hasMessageContaining("tenant")
                .hasMessageNotContaining(TENANT));

    assertThat(builds).hasValue(0);
    assertThat(defaultBuilds).isZero();
  }

  @Test
  void tenantComparisonIgnoresCase() {
    PolarisStorageIntegration integration =
        provider(resolveByName())
            .getStorageIntegration(
                entityPath(PolarisEntityType.CATALOG, "team-a", TENANT.toUpperCase(Locale.ROOT)));

    assertThat(integration).isInstanceOf(AzureCredentialsStorageIntegration.class);
    assertThat(builds).hasValue(1);
  }

  @Test
  void theFlagIsReadPerRealm() {
    Map<String, Object> flags = new HashMap<>();
    PolarisStorageIntegrationProviderImpl provider = provider(flags);

    int defaultBuilds =
        defaultCredentialBuilds(() -> {
          flags.put(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), true);
          provider.getStorageIntegration(entityPath(PolarisEntityType.CATALOG, "team-a", TENANT));
          flags.put(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), false);
          provider.getStorageIntegration(entityPath(PolarisEntityType.CATALOG, "team-a", TENANT));
          flags.put(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), true);
          provider.getStorageIntegration(entityPath(PolarisEntityType.CATALOG, "team-a", TENANT));
        });

    // The realm with the flag off gets the default credential; the realms with it on get team-a's.
    assertThat(defaultBuilds).isEqualTo(1);
    assertThat(builtClientIds()).containsExactly("app-team-a", "app-team-a");
  }

  @Test
  void unnamedPropertiesReplaceTheDefaultWithoutAName() {
    PolarisStorageIntegrationProviderImpl provider = provider(resolveByName(), teamAAndUnnamed());

    int defaultBuilds =
        defaultCredentialBuilds(() -> {
          provider.getStorageIntegration(entityPath(PolarisEntityType.CATALOG, null, TENANT));
          provider.getStorageIntegration(entityPath(PolarisEntityType.TASK, null, TENANT));
        });

    assertThat(defaultBuilds).isZero();
    assertThat(builtClientIds()).containsExactly(UNNAMED_CLIENT_ID, UNNAMED_CLIENT_ID);
  }

  @Test
  void unnamedPropertiesAreUsedWhenTheFlagIsOff() {
    PolarisStorageIntegrationProviderImpl provider = provider(Map.of(), teamAAndUnnamed());

    int defaultBuilds =
        defaultCredentialBuilds(() ->
            provider.getStorageIntegration(
                entityPath(PolarisEntityType.CATALOG, "team-a", TENANT)));

    assertThat(defaultBuilds).isZero();
    assertThat(builtClientIds()).containsExactly(UNNAMED_CLIENT_ID);
  }

  @Test
  void aStorageNameWinsOverTheUnnamedProperties() {
    PolarisStorageIntegrationProviderImpl provider = provider(resolveByName(), teamAAndUnnamed());

    provider.getStorageIntegration(entityPath(PolarisEntityType.CATALOG, "team-a", TENANT));

    assertThat(builtClientIds()).containsExactly("app-team-a");
  }

  @ParameterizedTest
  @ValueSource(strings = {OTHER_TENANT, "contoso.onmicrosoft.com"})
  void unnamedTenantMismatchFailsWithoutRevealingTheConfiguredTenant(String catalogTenant) {
    PolarisStorageIntegrationProviderImpl provider = provider(resolveByName(), teamAAndUnnamed());

    int defaultBuilds =
        defaultCredentialBuilds(() ->
            assertThatThrownBy(() ->
                    provider.getStorageIntegration(
                        entityPath(PolarisEntityType.CATALOG, null, catalogTenant)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("polaris.storage.azure.tenant-id")
                .hasMessageNotContaining(TENANT));

    assertThat(defaultBuilds).isZero();
    assertThat(builds).hasValue(0);
  }

  @Test
  void incompleteUnnamedPropertiesFailNamingOnlyTheMissingOnes() {
    Map<String, String> properties = new HashMap<>(teamA());
    properties.put("polaris.storage.azure.tenant-id", TENANT);
    properties.put("polaris.storage.azure.client-secret", "secret-unnamed");
    PolarisStorageIntegrationProviderImpl provider = provider(resolveByName(), properties);

    int defaultBuilds =
        defaultCredentialBuilds(() ->
            assertThatThrownBy(() ->
                    provider.getStorageIntegration(
                        entityPath(PolarisEntityType.CATALOG, null, TENANT)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("polaris.storage.azure.client-id")
                .hasMessageNotContaining("tenant-id")
                .hasMessageNotContaining("secret-unnamed"));

    assertThat(defaultBuilds).isZero();
    // A named credential is unaffected.
    provider.getStorageIntegration(entityPath(PolarisEntityType.CATALOG, "team-a", TENANT));
    assertThat(builtClientIds()).containsExactly("app-team-a");
  }
}
