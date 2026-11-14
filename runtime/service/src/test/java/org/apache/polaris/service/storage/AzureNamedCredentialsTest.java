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
import static org.mockito.Mockito.verify;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import io.smallrye.config.ConfigValidationException;
import io.smallrye.config.EnvConfigSource;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Answers;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

class AzureNamedCredentialsTest {

  private static final String TENANT = "11111111-1111-1111-1111-111111111111";
  private static final String OTHER_TENANT = "22222222-2222-2222-2222-222222222222";
  private static final String SECRET_A = "secret-value-of-team-a";
  private static final String SECRET_B = "secret-value-of-team-b";

  private static StorageConfiguration configFrom(Map<String, String> properties) {
    return configFrom(properties, Map.of());
  }

  private static StorageConfiguration configFrom(
      Map<String, String> properties, Map<String, String> environment) {
    return new SmallRyeConfigBuilder()
        .withMapping(StorageConfiguration.class)
        .withSources(new PropertiesConfigSource(properties, "test", 100))
        .withSources(new EnvConfigSource(environment, 300))
        .build()
        .getConfigMapping(StorageConfiguration.class);
  }

  private static Map<String, String> properties(String name, String clientSecret) {
    return Map.of(
        "polaris.storage.azure." + name + ".tenant-id", TENANT,
        "polaris.storage.azure." + name + ".client-id", "app-" + name,
        "polaris.storage.azure." + name + ".client-secret", clientSecret);
  }

  private static Map<String, String> twoNames() {
    Map<String, String> properties = new HashMap<>(properties("team-a", SECRET_A));
    properties.putAll(properties("team-b", SECRET_B));
    return properties;
  }

  private static AzureStorageConfigurationInfo storage(String storageName, String tenantId) {
    return AzureStorageConfigurationInfo.builder()
        .addAllowedLocation("abfss://container@account.dfs.core.windows.net/path/")
        .tenantId(tenantId)
        .storageName(storageName)
        .build();
  }

  private static TokenCredential credentialOf(StorageConfiguration config, String name) {
    return credentialOf(config, name, TENANT);
  }

  private static TokenCredential credentialOf(
      StorageConfiguration config, String name, String tenantId) {
    Map<String, Object> flags = Map.of(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), true);
    RealmConfig realmConfig =
        new RealmConfigImpl((rc, flag) -> flags.get(flag), () -> "test-realm");
    return new AzureCredentialsFactory(config, realmConfig).credential(storage(name, tenantId));
  }

  /** Builds the credential and returns the builder that built it, to check what it was given. */
  private static ClientSecretCredentialBuilder builderOf(StorageConfiguration config, String name) {
    try (MockedConstruction<ClientSecretCredentialBuilder> builders =
        Mockito.mockConstruction(
            ClientSecretCredentialBuilder.class,
            Mockito.withSettings().defaultAnswer(Answers.RETURNS_SELF),
            (builder, context) ->
                Mockito.when(builder.build())
                    .thenReturn(Mockito.mock(ClientSecretCredential.class)))) {
      // The mocked builder builds nothing real; only the values it was given matter here.
      assertThat(credentialOf(config, name)).isNotNull();
      assertThat(builders.constructed()).hasSize(1);
      return builders.constructed().get(0);
    }
  }

  @Test
  void eachNameUsesItsOwnServicePrincipal() {
    StorageConfiguration config = configFrom(twoNames());

    assertThat(config.azure().storages()).containsOnlyKeys("team-a", "team-b");
    assertThat(config.azure().storages().get("team-a").tenantId()).isEqualTo(TENANT);
    ClientSecretCredentialBuilder builderA = builderOf(config, "team-a");
    verify(builderA).tenantId(TENANT);
    verify(builderA).clientId("app-team-a");
    verify(builderA).clientSecret(SECRET_A);
    ClientSecretCredentialBuilder builderB = builderOf(config, "team-b");
    verify(builderB).clientId("app-team-b");
    verify(builderB).clientSecret(SECRET_B);
  }

  @Test
  void unknownNameFailsToBuild() {
    StorageConfiguration config = configFrom(properties("team-a", SECRET_A));

    assertThatThrownBy(() -> credentialOf(config, "unknown"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unknown")
        .hasMessageContaining("not configured");
  }

  @ParameterizedTest
  @ValueSource(strings = {OTHER_TENANT, "contoso.onmicrosoft.com"})
  void tenantMismatchFailsWithoutRevealingTheConfiguredTenant(String requestedTenant) {
    StorageConfiguration config = configFrom(properties("team-a", SECRET_A));

    assertThatThrownBy(() -> credentialOf(config, "team-a", requestedTenant))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("team-a")
        .hasMessageContaining("tenant")
        .hasMessageNotContaining(TENANT);
  }

  @Test
  void tenantComparisonIgnoresCase() {
    StorageConfiguration config = configFrom(properties("team-a", SECRET_A));

    assertThat(credentialOf(config, "team-a", TENANT.toUpperCase(Locale.ROOT))).isNotNull();
  }

  @Test
  void badEntryFailsOnlyItsOwnNameAndKeepsTheSecretOutOfTheError() {
    Map<String, String> properties = twoNames();
    properties.put("polaris.storage.azure.team-a.tenant-id", "not/a/tenant");
    StorageConfiguration config = configFrom(properties);

    // The catalog asks for the same tenant, so the build itself is what fails.
    assertThatThrownBy(() -> credentialOf(config, "team-a", "not/a/tenant"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("team-a")
        .hasMessageNotContaining(SECRET_A)
        .hasMessageNotContaining("not/a/tenant")
        .hasCauseInstanceOf(IllegalArgumentException.class);
    TokenCredential teamB = credentialOf(config, "team-b");
    assertThat(teamB).isNotNull();
  }

  @Test
  void incompleteEntryFailsAtStartupNamingThePropertyButNoValues() {
    assertThatThrownBy(
            () ->
                configFrom(
                    Map.of(
                        "polaris.storage.azure.team-a.tenant-id", TENANT,
                        "polaris.storage.azure.team-a.client-secret", SECRET_A)))
        .isInstanceOf(ConfigValidationException.class)
        .hasMessageContaining("polaris.storage.azure.team-a.client-id")
        .hasMessageNotContaining(SECRET_A);
  }

  @Test
  void environmentVariableSuppliesTheSecretOfANameDefinedByProperties() {
    StorageConfiguration config =
        configFrom(
            Map.of(
                "polaris.storage.azure.team-a.tenant-id",
                TENANT,
                "polaris.storage.azure.team-a.client-id",
                "app-team-a"),
            Map.of("POLARIS_STORAGE_AZURE_TEAM_A_CLIENT_SECRET", SECRET_A));

    verify(builderOf(config, "team-a")).clientSecret(SECRET_A);
  }

  @Test
  void unnamedPropertiesAreNotTakenForAStorageName() {
    Map<String, String> properties = new HashMap<>(properties("team-a", SECRET_A));
    properties.put("polaris.storage.azure.tenant-id", OTHER_TENANT);
    properties.put("polaris.storage.azure.client-id", "app-unnamed");
    properties.put("polaris.storage.azure.client-secret", "secret-value-unnamed");
    StorageConfiguration config = configFrom(properties);

    assertThat(config.azure().storages()).containsOnlyKeys("team-a");
    assertThat(config.azure().tenantId()).contains(OTHER_TENANT);
    assertThat(config.azure().clientId()).contains("app-unnamed");
    assertThat(config.azure().clientSecret()).contains("secret-value-unnamed");
    assertThat(config.azure().storages().get("team-a").tenantId()).isEqualTo(TENANT);
  }

  @Test
  void unnamedPropertiesAreOptional() {
    StorageConfiguration config = configFrom(properties("team-a", SECRET_A));

    assertThat(config.azure().tenantId()).isEmpty();
    assertThat(config.azure().clientId()).isEmpty();
    assertThat(config.azure().clientSecret()).isEmpty();
  }
}
