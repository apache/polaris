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
import static org.apache.polaris.service.storage.GcpNamedCredentialsTest.serviceAccountJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.gcp.GcpCredentialsStorageIntegration;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

/** How {@link PolarisStorageIntegrationProviderImpl} selects GCP credentials for a storage. */
class GcpNamedCredentialsProviderTest {

  private static final RealmContext REALM = () -> "test-realm";

  private final List<GoogleCredentials> sources = new ArrayList<>();
  private MockedConstruction<GcpCredentialsStorageIntegration> integrations;

  @BeforeEach
  void captureSourceCredentials() {
    // The real selection runs; only the integration, which would call GCP, is replaced. Its first
    // constructor argument is the source credentials.
    integrations =
        Mockito.mockConstruction(
            GcpCredentialsStorageIntegration.class,
            (integration, context) -> sources.add((GoogleCredentials) context.arguments().get(0)));
  }

  @AfterEach
  void closeIntegrations() {
    integrations.close();
  }

  private static Map<String, String> teamAAndToken() {
    return Map.of(
        "polaris.storage.gcp.token",
        "gcp-token",
        "polaris.storage.gcp.team-a.credentials",
        serviceAccountJson("team-a@p.iam"));
  }

  private static Map<String, String> teamAAndUnnamed() {
    return Map.of(
        "polaris.storage.gcp.credentials", serviceAccountJson("unnamed@p.iam"),
        "polaris.storage.gcp.team-a.credentials", serviceAccountJson("team-a@p.iam"));
  }

  private PolarisStorageIntegrationProviderImpl provider(Map<String, Object> flags) {
    return provider(flags, teamAAndToken());
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
        new S3CredentialVendingMechanisms(Map.of()),
        realmConfig,
        null,
        null,
        new GcpCredentialsFactory(storageConfiguration, realmConfig, Clock.systemUTC()));
  }

  /** The source credentials of the integrations built so far: a client email, or the token. */
  private List<String> sourceCredentials() {
    return sources.stream()
        .map(credentials ->
            credentials instanceof ServiceAccountCredentials serviceAccount
                ? serviceAccount.getClientEmail()
                : credentials instanceof FixedGcpAccessTokenCredentials ? "token" : "other")
        .toList();
  }

  /** A catalog, or a cleanup task: both carry the configuration in their internal properties. */
  private static List<PolarisEntity> entityPath(PolarisEntityType type, String storageName) {
    GcpStorageConfigurationInfo config =
        GcpStorageConfigurationInfo.builder()
            .addAllowedLocation("gs://bucket/path/")
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

  @ParameterizedTest
  @EnumSource(
      value = PolarisEntityType.class,
      names = {"CATALOG", "TASK"})
  void flagOnWithNameUsesOnlyTheNamedCredentials(PolarisEntityType type) {
    PolarisStorageIntegration integration =
        provider(resolveByName()).getStorageIntegration(entityPath(type, "team-a"));

    assertThat(integration).isInstanceOf(GcpCredentialsStorageIntegration.class);
    assertThat(sourceCredentials()).containsExactly("team-a@p.iam");
  }

  @Test
  void flagOnWithoutNameUsesTheDefault() {
    provider(resolveByName()).getStorageIntegration(entityPath(PolarisEntityType.CATALOG, null));

    assertThat(sourceCredentials()).containsExactly("token");
  }

  @Test
  void flagOffIgnoresTheName() {
    provider(Map.of(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), false))
        .getStorageIntegration(entityPath(PolarisEntityType.CATALOG, "team-a"));
    provider(Map.of()).getStorageIntegration(entityPath(PolarisEntityType.CATALOG, "unknown"));

    assertThat(sourceCredentials()).containsExactly("token", "token");
  }

  @ParameterizedTest
  @EnumSource(
      value = PolarisEntityType.class,
      names = {"CATALOG", "TASK"})
  void flagOnWithUnknownNameFailsWithoutFallback(PolarisEntityType type) {
    PolarisStorageIntegrationProviderImpl provider = provider(resolveByName());

    assertThatThrownBy(() -> provider.getStorageIntegration(entityPath(type, "unknown")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unknown")
        .hasMessageContaining("not configured");
    assertThat(sources).isEmpty();
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

    assertThatThrownBy(
            () -> provider.getStorageIntegration(entityPath(PolarisEntityType.CATALOG, "team-a")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("team-a")
        .hasMessageContaining(GCS_PRINCIPAL_ATTRIBUTION_ENABLED.key());
    assertThat(sources).isEmpty();
  }

  @Test
  void theFlagIsReadPerRealm() {
    Map<String, Object> flags = new HashMap<>();
    PolarisStorageIntegrationProviderImpl provider = provider(flags);

    flags.put(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), true);
    provider.getStorageIntegration(entityPath(PolarisEntityType.CATALOG, "team-a"));
    flags.put(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), false);
    provider.getStorageIntegration(entityPath(PolarisEntityType.CATALOG, "team-a"));
    flags.put(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), true);
    provider.getStorageIntegration(entityPath(PolarisEntityType.CATALOG, "team-a"));

    // The realm with the flag off gets the default; the realms with it on share team-a's.
    assertThat(sourceCredentials()).containsExactly("team-a@p.iam", "token", "team-a@p.iam");
    assertThat(sources.get(2)).isSameAs(sources.get(0));
  }

  @Test
  void unnamedCredentialsReplaceTheDefaultWithoutAName() {
    PolarisStorageIntegrationProviderImpl provider = provider(resolveByName(), teamAAndUnnamed());

    provider.getStorageIntegration(entityPath(PolarisEntityType.CATALOG, null));
    provider.getStorageIntegration(entityPath(PolarisEntityType.TASK, null));

    assertThat(sourceCredentials()).containsExactly("unnamed@p.iam", "unnamed@p.iam");
  }

  @Test
  void unnamedCredentialsAreUsedWhenTheFlagIsOff() {
    provider(Map.of(), teamAAndUnnamed())
        .getStorageIntegration(entityPath(PolarisEntityType.CATALOG, "team-a"));

    assertThat(sourceCredentials()).containsExactly("unnamed@p.iam");
  }

  @Test
  void aStorageNameWinsOverTheUnnamedCredentials() {
    provider(resolveByName(), teamAAndUnnamed())
        .getStorageIntegration(entityPath(PolarisEntityType.CATALOG, "team-a"));

    assertThat(sourceCredentials()).containsExactly("team-a@p.iam");
  }
}
