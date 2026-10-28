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
package org.apache.polaris.service.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProductionReadinessChecksGcsAttributionTest {

  private static final String ENABLED_KEY =
      FeatureConfiguration.GCS_PRINCIPAL_ATTRIBUTION_ENABLED.key();
  private static final String AUDIENCE_KEY =
      FeatureConfiguration.GCS_PRINCIPAL_ATTRIBUTION_WIF_AUDIENCE.key();
  private static final String ISSUER_KEY =
      FeatureConfiguration.GCS_PRINCIPAL_ATTRIBUTION_TOKEN_ISSUER.key();
  private static final String KEY_FILE_KEY =
      FeatureConfiguration.GCS_PRINCIPAL_ATTRIBUTION_SIGNING_KEY_FILE.key();

  private ProductionReadinessChecks checks;

  @BeforeEach
  void setUp() {
    checks = new ProductionReadinessChecks();
  }

  @Test
  void attributionDisabledReturnsOk() {
    FeaturesConfiguration config = mockConfig(Map.of(ENABLED_KEY, "false"), Map.of());

    ProductionReadinessCheck result = checks.checkGcsPrincipalAttribution(config);

    assertThat(result.ready()).isTrue();
  }

  @Test
  void attributionEnabledWithAllFieldsSetReturnsOk() {
    FeaturesConfiguration config =
        mockConfig(
            Map.of(
                ENABLED_KEY, "true",
                AUDIENCE_KEY, "//iam.googleapis.com/projects/123/...",
                ISSUER_KEY, "https://polaris.example.com",
                KEY_FILE_KEY, "/etc/polaris/signing-key.pem"),
            Map.of());

    ProductionReadinessCheck result = checks.checkGcsPrincipalAttribution(config);

    assertThat(result.ready()).isTrue();
  }

  @Test
  void attributionEnabledWithMissingAudienceReturnsError() {
    FeaturesConfiguration config =
        mockConfig(
            Map.of(
                ENABLED_KEY, "true",
                ISSUER_KEY, "https://polaris.example.com",
                KEY_FILE_KEY, "/etc/polaris/signing-key.pem"),
            Map.of());

    ProductionReadinessCheck result = checks.checkGcsPrincipalAttribution(config);

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors()).hasSize(1);
    assertThat(result.getErrors().get(0).offendingProperty()).contains(AUDIENCE_KEY);
  }

  @Test
  void attributionEnabledWithAllRequiredFieldsMissingReturnsThreeErrors() {
    FeaturesConfiguration config = mockConfig(Map.of(ENABLED_KEY, "true"), Map.of());

    ProductionReadinessCheck result = checks.checkGcsPrincipalAttribution(config);

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors()).hasSize(3);
  }

  @Test
  void realmOverrideEnablesAttributionWithMissingFieldsReturnsError() {
    FeaturesConfiguration config =
        mockConfig(
            Map.of(),
            Map.of(
                "test-realm",
                mockRealmOverrides(
                    Map.of(
                        ENABLED_KEY, "true",
                        ISSUER_KEY, "https://polaris.example.com",
                        KEY_FILE_KEY, "/etc/polaris/signing-key.pem"))));

    ProductionReadinessCheck result = checks.checkGcsPrincipalAttribution(config);

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors()).hasSize(1);
    assertThat(result.getErrors().get(0).offendingProperty())
        .contains("test-realm")
        .contains(AUDIENCE_KEY);
  }

  @Test
  void realmOverrideInheritsRequiredFieldsFromDefaultsReturnsOk() {
    FeaturesConfiguration config =
        mockConfig(
            Map.of(
                AUDIENCE_KEY, "//iam.googleapis.com/projects/123/...",
                ISSUER_KEY, "https://polaris.example.com",
                KEY_FILE_KEY, "/etc/polaris/signing-key.pem"),
            Map.of("test-realm", mockRealmOverrides(Map.of(ENABLED_KEY, "true"))));

    ProductionReadinessCheck result = checks.checkGcsPrincipalAttribution(config);

    assertThat(result.ready()).isTrue();
  }

  @Test
  void realmOverrideDisablesAttributionOverridesDefaultEnabledReturnsOk() {
    FeaturesConfiguration config =
        mockConfig(
            Map.of(ENABLED_KEY, "true"),
            Map.of("test-realm", mockRealmOverrides(Map.of(ENABLED_KEY, "false"))));

    ProductionReadinessCheck result = checks.checkGcsPrincipalAttribution(config);

    // default-level errors still reported, but realm override is clean
    assertThat(result.getErrors()).noneMatch(e -> e.offendingProperty().contains("test-realm"));
  }

  private static FeaturesConfiguration mockConfig(
      Map<String, String> defaults, Map<String, RealmOverridable.RealmOverrides> realmOverrides) {
    FeaturesConfiguration config = mock(FeaturesConfiguration.class);
    when(config.defaults()).thenReturn(defaults);
    when(config.realmOverrides()).thenReturn(realmOverrides);
    return config;
  }

  private static RealmOverridable.RealmOverrides mockRealmOverrides(Map<String, String> overrides) {
    RealmOverridable.RealmOverrides realmOverrides = mock(RealmOverridable.RealmOverrides.class);
    when(realmOverrides.overrides()).thenReturn(overrides);
    return realmOverrides;
  }
}
