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

import java.util.Map;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.Mockito;

@ExtendWith(MockitoExtension.class)
class ProductionReadinessChecksStorageCredentialRefreshBufferTest {

  private static final String BUFFER_KEY =
      FeatureConfiguration.STORAGE_CREDENTIAL_REFRESH_BUFFER_SECONDS.key();
  private static final String DURATION_KEY =
      FeatureConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS.key();

  private ProductionReadinessChecks checks;

  @BeforeEach
  void setUp() {
    checks = new ProductionReadinessChecks();
  }

  @Test
  void unsetDefaultsReturnOk() {
    // Neither flag configured: falls back to the flags' own built-in defaults
    // (buffer=0, duration=3600), which are mutually valid.
    FeaturesConfiguration config = mockConfig(Map.of(), Map.of());

    ProductionReadinessCheck result = checks.checkStorageCredentialRefreshBuffer(config);

    assertThat(result.ready()).isTrue();
  }

  @Test
  void validDefaultsReturnOk() {
    FeaturesConfiguration config =
        mockConfig(Map.of(BUFFER_KEY, "300", DURATION_KEY, "3600"), Map.of());

    ProductionReadinessCheck result = checks.checkStorageCredentialRefreshBuffer(config);

    assertThat(result.ready()).isTrue();
  }

  @Test
  void negativeDefaultReturnsWarning() {
    FeaturesConfiguration config =
        mockConfig(Map.of(BUFFER_KEY, "-1", DURATION_KEY, "3600"), Map.of());

    ProductionReadinessCheck result = checks.checkStorageCredentialRefreshBuffer(config);

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(error -> {
          assertThat(error.offendingProperty()).contains(BUFFER_KEY);
          assertThat(error.severe()).isFalse();
        });
  }

  @Test
  void defaultAtOrAboveDurationReturnsWarning() {
    FeaturesConfiguration config =
        mockConfig(Map.of(BUFFER_KEY, "3600", DURATION_KEY, "3600"), Map.of());

    ProductionReadinessCheck result = checks.checkStorageCredentialRefreshBuffer(config);

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors()).hasSize(1);
  }

  @Test
  void realmOverrideOutOfRangeReturnsWarning() {
    FeaturesConfiguration config =
        mockConfig(
            Map.of(BUFFER_KEY, "300", DURATION_KEY, "3600"),
            Map.of("test-realm", mockRealmOverrides(Map.of(BUFFER_KEY, "-1"))));

    ProductionReadinessCheck result = checks.checkStorageCredentialRefreshBuffer(config);

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(error ->
            assertThat(error.offendingProperty()).contains("test-realm").contains(BUFFER_KEY));
  }

  @Test
  void realmOverrideWithOwnDurationIsCheckedAgainstItsOwnDuration() {
    // The realm overrides both: buffer=1800 is fine against the realm's own duration=7200,
    // even though it would be invalid against the global default duration of 3600.
    FeaturesConfiguration config =
        mockConfig(
            Map.of(BUFFER_KEY, "300", DURATION_KEY, "3600"),
            Map.of(
                "test-realm",
                mockRealmOverrides(Map.of(BUFFER_KEY, "1800", DURATION_KEY, "7200"))));

    ProductionReadinessCheck result = checks.checkStorageCredentialRefreshBuffer(config);

    assertThat(result.ready()).isTrue();
  }

  @Test
  void realmNotOverridingBufferIsNotChecked() {
    // Default buffer/duration are valid; the realm only overrides an unrelated flag, so it
    // shouldn't be re-evaluated against the buffer check at all.
    FeaturesConfiguration config =
        mockConfig(
            Map.of(BUFFER_KEY, "300", DURATION_KEY, "3600"),
            Map.of("test-realm", mockRealmOverrides(Map.of())));

    ProductionReadinessCheck result = checks.checkStorageCredentialRefreshBuffer(config);

    assertThat(result.ready()).isTrue();
  }

  private static FeaturesConfiguration mockConfig(
      Map<String, String> defaults, Map<String, RealmOverridable.RealmOverrides> realmOverrides) {
    FeaturesConfiguration config = Mockito.mock(FeaturesConfiguration.class);
    Mockito.when(config.defaults()).thenReturn(defaults);
    Mockito.when(config.realmOverrides()).thenReturn(realmOverrides);
    return config;
  }

  private static RealmOverridable.RealmOverrides mockRealmOverrides(Map<String, String> overrides) {
    RealmOverridable.RealmOverrides realmOverrides =
        Mockito.mock(RealmOverridable.RealmOverrides.class);
    Mockito.when(realmOverrides.overrides()).thenReturn(overrides);
    return realmOverrides;
  }
}
