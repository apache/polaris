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
class ProductionReadinessChecksOptimizedSiblingCheckTest {

  private static final String OPTIMIZED_KEY = FeatureConfiguration.OPTIMIZED_SIBLING_CHECK.key();
  private static final String ALLOW_KEY = FeatureConfiguration.ALLOW_OPTIMIZED_SIBLING_CHECK.key();

  private ProductionReadinessChecks checks;

  @BeforeEach
  void setUp() {
    checks = new ProductionReadinessChecks();
  }

  @Test
  void disabledByDefaultReturnsOk() {
    ProductionReadinessCheck result =
        checks.checkOverlappingSiblingCheckSettings(mockConfig(Map.of(), Map.of()));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void enabledWithoutAcknowledgementReturnsSevereError() {
    ProductionReadinessCheck result =
        checks.checkOverlappingSiblingCheckSettings(
            mockConfig(Map.of(OPTIMIZED_KEY, "true"), Map.of()));

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors()).hasSize(1);
    assertThat(result.getErrors().get(0).severe()).isTrue();
    assertThat(result.getErrors().get(0).offendingProperty()).contains(OPTIMIZED_KEY);
  }

  @Test
  void enabledAndAcknowledgedReturnsOk() {
    ProductionReadinessCheck result =
        checks.checkOverlappingSiblingCheckSettings(
            mockConfig(Map.of(OPTIMIZED_KEY, "true", ALLOW_KEY, "true"), Map.of()));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void realmOverrideEnablesWithoutAcknowledgementReturnsSevereError() {
    ProductionReadinessCheck result =
        checks.checkOverlappingSiblingCheckSettings(
            mockConfig(
                Map.of(), Map.of("test-realm", mockRealmOverrides(Map.of(OPTIMIZED_KEY, "true")))));

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors()).hasSize(1);
    assertThat(result.getErrors().get(0).offendingProperty())
        .contains("test-realm")
        .contains(OPTIMIZED_KEY);
  }

  @Test
  void realmOverrideInheritsAcknowledgementFromDefaultsReturnsOk() {
    // The realm only turns the check on; the acknowledgement comes from the defaults, which is
    // what the realm effectively runs with.
    ProductionReadinessCheck result =
        checks.checkOverlappingSiblingCheckSettings(
            mockConfig(
                Map.of(ALLOW_KEY, "true"),
                Map.of("test-realm", mockRealmOverrides(Map.of(OPTIMIZED_KEY, "true")))));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void realmOverrideInheritsEnabledCheckAndWithdrawsAcknowledgementReturnsSevereError() {
    // The defaults are acknowledged, but the realm withdraws the acknowledgement while still
    // inheriting the enabled check.
    ProductionReadinessCheck result =
        checks.checkOverlappingSiblingCheckSettings(
            mockConfig(
                Map.of(OPTIMIZED_KEY, "true", ALLOW_KEY, "true"),
                Map.of("test-realm", mockRealmOverrides(Map.of(ALLOW_KEY, "false")))));

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors()).hasSize(1);
    assertThat(result.getErrors().get(0).offendingProperty())
        .contains("test-realm")
        .contains(OPTIMIZED_KEY);
  }

  @Test
  void realmOverrideDisablesInheritedCheckReturnsOk() {
    // The realm also withdraws the acknowledgement, so the result is only OK if the realm's
    // OPTIMIZED_SIBLING_CHECK=false override is honored.
    ProductionReadinessCheck result =
        checks.checkOverlappingSiblingCheckSettings(
            mockConfig(
                Map.of(OPTIMIZED_KEY, "true", ALLOW_KEY, "true"),
                Map.of(
                    "test-realm",
                    mockRealmOverrides(Map.of(OPTIMIZED_KEY, "false", ALLOW_KEY, "false")))));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void realmOverridingUnrelatedFlagsIsNotReportedAgain() {
    // The realm runs with the default values for both flags, which are already reported once.
    ProductionReadinessCheck result =
        checks.checkOverlappingSiblingCheckSettings(
            mockConfig(
                Map.of(OPTIMIZED_KEY, "true"),
                Map.of(
                    "test-realm",
                    mockRealmOverrides(
                        Map.of(FeatureConfiguration.DROP_WITH_PURGE_ENABLED.key(), "true")))));

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors()).hasSize(1);
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
