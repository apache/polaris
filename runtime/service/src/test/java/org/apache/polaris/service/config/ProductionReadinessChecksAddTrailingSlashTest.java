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
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProductionReadinessChecksAddTrailingSlashTest {

  private static final String FLAG_KEY = "ADD_TRAILING_SLASH_TO_LOCATION";

  private ProductionReadinessChecks checks;

  @BeforeEach
  void setUp() {
    checks = new ProductionReadinessChecks();
  }

  @Test
  void flagUnsetReturnsOk() {
    FeaturesConfiguration config = mockConfig(Map.of(), Map.of());

    ProductionReadinessCheck result = checks.checkAddTrailingSlashToLocation(config);

    assertThat(result.ready()).isTrue();
  }

  @Test
  void flagExplicitlyTrueReturnsOk() {
    FeaturesConfiguration config = mockConfig(Map.of(FLAG_KEY, "true"), Map.of());

    ProductionReadinessCheck result = checks.checkAddTrailingSlashToLocation(config);

    assertThat(result.ready()).isTrue();
  }

  @Test
  void flagExplicitlyFalseReturnsWarning() {
    FeaturesConfiguration config = mockConfig(Map.of(FLAG_KEY, "false"), Map.of());

    ProductionReadinessCheck result = checks.checkAddTrailingSlashToLocation(config);

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.offendingProperty()).contains(FLAG_KEY);
              assertThat(error.severe()).isFalse();
            });
  }

  @Test
  void realmOverrideFalseReturnsWarning() {
    FeaturesConfiguration config =
        mockConfig(Map.of(), Map.of("test-realm", mockRealmOverrides(Map.of(FLAG_KEY, "false"))));

    ProductionReadinessCheck result = checks.checkAddTrailingSlashToLocation(config);

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.offendingProperty()).contains("test-realm").contains(FLAG_KEY);
              assertThat(error.severe()).isFalse();
            });
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
