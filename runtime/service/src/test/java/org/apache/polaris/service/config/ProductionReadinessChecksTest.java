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
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Map;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProductionReadinessChecksTest {

  private static final String REFLECTION_FREE_SERIALIZERS_PROPERTY =
      "quarkus.rest.jackson.optimization.enable-reflection-free-serializers";

  private ProductionReadinessChecks checks;

  @BeforeEach
  void setUp() {
    checks = new ProductionReadinessChecks();
  }

  @Test
  void reflectionFreeSerializersDisabledReturnsOk() {
    ProductionReadinessCheck result =
        checks.checkReflectionFreeSerializers(configWithReflectionFreeSerializers("false"));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void reflectionFreeSerializersUnsetReturnsOk() {
    ProductionReadinessCheck result =
        checks.checkReflectionFreeSerializers(configWithReflectionFreeSerializers(null));

    assertThat(result.ready()).isTrue();
  }

  @Test
  void reflectionFreeSerializersEnabledReturnsSevereError() {
    ProductionReadinessCheck result =
        checks.checkReflectionFreeSerializers(configWithReflectionFreeSerializers("true"));

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.offendingProperty()).isEqualTo(REFLECTION_FREE_SERIALIZERS_PROPERTY);
              assertThat(error.severe()).isTrue();
            });
  }

  private static Config configWithReflectionFreeSerializers(String value) {
    Config config = mock(Config.class);
    ConfigValue configValue = mock(ConfigValue.class);
    when(config.getConfigValue(REFLECTION_FREE_SERIALIZERS_PROPERTY)).thenReturn(configValue);
    when(configValue.getValue()).thenReturn(value);
    return config;
  }

  private static final String ISSUERS_KEY =
      FeatureConfiguration.SUPPORTED_S3_CREDENTIAL_ISSUERS.key();

  private static FeaturesConfiguration featuresConfig(
      Map<String, String> defaults, Map<String, RealmOverridable.RealmOverrides> realmOverrides) {
    // CALLS_REAL_METHODS keeps the interface's default parseDefaults/parseRealmOverrides working.
    FeaturesConfiguration config =
        mock(FeaturesConfiguration.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    when(config.defaults()).thenReturn(defaults);
    when(config.realmOverrides()).thenReturn(realmOverrides);
    return config;
  }

  private static RealmOverridable.RealmOverrides overrides(Map<String, String> values) {
    RealmOverridable.RealmOverrides realmOverrides = mock(RealmOverridable.RealmOverrides.class);
    when(realmOverrides.overrides()).thenReturn(values);
    return realmOverrides;
  }

  @Test
  void knownIssuerNamesAreReady() {
    ProductionReadinessCheck result =
        checks.checkS3CredentialIssuers(
            featuresConfig(
                Map.of(ISSUERS_KEY, "[\"STS\",\"CLOUDFLARE_R2\"]"),
                Map.of("r1", overrides(Map.of(ISSUERS_KEY, "[\"STS\"]")))));
    assertThat(result.ready()).isTrue();
  }

  @Test
  void unknownIssuerNameInDefaultsIsSevere() {
    ProductionReadinessCheck result =
        checks.checkS3CredentialIssuers(
            featuresConfig(Map.of(ISSUERS_KEY, "[\"STS\",\"BOGUS\"]"), Map.of()));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.severe()).isTrue();
              assertThat(error.offendingProperty())
                  .isEqualTo("polaris.features.\"" + ISSUERS_KEY + "\"");
              assertThat(error.message())
                  .contains("BOGUS")
                  .contains("STS")
                  .contains("CLOUDFLARE_R2");
            });
  }

  @Test
  void unknownIssuerNameInARealmOverrideNamesTheRealm() {
    ProductionReadinessCheck result =
        checks.checkS3CredentialIssuers(
            featuresConfig(Map.of(), Map.of("r1", overrides(Map.of(ISSUERS_KEY, "[\"NOPE\"]")))));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.severe()).isTrue();
              assertThat(error.offendingProperty()).contains("r1").contains(ISSUERS_KEY);
              assertThat(error.message()).contains("NOPE");
            });
  }
}
