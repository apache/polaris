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

import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.apache.polaris.core.storage.aws.S3CredentialVendingMechanism;
import org.apache.polaris.service.storage.S3CredentialVendingMechanisms;
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

  private static final String MECHANISMS_KEY =
      FeatureConfiguration.SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS.key();

  private static S3CredentialVendingMechanisms installed(String... ids) {
    Map<String, S3CredentialVendingMechanism> mechanisms = new HashMap<>();
    for (String id : ids) {
      mechanisms.put(id, mock(S3CredentialVendingMechanism.class));
    }
    return new S3CredentialVendingMechanisms(mechanisms);
  }

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
  void everyAllowlistedMechanismInstalledIsReady() {
    ProductionReadinessCheck result =
        checks.checkS3CredentialVendingMechanisms(
            featuresConfig(
                Map.of(MECHANISMS_KEY, "[\"STS\"]"),
                Map.of("r1", overrides(Map.of(MECHANISMS_KEY, "[\"STS\"]")))),
            installed("STS"));
    assertThat(result.ready()).isTrue();
  }

  @Test
  void anAllowlistedUninstalledMechanismInDefaultsIsNonSevereAndNamesWhatIsAvailable() {
    ProductionReadinessCheck result =
        checks.checkS3CredentialVendingMechanisms(
            featuresConfig(Map.of(MECHANISMS_KEY, "[\"STS\",\"BOGUS\"]"), Map.of()),
            installed("STS"));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.severe()).isFalse();
              assertThat(error.offendingProperty())
                  .isEqualTo("polaris.features.\"" + MECHANISMS_KEY + "\"");
              assertThat(error.message()).contains("BOGUS").contains("STS");
            });
  }

  @Test
  void anUnconfiguredAllowlistChecksTheCodeDefault() {
    ProductionReadinessCheck result =
        checks.checkS3CredentialVendingMechanisms(
            featuresConfig(Map.of(), Map.of()), installed("DEFAULT"));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.severe()).isFalse();
              assertThat(error.offendingProperty())
                  .isEqualTo("polaris.features.\"" + MECHANISMS_KEY + "\"");
              assertThat(error.message()).contains("STS");
            });
  }

  @Test
  void anUnconfiguredAllowlistIsReadyWhenTheDefaultMechanismIsInstalled() {
    ProductionReadinessCheck result =
        checks.checkS3CredentialVendingMechanisms(
            featuresConfig(Map.of(), Map.of()), installed("STS", "DEFAULT"));
    assertThat(result.ready()).isTrue();
  }

  @Test
  void anAllowlistedUninstalledMechanismInARealmOverrideNamesTheRealm() {
    ProductionReadinessCheck result =
        checks.checkS3CredentialVendingMechanisms(
            featuresConfig(Map.of(), Map.of("r1", overrides(Map.of(MECHANISMS_KEY, "[\"NOPE\"]")))),
            installed("STS"));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.severe()).isFalse();
              assertThat(error.offendingProperty()).contains("r1").contains(MECHANISMS_KEY);
              assertThat(error.message()).contains("NOPE").contains("STS");
            });
  }

  @Test
  void aListedDefaultIsANonSevereWarningEvenThoughItIsInstalled() {
    ProductionReadinessCheck result =
        checks.checkS3CredentialVendingMechanisms(
            featuresConfig(Map.of(MECHANISMS_KEY, "[\"STS\",\"DEFAULT\"]"), Map.of()),
            installed("STS", "DEFAULT"));
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.severe()).isFalse();
              assertThat(error.message()).contains("DEFAULT").contains("has no effect");
            });
  }
}
