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
package org.apache.polaris.service.quarkus.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Map;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.service.config.DefaultConfigurationStore;
import org.apache.polaris.service.config.FeaturesConfiguration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@QuarkusTest
@TestProfile(DefaultConfigurationStoreTest.Profile.class)
public class DefaultConfigurationStoreTest {
  // the key whose value is set to `false` for all realms
  private static final String falseByDefaultKey = "ALLOW_SPECIFYING_FILE_IO_IMPL";
  // the key whose value is set to `true` for all realms
  private static final String trueByDefaultKey = "ENABLE_GENERIC_TABLES";
  private static final String realmOne = "realm1";
  private static final String realmTwo = "realm2";
  private static final RealmContext realmOneContext = () -> realmOne;
  private static final RealmContext realmTwoContext = () -> realmTwo;

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.realm-context.realms",
          "realm1,realm2",
          String.format("polaris.features.\"%s\"", trueByDefaultKey),
          "true",
          String.format("polaris.features.\"%s\"", falseByDefaultKey),
          "false",
          String.format(
              "polaris.features.realm-overrides.\"%s\".\"%s\"", realmOne, falseByDefaultKey),
          "true",
          String.format(
              "polaris.features.realm-overrides.\"%s\".\"%s\"", realmTwo, trueByDefaultKey),
          "false");
    }
  }

  private RealmContext realmContext;

  @Inject PolarisConfigurationStore configurationStore;
  @Inject FeaturesConfiguration featuresConfiguration;

  @BeforeEach
  public void before(TestInfo testInfo) {
    String realmName =
        "realm_%s_%s"
            .formatted(
                testInfo.getTestMethod().map(java.lang.reflect.Method::getName).orElse("test"),
                System.nanoTime());
    realmContext = () -> realmName;
  }

  @Test
  public void testGetConfiguration() {
    Object value = configurationStore.getConfiguration(realmContext, "missingKeyWithoutDefault");
    assertThat(value).isNull();
    Object defaultValue =
        configurationStore.getConfiguration(realmContext, "missingKeyWithDefault", "defaultValue");
    assertThat(defaultValue).isEqualTo("defaultValue");

    // the falseByDefaultKey is set to false for all realms in Profile.getConfigOverrides
    assertThat((Boolean) configurationStore.getConfiguration(realmContext, falseByDefaultKey))
        .isFalse();
    // the trueByDefaultKey is set to true for all realms in Profile.getConfigOverrides
    assertThat((Boolean) configurationStore.getConfiguration(realmContext, trueByDefaultKey))
        .isTrue();
  }

  @Test
  public void testGetRealmConfiguration() {
    // check the realmOne configuration
    // the falseByDefaultKey is set to `false` for all realms, but overwrite with value `true` for
    // realmOne.
    assertThat((Boolean) configurationStore.getConfiguration(realmOneContext, falseByDefaultKey))
        .isTrue();
    // the trueByDefaultKey is set to `false` for all realms, no overwrite for realmOne
    assertThat((Boolean) configurationStore.getConfiguration(realmOneContext, trueByDefaultKey))
        .isTrue();

    // check the realmTwo configuration
    // the falseByDefaultKey is set to `false` for all realms, no overwrite for realmTwo
    assertThat((Boolean) configurationStore.getConfiguration(realmTwoContext, falseByDefaultKey))
        .isFalse();
    // the trueByDefaultKey is set to `false` for all realms, and overwrite with value `false` for
    // realmTwo
    assertThat((Boolean) configurationStore.getConfiguration(realmTwoContext, trueByDefaultKey))
        .isFalse();
  }

  @Test
  void testGetConfigurationWithRealm() {
    // the falseByDefaultKey is set to `false` for all realms, but overwrite with value `true` for
    // realmOne.
    assertThat((Boolean) configurationStore.getConfiguration(realmOneContext, falseByDefaultKey))
        .isTrue();
    // the trueByDefaultKey is set to `false` for all realms, no overwrite for realmOne
    assertThat((Boolean) configurationStore.getConfiguration(realmOneContext, trueByDefaultKey))
        .isTrue();

    // the falseByDefaultKey is set to `false` for all realms, no overwrite for realmTwo
    assertThat((Boolean) configurationStore.getConfiguration(realmTwoContext, falseByDefaultKey))
        .isFalse();
    // the trueByDefaultKey is set to `false` for all realms, and overwrite with value `false` for
    // realmTwo
    assertThat((Boolean) configurationStore.getConfiguration(realmTwoContext, trueByDefaultKey))
        .isFalse();
  }

  @Test
  public void testInjectedConfigurationStore() {
    // the default value for trueByDefaultKey is `true`
    Boolean featureDefaultValue =
        configurationStore.getConfiguration(realmContext, trueByDefaultKey);
    assertThat(featureDefaultValue).isTrue();

    // the value for falseByDefaultKey is `false`, and no realm override for realmTwo
    Boolean realmTwoValue = configurationStore.getConfiguration(realmTwoContext, falseByDefaultKey);
    assertThat(realmTwoValue).isFalse();

    // Now, realmOne override falseByDefaultKey to `True`
    Boolean realmOneValue = configurationStore.getConfiguration(realmOneContext, falseByDefaultKey);
    assertThat(realmOneValue).isTrue();

    assertThat(configurationStore).isInstanceOf(DefaultConfigurationStore.class);
  }

  @Test
  public void testInjectedFeaturesConfiguration() {
    assertThat(featuresConfiguration).isInstanceOf(QuarkusResolvedFeaturesConfiguration.class);

    assertThat(featuresConfiguration.defaults())
        .containsKeys(falseByDefaultKey, trueByDefaultKey)
        .allSatisfy((key, value) -> assertThat(value).doesNotContain(realmOne));

    assertThat(featuresConfiguration.realmOverrides()).hasSize(2);
    assertThat(featuresConfiguration.realmOverrides()).containsKey(realmOne);

    assertThat(featuresConfiguration.realmOverrides().get(realmOne).overrides()).hasSize(1);
    assertThat(featuresConfiguration.realmOverrides().get(realmOne).overrides())
        .containsKey(falseByDefaultKey);

    assertThat(featuresConfiguration.realmOverrides().get(realmTwo).overrides()).hasSize(1);
    assertThat(featuresConfiguration.realmOverrides().get(realmTwo).overrides())
        .containsKey(trueByDefaultKey);
  }

  @Test
  public void testRegisterAndUseFeatureConfigurations() {
    String prefix = "testRegisterAndUseFeatureConfigurations";

    FeatureConfiguration<Boolean> safeConfig =
        FeatureConfiguration.<Boolean>builder()
            .key(String.format("%s_safe", prefix))
            .catalogConfig(String.format("polaris.config.%s.safe", prefix))
            .defaultValue(true)
            .description(prefix)
            .buildFeatureConfiguration();

    FeatureConfiguration<Boolean> unsafeConfig =
        FeatureConfiguration.<Boolean>builder()
            .key(String.format("%s_unsafe", prefix))
            .catalogConfigUnsafe(String.format("%s.unsafe", prefix))
            .defaultValue(true)
            .description(prefix)
            .buildFeatureConfiguration();

    FeatureConfiguration<Boolean> bothConfig =
        FeatureConfiguration.<Boolean>builder()
            .key(String.format("%s_both", prefix))
            .catalogConfig(String.format("polaris.config.%s.both", prefix))
            .catalogConfigUnsafe(String.format("%s.both", prefix))
            .defaultValue(true)
            .description(prefix)
            .buildFeatureConfiguration();

    CatalogEntity catalog = new CatalogEntity.Builder().build();

    Assertions.assertThat(configurationStore.getConfiguration(realmContext, catalog, safeConfig))
        .isTrue();

    Assertions.assertThat(configurationStore.getConfiguration(realmContext, catalog, unsafeConfig))
        .isTrue();

    Assertions.assertThat(configurationStore.getConfiguration(realmContext, catalog, bothConfig))
        .isTrue();
  }
}
