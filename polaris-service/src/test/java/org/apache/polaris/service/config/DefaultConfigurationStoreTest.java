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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class DefaultConfigurationStoreTest {
  @Test
  public void testGetConfiguration() {
    DefaultConfigurationStore defaultConfigurationStore =
        new DefaultConfigurationStore(Map.of("key1", 1, "key2", "value"));
    InMemoryPolarisMetaStoreManagerFactory metastoreFactory =
        new InMemoryPolarisMetaStoreManagerFactory();
    PolarisCallContext callCtx =
        new PolarisCallContext(
            metastoreFactory.getOrCreateSessionSupplier(() -> "realm1").get(),
            new PolarisDefaultDiagServiceImpl());
    Object value = defaultConfigurationStore.getConfiguration(callCtx, "missingKeyWithoutDefault");
    assertThat(value).isNull();
    Object defaultValue =
        defaultConfigurationStore.getConfiguration(
            callCtx, "missingKeyWithDefault", "defaultValue");
    assertThat(defaultValue).isEqualTo("defaultValue");
    Integer keyOne = defaultConfigurationStore.getConfiguration(callCtx, "key1");
    assertThat(keyOne).isEqualTo(1);
    String keyTwo = defaultConfigurationStore.getConfiguration(callCtx, "key2");
    assertThat(keyTwo).isEqualTo("value");
  }

  @Test
  public void testGetRealmConfiguration() {
    int defaultKeyOneValue = 1;
    String defaultKeyTwoValue = "value";

    int realm1KeyOneValue = 2;
    int realm2KeyOneValue = 3;
    String realm2KeyTwoValue = "value3";
    DefaultConfigurationStore defaultConfigurationStore =
        new DefaultConfigurationStore(
            Map.of("key1", defaultKeyOneValue, "key2", defaultKeyTwoValue),
            Map.of(
                "realm1",
                Map.of("key1", realm1KeyOneValue),
                "realm2",
                Map.of("key1", realm2KeyOneValue, "key2", realm2KeyTwoValue)),
            new NoOpDynamicFeatureConfigResolver());
    InMemoryPolarisMetaStoreManagerFactory metastoreFactory =
        new InMemoryPolarisMetaStoreManagerFactory();

    // check realm1 values
    PolarisCallContext realm1Ctx =
        new PolarisCallContext(
            metastoreFactory.getOrCreateSessionSupplier(() -> "realm1").get(),
            new PolarisDefaultDiagServiceImpl());
    Object value =
        defaultConfigurationStore.getConfiguration(realm1Ctx, "missingKeyWithoutDefault");
    assertThat(value).isNull();
    Object defaultValue =
        defaultConfigurationStore.getConfiguration(
            realm1Ctx, "missingKeyWithDefault", "defaultValue");
    assertThat(defaultValue).isEqualTo("defaultValue");
    Integer keyOneRealm1 = defaultConfigurationStore.getConfiguration(realm1Ctx, "key1");
    assertThat(keyOneRealm1).isEqualTo(realm1KeyOneValue);
    String keyTwoRealm1 = defaultConfigurationStore.getConfiguration(realm1Ctx, "key2");
    assertThat(keyTwoRealm1).isEqualTo(defaultKeyTwoValue);

    // check realm2 values
    PolarisCallContext realm2Ctx =
        new PolarisCallContext(
            metastoreFactory.getOrCreateSessionSupplier(() -> "realm2").get(),
            new PolarisDefaultDiagServiceImpl());
    Integer keyOneRealm2 = defaultConfigurationStore.getConfiguration(realm2Ctx, "key1");
    assertThat(keyOneRealm2).isEqualTo(realm2KeyOneValue);
    String keyTwoRealm2 = defaultConfigurationStore.getConfiguration(realm2Ctx, "key2");
    assertThat(keyTwoRealm2).isEqualTo(realm2KeyTwoValue);

    // realm3 has no realm-overrides, so just returns default values
    PolarisCallContext realm3Ctx =
        new PolarisCallContext(
            metastoreFactory.getOrCreateSessionSupplier(() -> "realm3").get(),
            new PolarisDefaultDiagServiceImpl());
    Integer keyOneRealm3 = defaultConfigurationStore.getConfiguration(realm3Ctx, "key1");
    assertThat(keyOneRealm3).isEqualTo(defaultKeyOneValue);
    String keyTwoRealm3 = defaultConfigurationStore.getConfiguration(realm3Ctx, "key2");
    assertThat(keyTwoRealm3).isEqualTo(defaultKeyTwoValue);
  }

  @Test
  public void testDynamicConfig() {
    InMemoryPolarisMetaStoreManagerFactory metastoreFactory =
        new InMemoryPolarisMetaStoreManagerFactory();
    PolarisCallContext polarisCtx =
        new PolarisCallContext(
            metastoreFactory.getOrCreateSessionSupplier(() -> "realm1").get(),
            new PolarisDefaultDiagServiceImpl());

    String key = "k1";
    Map<String, Object> staticConfig = Map.of(key, 10);

    assertThat(
            new DefaultConfigurationStore(staticConfig, Map.of(), k -> Optional.empty())
                .<Integer>getConfiguration(polarisCtx, key))
        .as("The DynamicFeatureConfigResolver always returns Optional.empty()")
        .isEqualTo(10);

    assertThat(
            new DefaultConfigurationStore(staticConfig, Map.of(), k -> Optional.of(5))
                .<Integer>getConfiguration(polarisCtx, key))
        .as("The DynamicFeatureConfigResolver always returns 5")
        .isEqualTo(5);
  }

  @ParameterizedTest
  @MethodSource("getTestConfigs")
  public void testPrecedenceIsDynamicThenStaticPerRealmThenStaticGlobal(TestConfig testConfig) {
    InMemoryPolarisMetaStoreManagerFactory metastoreFactory =
        new InMemoryPolarisMetaStoreManagerFactory();

    String realm = "realm1";
    PolarisCallContext polarisCtx =
        new PolarisCallContext(
            metastoreFactory.getOrCreateSessionSupplier(() -> realm).get(),
            new PolarisDefaultDiagServiceImpl());

    String key = "k1";

    Map<String, Object> staticConfig = new HashMap<>();
    if (testConfig.staticConfig != null) {
      staticConfig.put(key, testConfig.staticConfig);
    }

    Map<String, Object> realmConfig = new HashMap<>();
    if (testConfig.realmConfig != null) {
      realmConfig.put(key, testConfig.realmConfig);
    }

    DefaultConfigurationStore configStore =
        new DefaultConfigurationStore(
            staticConfig,
            Map.of(realm, realmConfig),
            (k) -> Optional.ofNullable(testConfig.dynamicConfig));
    assertThat(configStore.<Integer>getConfiguration(polarisCtx, key))
        .isEqualTo(testConfig.expectedValue);
  }

  private static Stream<TestConfig> getTestConfigs() {
    return Stream.of(
        new TestConfig(null, null, null, null),
        new TestConfig(5, null, null, 5),
        new TestConfig(5, 6, null, 6),
        new TestConfig(5, 6, 7, 7),
        new TestConfig(5, null, 7, 7),
        new TestConfig(null, null, 7, 7),
        new TestConfig(null, 6, 7, 7),
        new TestConfig(null, 6, null, 6));
  }

  public record TestConfig(
      Integer staticConfig, Integer realmConfig, Integer dynamicConfig, Integer expectedValue) {}
}
