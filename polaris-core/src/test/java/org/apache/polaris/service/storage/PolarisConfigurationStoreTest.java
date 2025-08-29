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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.polaris.core.config.BehaviorChangeConfiguration;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfiguration;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit test for the default behaviors of the PolarisConfigurationStore interface. */
public class PolarisConfigurationStoreTest {
  private final RealmContext testRealmContext = () -> "testRealm";

  @Test
  public void testConfigsCanBeCastedFromString() {
    List<PolarisConfiguration<?>> configs =
        List.of(
            buildConfig("bool", true),
            buildConfig("int", 12),
            buildConfig("long", 34L),
            buildConfig("double", 5.6D));

    PolarisConfigurationStore store =
        new PolarisConfigurationStore() {
          /**
           * Ad-hoc configuration store implementation that just returns the stringified version of
           * the config's default value
           */
          @SuppressWarnings("unchecked")
          @Override
          public <T> @Nullable T getConfiguration(@Nonnull RealmContext ctx, String configName) {
            for (PolarisConfiguration<?> c : configs) {
              if (c.key().equals(configName)) {
                return (T) String.valueOf(c.defaultValue());
              }
            }

            throw new IllegalStateException(
                String.format(
                    "Didn't find config value for %s, the test isn't set up correctly",
                    configName));
          }
        };

    // Ensure that we can fetch all the configs and that the value is what we expect, which
    // is the config's default value based on how we've implemented PolarisConfigurationStore above.
    for (PolarisConfiguration<?> c : configs) {
      Assertions.assertEquals(c.defaultValue(), store.getConfiguration(testRealmContext, c));
    }
  }

  @Test
  public void testInvalidCastThrowsException() {
    // Bool not included because Boolean.valueOf turns non-boolean strings to false
    List<PolarisConfiguration<?>> configs =
        List.of(buildConfig("int2", 12), buildConfig("long2", 34L), buildConfig("double2", 5.6D));

    PolarisConfigurationStore store =
        new PolarisConfigurationStore() {
          @SuppressWarnings("unchecked")
          @Override
          public <T> T getConfiguration(@Nonnull RealmContext ctx, String configName) {
            return (T) "abc123";
          }
        };

    for (PolarisConfiguration<?> c : configs) {
      Assertions.assertThrows(
          NumberFormatException.class, () -> store.getConfiguration(testRealmContext, c));
    }
  }

  private static <T> PolarisConfiguration<T> buildConfig(String key, T defaultValue) {
    return PolarisConfiguration.<T>builder()
        .key(key)
        .description("")
        .defaultValue(defaultValue)
        .buildFeatureConfiguration();
  }

  private static class PolarisConfigurationConsumer {

    private final RealmContext realmContext;
    private final PolarisConfigurationStore configurationStore;

    public PolarisConfigurationConsumer(
        RealmContext realmContext, PolarisConfigurationStore configurationStore) {
      this.realmContext = realmContext;
      this.configurationStore = configurationStore;
    }

    public <T> T consumeConfiguration(
        PolarisConfiguration<Boolean> config, Supplier<T> code, T defaultVal) {
      if (configurationStore.getConfiguration(realmContext, config)) {
        return code.get();
      }
      return defaultVal;
    }
  }

  @Test
  public void testBehaviorAndFeatureConfigs() {
    PolarisConfigurationConsumer consumer =
        new PolarisConfigurationConsumer(testRealmContext, new PolarisConfigurationStore() {});

    FeatureConfiguration<Boolean> featureConfig =
        PolarisConfiguration.<Boolean>builder()
            .key("example")
            .description("example")
            .defaultValue(true)
            .buildFeatureConfiguration();

    BehaviorChangeConfiguration<Boolean> behaviorChangeConfig =
        PolarisConfiguration.<Boolean>builder()
            .key("example2")
            .description("example")
            .defaultValue(true)
            .buildBehaviorChangeConfiguration();

    consumer.consumeConfiguration(behaviorChangeConfig, () -> 21, 22);

    consumer.consumeConfiguration(featureConfig, () -> 42, 43);
  }

  @Test
  public void testEntityOverrides() {
    @SuppressWarnings("deprecation")
    Function<Integer, FeatureConfiguration<String>> cfg =
        i ->
            PolarisConfiguration.<String>builder()
                .key("key" + i)
                .catalogConfig("polaris.config.catalog-key" + i)
                .catalogConfigUnsafe("legacy-key" + i)
                .description("")
                .defaultValue("test-default" + i)
                .buildFeatureConfiguration();

    PolarisConfigurationStore store =
        new PolarisConfigurationStore() {
          @Override
          public <T> @Nullable T getConfiguration(
              @Nonnull RealmContext realmContext, String configName) {
            //noinspection unchecked
            return (T) Map.of("key2", "config-value2").get(configName);
          }
        };

    CatalogEntity entity =
        new CatalogEntity.Builder()
            .addProperty("polaris.config.catalog-key3", "entity-new3")
            .addProperty("legacy-key4", "entity-legacy4")
            .build();

    Assertions.assertEquals(
        "test-default1", store.getConfiguration(testRealmContext, entity, cfg.apply(1)));
    Assertions.assertEquals(
        "config-value2", store.getConfiguration(testRealmContext, entity, cfg.apply(2)));
    Assertions.assertEquals(
        "entity-new3", store.getConfiguration(testRealmContext, entity, cfg.apply(3)));
    Assertions.assertEquals(
        "entity-legacy4", store.getConfiguration(testRealmContext, entity, cfg.apply(4)));
  }
}
