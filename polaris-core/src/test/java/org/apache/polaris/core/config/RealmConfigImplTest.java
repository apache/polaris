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

package org.apache.polaris.core.config;

import static org.apache.polaris.core.config.RealmConfigurationSource.EMPTY_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.polaris.core.entity.CatalogEntity;
import org.junit.jupiter.api.Test;

class RealmConfigImplTest {

  private static final RealmConfigurationSource stringsSource =
      (rc, name) -> String.format("value-%s-%s", rc.getRealmIdentifier(), name);

  private RealmConfig strings(String realmName) {
    return new RealmConfigImpl(stringsSource, () -> realmName);
  }

  private RealmConfig fixed(Object value) {
    return new RealmConfigImpl((rc, name) -> value, () -> "test-realm");
  }

  private RealmConfig empty() {
    return new RealmConfigImpl(EMPTY_CONFIG, () -> "test-realm");
  }

  @Test
  @SuppressWarnings("removal")
  void legacyLookup() {
    assertThat((Object) strings("rc1").getConfig("c1")).isEqualTo("value-rc1-c1");
    assertThat((Object) strings("rc2").getConfig("c1", "default1")).isEqualTo("value-rc2-c1");
    assertThat((Object) empty().getConfig("c1", "default1")).isEqualTo("default1");
  }

  @Test
  void configPropertyOrder() {
    @SuppressWarnings("deprecation")
    FeatureConfiguration<String> cfg1 =
        PolarisConfiguration.<String>builder()
            .key("TEST-KEY1")
            .catalogConfig("polaris.config.cat-prop")
            .catalogConfigUnsafe("legacy-prop")
            .description("test")
            .defaultValue("default2")
            .buildFeatureConfiguration();
    assertThat(strings("rc1").getConfig(cfg1)).isEqualTo("value-rc1-TEST-KEY1");
    assertThat(strings("rc2").getConfig(cfg1, Map.of())).isEqualTo("value-rc2-TEST-KEY1");
    assertThat(strings("rc3").getConfig(cfg1, Map.of("polaris.config.cat-prop", "cat1")))
        .isEqualTo("cat1");
    assertThat(strings("rc3").getConfig(cfg1, Map.of("legacy-prop", "old2"))).isEqualTo("old2");
    assertThat(empty().getConfig(cfg1)).isEqualTo("default2");

    @SuppressWarnings("deprecation")
    FeatureConfiguration<String> cfg2 =
        PolarisConfiguration.<String>builder()
            .key("TEST-KEY2")
            .catalogConfigUnsafe("legacy-prop2")
            .description("test")
            .defaultValue("default2")
            .buildFeatureConfiguration();
    assertThat(strings("rc1").getConfig(cfg2)).isEqualTo("value-rc1-TEST-KEY2");
    assertThat(strings("rc2").getConfig(cfg2, Map.of())).isEqualTo("value-rc2-TEST-KEY2");
    assertThat(strings("rc3").getConfig(cfg2, Map.of("legacy-prop2", "old2"))).isEqualTo("old2");
    assertThat(empty().getConfig(cfg2)).isEqualTo("default2");

    FeatureConfiguration<String> cfg3 =
        PolarisConfiguration.<String>builder()
            .key("TEST-KEY3")
            .catalogConfig("polaris.config.cat-prop2")
            .description("test")
            .defaultValue("default2")
            .buildFeatureConfiguration();
    assertThat(strings("rc1").getConfig(cfg3)).isEqualTo("value-rc1-TEST-KEY3");
    assertThat(strings("rc2").getConfig(cfg3, Map.of())).isEqualTo("value-rc2-TEST-KEY3");
    assertThat(strings("rc3").getConfig(cfg3, Map.of("polaris.config.cat-prop2", "cat2")))
        .isEqualTo("cat2");
    assertThat(empty().getConfig(cfg3)).isEqualTo("default2");

    FeatureConfiguration<String> cfg4 =
        PolarisConfiguration.<String>builder()
            .key("TEST-KEY4")
            .description("test")
            .defaultValue("default2")
            .buildFeatureConfiguration();
    assertThat(strings("rc1").getConfig(cfg4)).isEqualTo("value-rc1-TEST-KEY4");
    assertThat(strings("rc2").getConfig(cfg4, Map.of())).isEqualTo("value-rc2-TEST-KEY4");
    assertThat(empty().getConfig(cfg4)).isEqualTo("default2");
  }

  @Test
  void entityProperties() {
    FeatureConfiguration<String> cfg =
        PolarisConfiguration.<String>builder()
            .key("TEST-ENTITY1")
            .catalogConfig("polaris.config.test-entity-prop")
            .description("test")
            .defaultValue("default2")
            .buildFeatureConfiguration();

    CatalogEntity entity =
        new CatalogEntity.Builder()
            .setProperties(Map.of("polaris.config.test-entity-prop", "entity2"))
            .build();

    assertThat(strings("rc1").getConfig(cfg)).isEqualTo("value-rc1-TEST-ENTITY1");
    assertThat(strings("rc2").getConfig(cfg, entity)).isEqualTo("entity2");
  }

  private <T> PolarisConfiguration<T> cfg(T value) {
    return PolarisConfiguration.<T>builder()
        .key("TEST-CAST-" + UUID.randomUUID().toString())
        .description("test")
        .defaultValue(value)
        .buildFeatureConfiguration();
  }

  @Test
  void typeCast() {
    assertThat(fixed("str").getConfig(cfg("test"))).isEqualTo("str");
    assertThat(fixed(1).getConfig(cfg(2))).isEqualTo(1);
    assertThat(fixed(2L).getConfig(cfg(1L))).isEqualTo(2L);
    assertThat(fixed(true).getConfig(cfg(false))).isEqualTo(true);
    assertThat(fixed(1.2f).getConfig(cfg(0.0f))).isEqualTo(1.2f);
    assertThat(fixed(3.4d).getConfig(cfg(0.1d))).isEqualTo(3.4d);
    assertThat(fixed(List.of("1", "2")).getConfig(cfg(List.of()))).isEqualTo(List.of("1", "2"));
  }

  @Test
  void typedDefaults() {
    assertThat(empty().getConfig(cfg("test"))).isEqualTo("test");
    assertThat(empty().getConfig(cfg(2))).isEqualTo(2);
    assertThat(empty().getConfig(cfg(1L))).isEqualTo(1L);
    assertThat(empty().getConfig(cfg(false))).isEqualTo(false);
    assertThat(empty().getConfig(cfg(0.1f))).isEqualTo(0.1f);
    assertThat(empty().getConfig(cfg(2.3d))).isEqualTo(2.3d);
    assertThat(empty().getConfig(cfg(List.of("3", "4")))).isEqualTo(List.of("3", "4"));
  }
}
