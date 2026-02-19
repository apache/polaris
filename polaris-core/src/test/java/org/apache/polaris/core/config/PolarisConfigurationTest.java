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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PolarisConfigurationTest {

  private <T> PolarisConfiguration<T> cfg(T value) {
    return PolarisConfiguration.<T>builder()
        .key("TEST-CONFIG-" + UUID.randomUUID())
        .description("test")
        .defaultValue(value)
        .buildFeatureConfiguration();
  }

  private <T> T resolve(T value, PolarisConfiguration<T> cfg) {
    return cfg.resolveValue(name -> value, name -> value);
  }

  private <T> T resolve(PolarisConfiguration<T> cfg) {
    return cfg.resolveValue(name -> null, name -> null);
  }

  private <T> T resolveString(String value, PolarisConfiguration<T> cfg) {
    return cfg.resolveValue(name -> value, name -> value);
  }

  @Test
  void typeCastSameType() {
    assertThat(resolve("str", cfg("test"))).isEqualTo("str");
    assertThat(resolve(1, cfg(2))).isEqualTo(1);
    assertThat(resolve(2L, cfg(1L))).isEqualTo(2L);
    assertThat(resolve(true, cfg(false))).isEqualTo(true);
    assertThat(resolve(1.2f, cfg(0.0f))).isEqualTo(1.2f);
    assertThat(resolve(3.4d, cfg(0.1d))).isEqualTo(3.4d);
    assertThat(resolve(List.of("1", "2"), cfg(List.of("3")))).isEqualTo(List.of("1", "2"));
  }

  @Test
  void typeCastFromString() {
    assertThat(resolveString("str", cfg("test"))).isEqualTo("str");
    assertThat(resolveString("1", cfg(2))).isEqualTo(1);
    assertThat(resolveString("2", cfg(1L))).isEqualTo(2L);
    assertThat(resolveString("true", cfg(false))).isEqualTo(true);
    assertThat(resolveString("1.2", cfg(0.0f))).isEqualTo(1.2f);
    assertThat(resolveString("3.4", cfg(0.1d))).isEqualTo(3.4d);
  }

  @Test
  public void testInvalidCast() {
    assertThatThrownBy(() -> resolveString("str", cfg(1)))
        .isInstanceOf(NumberFormatException.class);
    assertThatThrownBy(() -> resolveString("str", cfg(2L)))
        .isInstanceOf(NumberFormatException.class);
    assertThatThrownBy(() -> resolveString("str", cfg(0.1f)))
        .isInstanceOf(NumberFormatException.class);
    assertThatThrownBy(() -> resolveString("str", cfg(0.2d)))
        .isInstanceOf(NumberFormatException.class);

    assertThatThrownBy(() -> resolveString("str", cfg(List.of("1", "2"))))
        .isInstanceOf(ClassCastException.class);
  }

  @Test
  void typedDefaults() {
    assertThat(resolve(cfg("test"))).isEqualTo("test");
    assertThat(resolve(cfg(1))).isEqualTo(1);
    assertThat(resolve(cfg(2L))).isEqualTo(2L);
    assertThat(resolve(cfg(true))).isEqualTo(true);
    assertThat(resolve(cfg(1.2f))).isEqualTo(1.2f);
    assertThat(resolve(cfg(3.4d))).isEqualTo(3.4d);
    assertThat(resolve(cfg(List.of("3", "4")))).isEqualTo(List.of("3", "4"));
  }

  @Test
  void catalogConfigUnsafe() {
    @SuppressWarnings("removal")
    FeatureConfiguration<String> cfg =
        PolarisConfiguration.<String>builder()
            .key("TEST-catalogConfigUnsafe-" + UUID.randomUUID())
            .description("test")
            .defaultValue("def1")
            .catalogConfigUnsafe("unsafe1")
            .buildFeatureConfiguration();

    assertThat(cfg.resolveValue(name -> null, name -> null)).isEqualTo("def1");
    assertThat(cfg.resolveValue(name -> null, Map.of("unsafe1", "old2")::get)).isEqualTo("old2");
  }

  @Test
  void lookupPrecedence() {
    String key = "TEST-PRECEDENCE-" + UUID.randomUUID();
    FeatureConfiguration<String> cfg =
        PolarisConfiguration.<String>builder()
            .key(key)
            .description("test")
            .defaultValue("def1")
            .catalogConfig("polaris.config.test1")
            .legacyCatalogConfig("legacy2")
            .buildFeatureConfiguration();

    assertThat(cfg.resolveValue(name -> "glob", name -> "cat123")).isEqualTo("cat123");
    assertThat(cfg.resolveValue(name -> "glob", name -> null)).isEqualTo("glob");
    assertThat(cfg.resolveValue(name -> null, name -> null)).isEqualTo("def1");
    assertThat(cfg.resolveValue(Map.of(key, "glob")::get, name -> null)).isEqualTo("glob");
    assertThat(
            cfg.resolveValue(
                Map.of(key, "glob0")::get, Map.of("polaris.config.test1", "cat1")::get))
        .isEqualTo("cat1");
    assertThat(cfg.resolveValue(name -> null, Map.of("legacy2", "old2")::get)).isEqualTo("old2");
    assertThat(
            cfg.resolveValue(
                name -> null, Map.of("polaris.config.test1", "cat1", "legacy2", "old2")::get))
        .isEqualTo("cat1");
  }
}
