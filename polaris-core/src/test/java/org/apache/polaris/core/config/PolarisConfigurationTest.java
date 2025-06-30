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

import org.junit.jupiter.api.Test;

public class PolarisConfigurationTest {

  @Test
  void testFeatureConfigurationWithDeprecatedKey() {
    FeatureConfiguration<Boolean> config =
        PolarisConfiguration.<Boolean>builder()
            .key("FEATURE_CONFIG_WITH_DEPRECATED_KEY")
            .deprecatedKey("OLD_FEATURE_CONFIG_WITH_DEPRECATED_KEY")
            .description("Test configuration with deprecated key")
            .defaultValue(true)
            .buildFeatureConfiguration();

    assertThat(config.hasDeprecatedKey()).isTrue();
    assertThat(config.deprecatedKey()).isEqualTo("OLD_FEATURE_CONFIG_WITH_DEPRECATED_KEY");
    assertThat(config.key).isEqualTo("FEATURE_CONFIG_WITH_DEPRECATED_KEY");
    assertThat(config.description).isEqualTo("Test configuration with deprecated key");
    assertThat(config.defaultValue).isTrue();
  }

  @Test
  void testBehaviorChangeConfigurationWithDeprecatedKey() {
    BehaviorChangeConfiguration<String> config =
        PolarisConfiguration.<String>builder()
            .key("BEHAVIOR_CONFIG_WITH_DEPRECATED_KEY")
            .deprecatedKey("OLD_BEHAVIOR_CONFIG_WITH_DEPRECATED_KEY")
            .description("Test behavior configuration with deprecated key")
            .defaultValue("test-value")
            .buildBehaviorChangeConfiguration();

    assertThat(config.hasDeprecatedKey()).isTrue();
    assertThat(config.deprecatedKey()).isEqualTo("OLD_BEHAVIOR_CONFIG_WITH_DEPRECATED_KEY");
    assertThat(config.key).isEqualTo("BEHAVIOR_CONFIG_WITH_DEPRECATED_KEY");
    assertThat(config.defaultValue).isEqualTo("test-value");
  }

  @Test
  void testDuplicateKeyValidation() {
    PolarisConfiguration.<Boolean>builder()
        .key("DUPLICATE_KEY_TEST_1")
        .description("First configuration")
        .defaultValue(true)
        .buildFeatureConfiguration();

    assertThatThrownBy(
            () ->
                PolarisConfiguration.<Boolean>builder()
                    .key("DUPLICATE_KEY_TEST_1")
                    .description("Second configuration")
                    .defaultValue(false)
                    .buildFeatureConfiguration())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Config 'DUPLICATE_KEY_TEST_1' is already in use");
  }

  @Test
  void testDuplicateDeprecatedKeyValidation() {
    PolarisConfiguration.<Boolean>builder()
        .key("NEW_KEY_DEPRECATED_TEST_2")
        .deprecatedKey("DEPRECATED_KEY_TEST_2")
        .description("First configuration with deprecated key")
        .defaultValue(true)
        .buildFeatureConfiguration();

    assertThatThrownBy(
            () ->
                PolarisConfiguration.<Boolean>builder()
                    .key("ANOTHER_NEW_KEY_2")
                    .deprecatedKey("DEPRECATED_KEY_TEST_2")
                    .description("Second configuration with same deprecated key")
                    .defaultValue(false)
                    .buildFeatureConfiguration())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Config 'DEPRECATED_KEY_TEST_2' is already in use");
  }

  @Test
  void testNewKeyMatchingExistingDeprecatedKeyValidation() {
    PolarisConfiguration.<Boolean>builder()
        .key("ORIGINAL_KEY_3")
        .deprecatedKey("OLD_DEPRECATED_KEY_3")
        .description("First configuration with deprecated key")
        .defaultValue(true)
        .buildFeatureConfiguration();

    assertThatThrownBy(
            () ->
                PolarisConfiguration.<Boolean>builder()
                    .key("OLD_DEPRECATED_KEY_3")
                    .description("Configuration with key matching existing deprecated key")
                    .defaultValue(false)
                    .buildFeatureConfiguration())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Config 'OLD_DEPRECATED_KEY_3' is already in use");
  }

  @Test
  void testDeprecatedKeyMatchingExistingKeyValidation() {
    PolarisConfiguration.<Boolean>builder()
        .key("EXISTING_KEY_4")
        .description("First configuration")
        .defaultValue(true)
        .buildFeatureConfiguration();

    assertThatThrownBy(
            () ->
                PolarisConfiguration.<Boolean>builder()
                    .key("NEW_KEY_FOR_VALIDATION_4")
                    .deprecatedKey("EXISTING_KEY_4")
                    .description("Configuration with deprecated key matching existing key")
                    .defaultValue(false)
                    .buildFeatureConfiguration())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Config 'EXISTING_KEY_4' is already in use");
  }
}
