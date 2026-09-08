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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.junit.jupiter.api.Test;

class ReservedPropertiesTest {

  @Test
  void testReservedPropertyThrows() {
    ReservedProperties reservedProperties = new ReservedProperties() {};

    assertThatThrownBy(
            () -> reservedProperties.removeReservedProperties(Map.of("polaris.test", "value")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("polaris.test");
  }

  @Test
  void testNoneAllowsReservedProperties() {
    Map<String, String> properties = Map.of("polaris.test", "value");

    Map<String, String> result = ReservedProperties.NONE.removeReservedProperties(properties);

    assertThat(result).containsEntry("polaris.test", "value");
  }

  @Test
  void testReservedPropertyFilteredWhenThrowDisabled() {
    ReservedProperties reservedProperties =
        new ReservedProperties() {
          @Override
          public boolean shouldThrow() {
            return false;
          }
        };

    Map<String, String> result =
        reservedProperties.removeReservedProperties(Map.of("polaris.test", "value"));

    assertThat(result).isEmpty();
  }

  @Test
  void testUpdateRestoresExistingReservedProperty() {
    ReservedProperties reservedProperties =
        new ReservedProperties() {
          @Override
          public boolean shouldThrow() {
            return false;
          }
        };

    Map<String, String> result =
        reservedProperties.removeReservedPropertiesFromUpdate(
            Map.of("polaris.test", "original"), Map.of("polaris.test", "new"));

    assertThat(result).containsEntry("polaris.test", "original");
  }

  @Test
  void testRemoveReservedPropertiesList() {
    ReservedProperties reservedProperties =
        new ReservedProperties() {
          @Override
          public boolean shouldThrow() {
            return false;
          }
        };

    assertThat(reservedProperties.removeReservedProperties(List.of("polaris.test", "user.test")))
        .containsExactly("user.test");
  }

  @Test
  @SuppressWarnings({"deprecation", "removal"}) // exercises the deprecated flag's compat contract
  void testDeprecatedAddTrailingSlashConfigStaysAcceptedButIgnored() {
    // Regression for the accepted-but-ignored compatibility contract of the deprecated
    // ADD_TRAILING_SLASH_TO_LOCATION flag. The catalog-config key stays on the allowlist only
    // because the deprecated feature config still declares it. If that declaration is removed, this
    // key falls through to the reserved "polaris." prefix check and catalog create/update starts
    // returning 400 again -- including a plain read-modify-write of an existing catalog that still
    // carries the key. Referencing the constant here also forces the feature-config registry to
    // load and turns a future removal into a compile-time failure on this test.
    String key = FeatureConfiguration.ADD_TRAILING_SLASH_TO_LOCATION.catalogConfig();
    assertThat(key).isEqualTo("polaris.config.add-trailing-slash-to-location");
    ReservedProperties reservedProperties = new ReservedProperties() {};

    assertThat(reservedProperties.allowlist()).contains(key);

    // Create path: the property is accepted rather than rejected or stripped.
    assertThat(reservedProperties.removeReservedProperties(Map.of(key, "false")))
        .containsEntry(key, "false");

    // Read-modify-write path: GET returns the key, PUT it back unchanged must not be rejected.
    assertThat(
            reservedProperties.removeReservedPropertiesFromUpdate(
                Map.of(key, "false"), Map.of(key, "false")))
        .containsEntry(key, "false");
  }
}
