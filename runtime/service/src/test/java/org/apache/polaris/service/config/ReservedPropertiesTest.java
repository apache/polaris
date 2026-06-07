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
}
