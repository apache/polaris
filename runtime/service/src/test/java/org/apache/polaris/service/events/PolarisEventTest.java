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
package org.apache.polaris.service.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class PolarisEventTest {

  private static final PolarisEventMetadata TEST_METADATA =
      ImmutablePolarisEventMetadata.builder().realmId("test-realm").build();

  @Test
  void testBuilderCreatesEvent() {
    PolarisEvent event =
        PolarisEvent.builder(PolarisEventType.BEFORE_CREATE_TABLE, TEST_METADATA)
            .attribute(EventAttributes.CATALOG_NAME, "my-catalog")
            .attribute(EventAttributes.TABLE_NAME, "my-table")
            .build();

    assertThat(event.type()).isEqualTo(PolarisEventType.BEFORE_CREATE_TABLE);
    assertThat(event.metadata()).isEqualTo(TEST_METADATA);
    assertThat(event.attribute(EventAttributes.CATALOG_NAME)).isEqualTo(Optional.of("my-catalog"));
    assertThat(event.attribute(EventAttributes.TABLE_NAME)).isEqualTo(Optional.of("my-table"));
  }

  @Test
  void testAttributeReturnsEmptyForMissingKey() {
    PolarisEvent event =
        PolarisEvent.builder(PolarisEventType.BEFORE_CREATE_TABLE, TEST_METADATA).build();

    assertThat(event.attribute(EventAttributes.CATALOG_NAME)).isEmpty();
  }

  @Test
  void testHasAttribute() {
    PolarisEvent event =
        PolarisEvent.builder(PolarisEventType.BEFORE_CREATE_TABLE, TEST_METADATA)
            .attribute(EventAttributes.CATALOG_NAME, "my-catalog")
            .build();

    assertThat(event.hasAttribute(EventAttributes.CATALOG_NAME)).isTrue();
    assertThat(event.hasAttribute(EventAttributes.TABLE_NAME)).isFalse();
  }

  @Test
  void testAttributesReturnsUnmodifiableMap() {
    PolarisEvent event =
        PolarisEvent.builder(PolarisEventType.BEFORE_CREATE_TABLE, TEST_METADATA)
            .attribute(EventAttributes.CATALOG_NAME, "my-catalog")
            .build();

    assertThat(event.attributes()).hasSize(1);
    assertThat(event.attributes().get(EventAttributes.CATALOG_NAME)).isEqualTo("my-catalog");
    assertThatThrownBy(() -> event.attributes().put(EventAttributes.TABLE_NAME, "table"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void testNullAttributeValueIsIgnored() {
    PolarisEvent event =
        PolarisEvent.builder(PolarisEventType.BEFORE_CREATE_TABLE, TEST_METADATA)
            .attribute(EventAttributes.CATALOG_NAME, null)
            .build();

    assertThat(event.hasAttribute(EventAttributes.CATALOG_NAME)).isFalse();
    assertThat(event.attributes()).isEmpty();
  }

  @Test
  void testRequiredAttributeReturnsValue() {
    PolarisEvent event =
        PolarisEvent.builder(PolarisEventType.BEFORE_CREATE_TABLE, TEST_METADATA)
            .attribute(EventAttributes.CATALOG_NAME, "my-catalog")
            .build();

    assertThat(event.requiredAttribute(EventAttributes.CATALOG_NAME)).isEqualTo("my-catalog");
  }

  @Test
  void testRequiredAttributeThrowsForMissingKey() {
    PolarisEvent event =
        PolarisEvent.builder(PolarisEventType.BEFORE_CREATE_TABLE, TEST_METADATA).build();

    assertThatThrownBy(() -> event.requiredAttribute(EventAttributes.CATALOG_NAME))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("catalog_name")
        .hasMessageContaining("BEFORE_CREATE_TABLE");
  }
}
