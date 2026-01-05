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

import org.junit.jupiter.api.Test;

class PolarisEventTest {

  private static final String TEST_REALM = "test-realm";
  private static final String TEST_CATALOG = "my-catalog";
  private static final String TEST_TABLE = "my-table";

  private static final PolarisEventMetadata TEST_METADATA =
      ImmutablePolarisEventMetadata.builder().realmId(TEST_REALM).build();

  @Test
  void testBuilderCreatesEvent() {
    PolarisEvent event =
        PolarisEvent.builder(PolarisEventType.BEFORE_CREATE_TABLE, TEST_METADATA)
            .attribute(EventAttributes.CATALOG_NAME, TEST_CATALOG)
            .attribute(EventAttributes.TABLE_NAME, TEST_TABLE)
            .build();

    assertThat(event.type()).isEqualTo(PolarisEventType.BEFORE_CREATE_TABLE);
    assertThat(event.metadata()).isEqualTo(TEST_METADATA);
    assertThat(event.requiredAttribute(EventAttributes.CATALOG_NAME)).isEqualTo(TEST_CATALOG);
    assertThat(event.requiredAttribute(EventAttributes.TABLE_NAME)).isEqualTo(TEST_TABLE);
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
            .attribute(EventAttributes.CATALOG_NAME, TEST_CATALOG)
            .build();

    assertThat(event.hasAttribute(EventAttributes.CATALOG_NAME)).isTrue();
    assertThat(event.hasAttribute(EventAttributes.TABLE_NAME)).isFalse();
  }

  @Test
  void testAttributesReturnsUnmodifiableMap() {
    PolarisEvent event =
        PolarisEvent.builder(PolarisEventType.BEFORE_CREATE_TABLE, TEST_METADATA)
            .attribute(EventAttributes.CATALOG_NAME, TEST_CATALOG)
            .build();

    assertThat(event.attributes()).hasSize(1);
    assertThat(event.attributes().get(EventAttributes.CATALOG_NAME)).isEqualTo(TEST_CATALOG);
    assertThatThrownBy(() -> event.attributes().put(EventAttributes.TABLE_NAME, TEST_TABLE))
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
            .attribute(EventAttributes.CATALOG_NAME, TEST_CATALOG)
            .build();

    assertThat(event.requiredAttribute(EventAttributes.CATALOG_NAME)).isEqualTo(TEST_CATALOG);
  }

  @Test
  void testRequiredAttributeThrowsForMissingKey() {
    PolarisEvent event =
        PolarisEvent.builder(PolarisEventType.BEFORE_CREATE_TABLE, TEST_METADATA).build();

    assertThatThrownBy(() -> event.requiredAttribute(EventAttributes.CATALOG_NAME))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Required attribute")
        .hasMessageContaining("catalog_name")
        .hasMessageContaining("not found")
        .hasMessageContaining("BEFORE_CREATE_TABLE");
  }
}
