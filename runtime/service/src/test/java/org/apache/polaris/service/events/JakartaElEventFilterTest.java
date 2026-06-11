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

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class JakartaElEventFilterTest {

  private static final String REALM_ID = "realm1";
  private static final String CATALOG_NAME = "catalog1";
  private static final String TABLE_NAME = "sales_orders";

  @ParameterizedTest
  @MethodSource
  void testIncludeExclude(String include, String exclude, boolean expected) {
    assertThat(new JakartaElEventFilter(include, exclude).test(sampleEvent())).isEqualTo(expected);
  }

  static Stream<Arguments> testIncludeExclude() {
    return Stream.of(
        Arguments.of("metadata.realmId() == 'realm1'", null, true),
        Arguments.of("metadata.realmId() == 'realm2'", null, false),
        Arguments.of("attributes.catalog_name == 'catalog1'", null, true),
        Arguments.of("attributes['table_name'] == 'sales_orders'", null, true),
        Arguments.of("!attributes.table_name.startsWith('tmp_')", null, true),
        Arguments.of("type.name() == 'BEFORE_CREATE_TABLE'", null, true),
        Arguments.of(
            "metadata.realmId() == 'realm1' && attributes.catalog_name == 'catalog1'", null, true),
        Arguments.of(
            "metadata.realmId() == 'realm1' && attributes.catalog_name == 'catalog2'", null, false),
        Arguments.of(
            "${metadata.realmId() == 'realm1' && attributes.catalog_name == 'catalog1'}",
            null,
            true),
        Arguments.of(null, "attributes.table_name.startsWith('tmp_')", true),
        Arguments.of(null, "attributes.table_name == 'sales_orders'", false),
        Arguments.of(
            "metadata.realmId() == 'realm1'", "attributes.table_name.startsWith('tmp_')", true),
        Arguments.of(
            "metadata.realmId() == 'realm1'", "attributes.catalog_name == 'catalog1'", false),
        Arguments.of(
            "metadata.realmId() == 'realm2'", "attributes.table_name.startsWith('tmp_')", false),
        Arguments.of(
            "metadata.realmId() == 'realm1'", "${attributes.table_name == 'sales_orders'}", false));
  }

  @Test
  @SuppressWarnings("DataFlowIssue")
  void testAtLeastOneIncludeOrExcludeIsRequired() {
    assertThatThrownBy(() -> new JakartaElEventFilter(null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("At least one include or exclude expression is required");
  }

  private static PolarisEvent sampleEvent() {
    return new PolarisEvent(
        PolarisEventType.BEFORE_CREATE_TABLE,
        ImmutablePolarisEventMetadata.builder().realmId(REALM_ID).build(),
        new EventAttributeMap()
            .put(EventAttributes.CATALOG_NAME, CATALOG_NAME)
            .put(EventAttributes.TABLE_NAME, TABLE_NAME));
  }
}
