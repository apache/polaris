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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.iceberg.catalog.Namespace;
import org.apache.polaris.core.admin.model.Catalog;
import org.junit.jupiter.api.Test;

class DefaultEventSanitizerTest {

  private final DefaultEventSanitizer sanitizer =
      new DefaultEventSanitizer(DefaultEventSanitizer.DEFAULT_DENYLIST);

  @Test
  void shouldDropDenylistedAttributes() {
    Catalog catalog = mock(Catalog.class);
    when(catalog.getName()).thenReturn("my-catalog");

    PolarisEvent input =
        new PolarisEvent(
            PolarisEventType.AFTER_CREATE_NAMESPACE,
            null,
            new EventAttributeMap()
                .put(EventAttributes.NAMESPACE, Namespace.of("ns1"))
                .put(EventAttributes.CATALOG, catalog));

    PolarisEvent sanitized = sanitizer.sanitize(input);

    assertThat(sanitized.attributes().contains(EventAttributes.CATALOG)).isFalse();
    assertThat(sanitized.attributes().contains(EventAttributes.NAMESPACE)).isTrue();
  }

  @Test
  void shouldDeriveCatalogNameFromDeniedCatalogObject() {
    Catalog catalog = mock(Catalog.class);
    when(catalog.getName()).thenReturn("derived");

    PolarisEvent input =
        new PolarisEvent(
            PolarisEventType.AFTER_CREATE_CATALOG,
            null,
            new EventAttributeMap().put(EventAttributes.CATALOG, catalog));

    PolarisEvent sanitized = sanitizer.sanitize(input);

    assertThat(sanitized.attributes().contains(EventAttributes.CATALOG)).isFalse();
    assertThat(sanitized.attributes().get(EventAttributes.CATALOG_NAME)).hasValue("derived");
  }

  @Test
  void shouldNotOverrideExistingCatalogName() {
    Catalog catalog = mock(Catalog.class);
    when(catalog.getName()).thenReturn("from-object");

    PolarisEvent input =
        new PolarisEvent(
            PolarisEventType.AFTER_CREATE_CATALOG,
            null,
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, "explicit")
                .put(EventAttributes.CATALOG, catalog));

    PolarisEvent sanitized = sanitizer.sanitize(input);

    assertThat(sanitized.attributes().get(EventAttributes.CATALOG_NAME)).hasValue("explicit");
  }

  @Test
  void shouldReturnNewEventInstanceWithoutMutatingInput() {
    EventAttributeMap originalAttributes =
        new EventAttributeMap().put(EventAttributes.NAMESPACE, Namespace.of("ns1"));
    PolarisEvent input =
        new PolarisEvent(PolarisEventType.AFTER_CREATE_NAMESPACE, null, originalAttributes);

    PolarisEvent sanitized = sanitizer.sanitize(input);

    assertThat(sanitized).isNotSameAs(input);
    assertThat(input.attributes().contains(EventAttributes.NAMESPACE)).isTrue();
  }
}
