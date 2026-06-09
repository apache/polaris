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
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.iceberg.catalog.Namespace;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(GlobalEventSanitizationTest.Profile.class)
class GlobalEventSanitizationTest {

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.of("polaris.event-listener.types", "sanitization-test-listener");
    }
  }

  @Singleton
  @Identifier("sanitization-test-listener")
  public static class SanitizationTestListener implements PolarisEventListener {
    final List<PolarisEvent> events = new CopyOnWriteArrayList<>();

    @Override
    public void onEvent(PolarisEvent event) {
      events.add(event);
    }
  }

  @Inject PolarisEventDispatcher eventDispatcher;

  @Inject
  @Identifier("sanitization-test-listener")
  PolarisEventListener listener;

  @Test
  void shouldStripNonAllowlistedAttributesBeforeDelivery() {
    var testListener = (SanitizationTestListener) listener;

    Catalog sensitiveCatalog = mock(Catalog.class);
    when(sensitiveCatalog.getName()).thenReturn("my-catalog");

    eventDispatcher.dispatch(
        new PolarisEvent(
            PolarisEventType.AFTER_CREATE_NAMESPACE,
            null,
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, "my-catalog")
                .put(EventAttributes.NAMESPACE, Namespace.of("ns1"))
                .put(EventAttributes.CATALOG, sensitiveCatalog)
                .put(EventAttributes.HTTP_METHOD, "POST")));

    await().until(() -> testListener.events.size() == 1);

    PolarisEvent delivered = testListener.events.getFirst();
    assertThat(delivered.attributes().contains(EventAttributes.CATALOG_NAME)).isTrue();
    assertThat(delivered.attributes().contains(EventAttributes.NAMESPACE)).isTrue();
    assertThat(delivered.attributes().contains(EventAttributes.HTTP_METHOD)).isTrue();
    assertThat(delivered.attributes().contains(EventAttributes.CATALOG)).isFalse();
  }

  @Test
  void shouldDeriveCatalogNameFromCatalogObjectDuringFiltering() {
    var testListener = (SanitizationTestListener) listener;
    int beforeCount = testListener.events.size();

    Catalog catalog = mock(Catalog.class);
    when(catalog.getName()).thenReturn("derived-catalog");

    eventDispatcher.dispatch(
        new PolarisEvent(
            PolarisEventType.AFTER_CREATE_CATALOG,
            null,
            new EventAttributeMap().put(EventAttributes.CATALOG, catalog)));

    await().until(() -> testListener.events.size() > beforeCount);

    PolarisEvent delivered =
        testListener.events.stream()
            .filter(e -> e.type() == PolarisEventType.AFTER_CREATE_CATALOG)
            .findFirst()
            .orElseThrow();

    assertThat(delivered.attributes().contains(EventAttributes.CATALOG)).isFalse();
    assertThat(delivered.attributes().get(EventAttributes.CATALOG_NAME))
        .hasValue("derived-catalog");
  }
}
