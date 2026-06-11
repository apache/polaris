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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(PolarisEventFiltersTest.Profile.class)
public class PolarisEventFiltersTest {
  private static final String FILTERED_LISTENER = "filtered-event-listener";
  private static final String UNFILTERED_LISTENER = "unfiltered-event-listener";

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.event-listener.types",
          FILTERED_LISTENER + "," + UNFILTERED_LISTENER,
          "polaris.event-listener." + FILTERED_LISTENER + ".filters",
          "realm,catalog",
          "polaris.event-filter.realm.type",
          "jakarta-el",
          "polaris.event-filter.realm.include",
          "metadata.realmId() == 'realm1'",
          "polaris.event-filter.catalog.type",
          "jakarta-el",
          "polaris.event-filter.catalog.include",
          "attributes.catalog_name == 'catalog1'");
    }
  }

  @Singleton
  @Identifier(FILTERED_LISTENER)
  public static class FilteredEventListener extends RecordingEventListener {}

  @Singleton
  @Identifier(UNFILTERED_LISTENER)
  public static class UnfilteredEventListener extends RecordingEventListener {}

  abstract static class RecordingEventListener implements PolarisEventListener {
    final List<PolarisEvent> events = new CopyOnWriteArrayList<>();

    @Override
    public void onEvent(PolarisEvent event) {
      events.add(event);
    }
  }

  @Inject PolarisEventDispatcher eventDispatcher;

  @Inject
  @Identifier(FILTERED_LISTENER)
  PolarisEventListener filteredEventListener;

  @Inject
  @Identifier(UNFILTERED_LISTENER)
  PolarisEventListener unfilteredEventListener;

  @Test
  public void testFiltersRestrictMappedListenerOnlyWhenAllFiltersPass() {
    var filtered = (FilteredEventListener) filteredEventListener;
    var unfiltered = (UnfilteredEventListener) unfilteredEventListener;

    eventDispatcher.dispatch(event("realm1", "catalog1"));
    eventDispatcher.dispatch(event("realm2", "catalog1"));
    eventDispatcher.dispatch(event("realm1", "catalog2"));

    await().untilAsserted(() -> assertThat(unfiltered.events).hasSize(3));
    await().untilAsserted(() -> assertThat(filtered.events).hasSize(1));

    assertThat(filtered.events.getFirst().metadata().realmId()).isEqualTo("realm1");
    assertThat(filtered.events.getFirst().attributes().getRequired(EventAttributes.CATALOG_NAME))
        .isEqualTo("catalog1");
  }

  private static PolarisEvent event(String realmId, String catalogName) {
    return new PolarisEvent(
        PolarisEventType.BEFORE_CREATE_TABLE,
        ImmutablePolarisEventMetadata.builder().realmId(realmId).build(),
        new EventAttributeMap().put(EventAttributes.CATALOG_NAME, catalogName));
  }
}
