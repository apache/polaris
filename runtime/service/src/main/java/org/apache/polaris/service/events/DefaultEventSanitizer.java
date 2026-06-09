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

import io.quarkus.arc.DefaultBean;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.polaris.core.admin.model.Catalog;

/**
 * Default implementation of {@link EventSanitizer}. Drops attributes whose keys are on the denylist
 * and derives safe attributes (e.g., {@code CATALOG_NAME} extracted from a denied {@code CATALOG}
 * object) from the originals. The built-in denylist covers attributes that carry credentials or
 * internal secrets and may be extended via {@code polaris.event-listener.denylisted-attributes}.
 *
 * <p>Operators who need a different denial or derivation policy should replace this bean.
 */
@ApplicationScoped
@DefaultBean
public class DefaultEventSanitizer implements EventSanitizer {

  // These carry full domain objects containing credentials or internal secrets:
  // PRINCIPAL - may contain client IDs, internal identity metadata
  // UPDATE_PRINCIPAL_REQUEST - credential rotation data, secret changes
  // CATALOG - includes StorageConfigInfo with cloud credentials (AWS keys, Azure tokens, etc.)
  // NOTIFICATION_REQUEST - contains metadata file locations and potentially signed URLs
  static final Set<AttributeKey<?>> DEFAULT_DENYLIST =
      Set.of(
          EventAttributes.PRINCIPAL,
          EventAttributes.UPDATE_PRINCIPAL_REQUEST,
          EventAttributes.CATALOG,
          EventAttributes.NOTIFICATION_REQUEST);

  @Inject PolarisEventListenerConfiguration configuration;

  private Set<AttributeKey<?>> denylist;

  DefaultEventSanitizer() {
    this(DEFAULT_DENYLIST);
  }

  DefaultEventSanitizer(Set<AttributeKey<?>> denylist) {
    this.denylist = Set.copyOf(denylist);
  }

  @PostConstruct
  void initializeFromConfiguration() {
    if (configuration == null) {
      return;
    }
    Set<String> configuredNames =
        configuration.denylistedAttributes().orElse(Collections.emptySet());
    if (!configuredNames.isEmpty()) {
      this.denylist = resolveAdditionalDenylist(configuredNames);
    }
  }

  @Override
  public PolarisEvent sanitize(PolarisEvent event) {
    EventAttributeMap filtered = new EventAttributeMap();
    event
        .attributes()
        .forEach(
            (key, value) -> {
              if (!denylist.contains(key)) {
                putUnchecked(filtered, key, value);
              }
            });
    extractDerivedAttributes(event, filtered);
    return new PolarisEvent(event.type(), event.metadata(), filtered);
  }

  public static Set<AttributeKey<?>> resolveAdditionalDenylist(Set<String> configuredNames) {
    Set<AttributeKey<?>> resolved = new LinkedHashSet<>(DEFAULT_DENYLIST);
    List<String> unknown = new ArrayList<>();

    for (String name : configuredNames) {
      EventAttributes.findByName(name).ifPresentOrElse(resolved::add, () -> unknown.add(name));
    }

    if (!unknown.isEmpty()) {
      throw new IllegalArgumentException(
          "Unknown event attributes in denylist configuration: " + unknown);
    }

    return Set.copyOf(resolved);
  }

  private static void extractDerivedAttributes(PolarisEvent event, EventAttributeMap filtered) {
    if (!filtered.contains(EventAttributes.CATALOG_NAME)) {
      event
          .attributes()
          .get(EventAttributes.CATALOG)
          .map(Catalog::getName)
          .filter(name -> !name.isBlank())
          .ifPresent(name -> filtered.put(EventAttributes.CATALOG_NAME, name));
    }
  }

  private static <T> void putUnchecked(EventAttributeMap map, AttributeKey<T> key, Object value) {
    map.put(key, key.cast(value));
  }
}
