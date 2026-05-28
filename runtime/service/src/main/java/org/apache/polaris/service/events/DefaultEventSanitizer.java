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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.admin.model.Catalog;

/**
 * Default implementation of {@link EventSanitizer}. Composes attribute filtering (delegated to
 * {@link EventAttributeFilter}) and derivation of safe attributes (e.g., {@code CATALOG_NAME}
 * extracted from a denied {@code CATALOG} object). Operators who want to keep the standard
 * derivation logic but customize denial can replace only the {@link EventAttributeFilter} bean.
 * Operators who need to change derivation or add raw-event passthrough should replace this bean.
 */
@ApplicationScoped
@DefaultBean
public class DefaultEventSanitizer implements EventSanitizer {

  @Inject EventAttributeFilter attributeFilter;

  DefaultEventSanitizer() {}

  DefaultEventSanitizer(EventAttributeFilter attributeFilter) {
    this.attributeFilter = attributeFilter;
  }

  @Override
  public PolarisEvent sanitize(PolarisEvent event) {
    EventAttributeMap filtered = new EventAttributeMap();
    event
        .attributes()
        .forEach(
            (key, value) -> {
              if (attributeFilter.isAllowed(key)) {
                putUnchecked(filtered, key, value);
              }
            });
    extractDerivedAttributes(event, filtered);
    return new PolarisEvent(event.type(), event.metadata(), filtered);
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

  @SuppressWarnings("unchecked")
  private static <T> void putUnchecked(EventAttributeMap map, AttributeKey<T> key, Object value) {
    map.put(key, (T) value);
  }
}
