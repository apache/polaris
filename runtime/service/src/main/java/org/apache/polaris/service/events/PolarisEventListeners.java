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

import static org.apache.polaris.service.events.PolarisServiceBusEventDispatcher.POLARIS_EVENT_CHANNEL;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.common.annotation.Identifier;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.concurrent.Executor;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class PolarisEventListeners {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisEventListeners.class);

  @Inject EventBus eventBus;
  @Inject @Any Instance<PolarisEventListener> eventListeners;
  @Inject PolarisEventListenerConfiguration configuration;
  @Inject EventAttributeFilter attributeFilter;

  @Inject
  @Identifier("event-listener-executor")
  Executor executor;

  private final EnumSet<PolarisEventType> enabledEventTypes = EnumSet.allOf(PolarisEventType.class);

  public void onStartup(@Observes StartupEvent event) {
    var listenerTypeSet = configuration.types().orElseGet(HashSet::new);
    for (String enabledEventListener : listenerTypeSet) {
      var listenerConfiguration = configuration.listenerConfig().get(enabledEventListener);
      var supportedTypes =
          listenerConfiguration == null
                  || (listenerConfiguration.enabledEventCategories().isEmpty()
                      && listenerConfiguration.enabledEventTypes().isEmpty())
              ? EnumSet.allOf(PolarisEventType.class)
              : EnumSet.noneOf(PolarisEventType.class);
      if (listenerConfiguration != null) {
        if (listenerConfiguration.enabledEventCategories().isPresent()) {
          for (var enabledEventCategory : listenerConfiguration.enabledEventCategories().get()) {
            supportedTypes.addAll(PolarisEventType.typesOfCategory(enabledEventCategory));
          }
        }
        if (listenerConfiguration.enabledEventTypes().isPresent()) {
          supportedTypes.addAll(listenerConfiguration.enabledEventTypes().get());
        }
      }
      var listener = eventListeners.select(Identifier.Literal.of(enabledEventListener)).get();
      Handler<Message<PolarisEvent>> handler =
          message -> deliverEvent(message.body(), enabledEventListener, listener);
      for (var polarisEventType : supportedTypes) {
        enabledEventTypes.add(polarisEventType);
        eventBus.localConsumer(POLARIS_EVENT_CHANNEL + "." + polarisEventType, handler);
      }
    }
  }

  private void deliverEvent(
      PolarisEvent event, String listenerName, PolarisEventListener listener) {
    LOGGER.debug("Delivering {} event to listener '{}' ({})", event.type(), listenerName, listener);
    // TODO: Add an opt-in mechanism (e.g., a marker interface or annotation) for specialized
    // listeners that need access to the raw, unsanitized event. When implemented, skip
    // sanitization for listeners that opt in to raw access.
    // see https://lists.apache.org/thread/w3mszmog7llyn5spw7rv9tq7r0qp0p6w
    PolarisEvent sanitizedEvent = sanitize(event);
    try {
      executor.execute(
          () -> {
            LOGGER.debug(
                "Delivering {} event to listener '{}' ({})", event.type(), listenerName, listener);
            try {
              listener.onEvent(sanitizedEvent);
            } catch (Exception e) {
              LOGGER.error(
                  "Error while delivering {} event to listener '{}' ({})",
                  event.type(),
                  listenerName,
                  listener,
                  e);
            }
            LOGGER.debug(
                "Delivered {} event to listener '{}' ({})", event.type(), listenerName, listener);
          });
    } catch (Exception e) {
      LOGGER.error(
          "Error while delivering {} event to listener '{}' ({})",
          event.type(),
          listenerName,
          listener,
          e);
    }
  }

  private PolarisEvent sanitize(PolarisEvent event) {
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
          .map(org.apache.polaris.core.admin.model.Catalog::getName)
          .filter(name -> !name.isBlank())
          .ifPresent(name -> filtered.put(EventAttributes.CATALOG_NAME, name));
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> void putUnchecked(EventAttributeMap map, AttributeKey<T> key, Object value) {
    map.put(key, (T) value);
  }

  public boolean hasListeners(PolarisEventType polarisEventType) {
    return enabledEventTypes.contains(polarisEventType);
  }
}
