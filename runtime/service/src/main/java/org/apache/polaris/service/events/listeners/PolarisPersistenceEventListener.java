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

package org.apache.polaris.service.events.listeners;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventType;

public abstract class PolarisPersistenceEventListener implements PolarisEventListener {

  private final Map<PolarisEventType, PersistenceEventHandler> handlers;

  protected PolarisPersistenceEventListener() {
    this(defaultHandlers());
  }

  PolarisPersistenceEventListener(Map<PolarisEventType, PersistenceEventHandler> handlers) {
    EnumMap<PolarisEventType, PersistenceEventHandler> copy = new EnumMap<>(PolarisEventType.class);
    copy.putAll(handlers);
    this.handlers = copy;
  }

  @Override
  public void onEvent(PolarisEvent event) {
    PersistenceEventHandler handler = handlers.get(event.type());
    if (handler != null) {
      handler.handle(event, event.metadata().realmId(), this);
    }
  }

  static Map<PolarisEventType, PersistenceEventHandler> defaultHandlers() {
    EnumMap<PolarisEventType, PersistenceEventHandler> handlers =
        new EnumMap<>(PolarisEventType.class);
    handlers.put(PolarisEventType.AFTER_CREATE_CATALOG, new AfterCreateCatalogEventHandler());
    handlers.putAll(TableEventHandler.createHandlers());
    validateTableHandlerCoverage(handlers.keySet());
    return Collections.unmodifiableMap(handlers);
  }

  private static void validateTableHandlerCoverage(Set<PolarisEventType> configuredEventTypes) {
    EnumSet<PolarisEventType> expectedTableEvents =
        Arrays.stream(PolarisEventType.values())
            .filter(PolarisPersistenceEventListener::isTableEvent)
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(PolarisEventType.class)));

    EnumSet<PolarisEventType> missingTableHandlers = EnumSet.copyOf(expectedTableEvents);
    missingTableHandlers.removeAll(configuredEventTypes);
    if (!missingTableHandlers.isEmpty()) {
      throw new IllegalStateException(
          "Missing persistence handlers for table events: " + missingTableHandlers);
    }
  }

  private static boolean isTableEvent(PolarisEventType type) {
    return type.code() >= PolarisEventType.BEFORE_CREATE_TABLE.code()
        && type.code() <= PolarisEventType.AFTER_REFRESH_TABLE.code();
  }

  final void persistEvent(
      PolarisEvent sourceEvent,
      String realmId,
      org.apache.polaris.core.entity.PolarisEvent.ResourceType resourceType,
      String catalogName,
      String resourceIdentifier,
      Map<String, String> additionalProperties) {
    org.apache.polaris.core.entity.PolarisEvent polarisEvent =
        new org.apache.polaris.core.entity.PolarisEvent(
            catalogName,
            sourceEvent.metadata().eventId().toString(),
            sourceEvent.metadata().requestId().orElse(null),
            sourceEvent.type().name(),
            sourceEvent.metadata().timestamp().toEpochMilli(),
            sourceEvent.metadata().user().map(PolarisPrincipal::getName).orElse(null),
            resourceType,
            resourceIdentifier);

    Map<String, String> finalProperties =
        new HashMap<>(sourceEvent.metadata().openTelemetryContext());
    if (additionalProperties != null && !additionalProperties.isEmpty()) {
      finalProperties.putAll(additionalProperties);
    }
    if (!finalProperties.isEmpty()) {
      polarisEvent.setAdditionalProperties(finalProperties);
    }
    processEvent(realmId, polarisEvent);
  }

  protected abstract void processEvent(
      String realmId, org.apache.polaris.core.entity.PolarisEvent event);
}
