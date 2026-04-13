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

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithParentName;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.service.events.listeners.PolarisEventListener;

@StaticInitSafe
@ConfigMapping(prefix = "polaris.event-listener")
public interface PolarisEventListenerConfiguration {
  /**
   * The type of the event listener to use. Must be a registered {@link PolarisEventListener}
   * identifier.
   *
   * @deprecated since 1.5.0, use 'polaris.event-listener.types' instead, if both are set, then
   *     polaris.event-listener.types is prioritized
   */
  @Deprecated(since = "1.5.0", forRemoval = true)
  Optional<String> type();

  /**
   * Comma separated list of event listeners, each item must be a registered {@link
   * PolarisEventListener} identifier.
   */
  Optional<Set<String>> types();

  /** Configuration of each event listener type. */
  @WithParentName
  Map<String, ListenerConfiguration> listenerConfig();

  interface ListenerConfiguration {
    /**
     * Comma separated list of enabled event types. This event listener will only receive events of
     * the selected types. If both the event types and event category configs are set, the listener
     * will listen to both. If no listener configuration is present, then all event types are
     * enabled. If a listener configuration exists, events are disabled by default (unless
     * explicitly listed in either enabled-event-types or enabled-event-categories).
     */
    Optional<Set<PolarisEventType>> enabledEventTypes();

    /**
     * Comma separated list of enabled event type categories. Each category is a collection of
     * related Polaris event types. This event listener will only receive events of the selected
     * event category, for example, consume only catalog events. If both the event types and event
     * category configs are set, the listener will listen to both. If no listener configuration is
     * present, then all event types are enabled. If a listener configuration exists, events are
     * disabled by default (unless explicitly listed in either enabled-event-types or
     * enabled-event-categories).
     */
    Optional<Set<PolarisEventType.Category>> enabledEventCategories();
  }
}
