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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.polaris.service.events.PolarisEventListenerConfiguration.ListenerConfiguration;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.events.listeners.RawEventAccess;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class PolarisEventListeners {

  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisEventListeners.class);

  private record NamedEventFilter(String name, EventFilter filter) {}

  @Inject EventBus eventBus;
  @Inject @Any Instance<PolarisEventListener> eventListeners;
  @Inject PolarisEventListenerConfiguration configuration;
  @Inject EventSanitizer eventSanitizer;
  @Inject @Any Instance<EventFilterFactory> eventFilterFactories;
  @Inject PolarisEventFilterConfiguration filterConfiguration;

  @Inject
  @Identifier("event-listener-executor")
  Executor sharedExecutor;

  private final EnumSet<PolarisEventType> eventTypesWithListeners =
      EnumSet.noneOf(PolarisEventType.class);

  /**
   * Registers configured Polaris event listeners with the local Vert.x event bus at startup.
   *
   * <p>Each listener may enable event categories, individual event types, or both. When no
   * per-listener filters are configured, the listener receives every Polaris event type.
   */
  public void init(@Observes StartupEvent event) {
    var listenerTypeSet = configuration.types().orElseGet(HashSet::new);
    if (listenerTypeSet.isEmpty()) {
      LOGGER.debug("No event listeners present, skipping event listener setup");
      return;
    }
    for (String listenerName : listenerTypeSet) {
      var listenerConfiguration = configuration.listenerConfig().get(listenerName);
      var supportedTypes = resolveSupportedTypes(listenerConfiguration);
      var listener = eventListeners.select(Identifier.Literal.of(listenerName)).get();
      var listenerFilters = listenerFilters(listenerName, listenerConfiguration);
      var listenerExecutor =
          new ListenerExecutor(sharedExecutor, configuration.listenerBacklog().queueSize());
      // Reuse the same handler for every selected event type for this listener.
      Handler<Message<PolarisEvent>> handler =
          message ->
              scheduleEventDelivery(
                  message.body(), listenerName, listener, listenerFilters, listenerExecutor);
      for (var polarisEventType : supportedTypes) {
        eventTypesWithListeners.add(polarisEventType);
        String address = PolarisServiceBusEventDispatcher.eventAddress(polarisEventType);
        eventBus.localConsumer(address, handler);
      }
    }
  }

  private static EnumSet<PolarisEventType> resolveSupportedTypes(
      @Nullable ListenerConfiguration listenerConfiguration) {
    // Missing or empty per-listener filters mean that the listener receives all event types.
    if (listenerConfiguration == null
        || (listenerConfiguration.enabledEventCategories().orElse(Set.of()).isEmpty()
            && listenerConfiguration.enabledEventTypes().orElse(Set.of()).isEmpty())) {
      return EnumSet.allOf(PolarisEventType.class);
    }

    // Category and type filters are additive: a listener receives the union of both.
    var supportedTypes = EnumSet.noneOf(PolarisEventType.class);
    if (listenerConfiguration.enabledEventCategories().isPresent()) {
      for (var enabledEventCategory : listenerConfiguration.enabledEventCategories().get()) {
        supportedTypes.addAll(PolarisEventType.typesOfCategory(enabledEventCategory));
      }
    }
    if (listenerConfiguration.enabledEventTypes().isPresent()) {
      supportedTypes.addAll(listenerConfiguration.enabledEventTypes().get());
    }
    return supportedTypes;
  }

  // Executed on a Vert.x event loop thread
  private void scheduleEventDelivery(
      PolarisEvent event,
      String listenerName,
      PolarisEventListener listener,
      List<NamedEventFilter> listenerFilters,
      Executor listenerExecutor) {
    LOGGER.debug(
        "Scheduling {} event delivery to listener '{}' ({})", event.type(), listenerName, listener);
    try {
      listenerExecutor.execute(() -> deliverEvent(event, listenerName, listener, listenerFilters));
      LOGGER.debug(
          "Successfully scheduled {} event delivery to listener '{}' ({})",
          event.type(),
          listenerName,
          listener);
    } catch (Exception e) {
      LOGGER.error(
          "Error while scheduling {} event delivery to listener '{}' ({})",
          event.type(),
          listenerName,
          listener,
          e);
    }
  }

  // Executed on the blocking shared executor
  private void deliverEvent(
      PolarisEvent event,
      String listenerName,
      PolarisEventListener listener,
      List<NamedEventFilter> listenerFilters) {
    LOGGER.debug("Delivering {} event to listener '{}' ({})", event.type(), listenerName, listener);

    // 1. Filters
    for (var filter : listenerFilters) {
      try {
        if (!filter.filter().test(event)) {
          LOGGER.debug(
              "Event {} filtered out by filter '{}' ({}) for listener '{}' ({})",
              event.type(),
              filter.name(),
              filter.filter(),
              listenerName,
              listener);
          return;
        }
      } catch (Exception e) {
        LOGGER.error(
            "Error while filtering {} event with filter '{}' ({}) for listener '{}' ({}); event will be dropped",
            event.type(),
            filter.name(),
            filter.filter(),
            listenerName,
            listener,
            e);
        return;
      }
    }

    // 2. Sanitizers
    // TODO make sanitizers selectable like filters
    PolarisEvent toDeliver = event;
    boolean sanitize = !(listener instanceof RawEventAccess);
    if (sanitize) {
      try {
        toDeliver = eventSanitizer.sanitize(event);
      } catch (Exception e) {
        LOGGER.error(
            "Error while sanitizing {} event for listener '{}' ({}); event will be dropped",
            event.type(),
            listenerName,
            listener,
            e);
        return;
      }
    }

    // 3. Listener
    try {
      listener.onEvent(toDeliver);
      LOGGER.debug(
          "Successfully delivered {} event to listener '{}' ({})",
          toDeliver.type(),
          listenerName,
          listener);
    } catch (Exception e) {
      LOGGER.error(
          "Error while delivering {} event to listener '{}' ({})",
          toDeliver.type(),
          listenerName,
          listener,
          e);
    }
  }

  private List<NamedEventFilter> listenerFilters(
      String listenerName, @Nullable ListenerConfiguration listenerConfiguration) {
    if (listenerConfiguration == null) {
      return List.of();
    }
    List<String> filterNames = listenerConfiguration.filters().orElse(List.of());
    if (filterNames.isEmpty()) {
      return List.of();
    }
    var filters = new ArrayList<NamedEventFilter>(filterNames.size());
    for (var filterName : filterNames) {
      var filterConfig = filterConfiguration.filters().get(filterName);
      if (filterConfig == null) {
        throw new IllegalArgumentException(
            "Unknown event filter '"
                + filterName
                + "' referenced by listener '"
                + listenerName
                + "'");
      }
      filters.add(new NamedEventFilter(filterName, createFilter(filterName, filterConfig)));
    }
    return filters;
  }

  private EventFilter createFilter(
      String filterName, PolarisEventFilterConfiguration.FilterConfiguration filterConfig) {
    var eventFilterFactory =
        eventFilterFactories.select(Identifier.Literal.of(filterConfig.type()));
    if (eventFilterFactory.isUnsatisfied()) {
      throw new IllegalArgumentException(
          "Unsupported Polaris event filter type '"
              + filterConfig.type()
              + "' for filter '"
              + filterName
              + "'");
    }
    if (eventFilterFactory.isAmbiguous()) {
      throw new IllegalArgumentException(
          "Ambiguous Polaris event filter type '"
              + filterConfig.type()
              + "' for filter '"
              + filterName
              + "'");
    }
    return eventFilterFactory.get().create(filterName, filterConfig);
  }

  public boolean hasListeners(PolarisEventType eventType) {
    return eventTypesWithListeners.contains(eventType);
  }

  private static class ListenerExecutor implements Executor {

    private final Executor delegate;
    private final Queue<Runnable> queue;
    private final AtomicBoolean draining = new AtomicBoolean(false);

    ListenerExecutor(Executor delegate, int capacity) {
      this.delegate = delegate;
      this.queue =
          capacity == -1 ? new ConcurrentLinkedQueue<>() : new ArrayBlockingQueue<>(capacity);
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public void execute(@NonNull Runnable task) {
      if (!queue.offer(task)) {
        throw new RejectedExecutionException("Event listener queue is full");
      }
      maybeScheduleDrain().exceptionally(this::onError);
    }

    private CompletableFuture<Void> maybeScheduleDrain() {
      if (!queue.isEmpty() && draining.compareAndSet(false, true)) {
        try {
          return CompletableFuture.runAsync(this::drain, delegate)
              // if tasks arrived in the interim, schedule another drain for fairness
              .thenCompose(v -> maybeScheduleDrain())
              .exceptionally(this::onError);
        } catch (Throwable t) {
          return CompletableFuture.failedFuture(t);
        }
      }
      return CompletableFuture.completedFuture(null);
    }

    private void drain() {
      try {
        Runnable task;
        while ((task = queue.poll()) != null) {
          task.run();
        }
      } finally {
        draining.set(false);
      }
    }

    private Void onError(Throwable error) {
      draining.set(false);
      LOGGER.warn("Failed to drain listener backlog; task delivery may be delayed", error);
      return null;
    }
  }
}
