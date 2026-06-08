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
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.events.listeners.RawEventAccess;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class PolarisEventListeners {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisEventListeners.class);

  @Inject EventBus eventBus;
  @Inject @Any Instance<PolarisEventListener> eventListeners;
  @Inject PolarisEventListenerConfiguration configuration;
  @Inject EventSanitizer eventSanitizer;

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
    for (String enabledEventListener : listenerTypeSet) {
      var listenerConfiguration = configuration.listenerConfig().get(enabledEventListener);
      var supportedTypes = resolveSupportedTypes(listenerConfiguration);
      var listener = eventListeners.select(Identifier.Literal.of(enabledEventListener)).get();
      var listenerExecutor =
          new ListenerExecutor(sharedExecutor, configuration.executor().queueSize());
      // Reuse the same handler for every selected event type for this listener.
      Handler<Message<PolarisEvent>> handler =
          message ->
              scheduleEventDelivery(
                  message.body(), enabledEventListener, listener, listenerExecutor);
      for (var polarisEventType : supportedTypes) {
        eventTypesWithListeners.add(polarisEventType);
        eventBus.localConsumer(POLARIS_EVENT_CHANNEL + "." + polarisEventType, handler);
      }
    }
  }

  private static EnumSet<PolarisEventType> resolveSupportedTypes(
      PolarisEventListenerConfiguration.ListenerConfiguration listenerConfiguration) {
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

  private void scheduleEventDelivery(
      PolarisEvent event,
      String listenerName,
      PolarisEventListener listener,
      ListenerExecutor listenerExecutor) {
    LOGGER.debug(
        "Scheduling {} event delivery to listener '{}' ({})", event.type(), listenerName, listener);
    try {
      listenerExecutor.execute(() -> deliverEvent(event, listenerName, listener));
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

  private void deliverEvent(
      PolarisEvent event, String listenerName, PolarisEventListener listener) {
    LOGGER.debug("Delivering {} event to listener '{}' ({})", event.type(), listenerName, listener);
    PolarisEvent toDeliver =
        (listener instanceof RawEventAccess) ? event : eventSanitizer.sanitize(event);
    try {
      listener.onEvent(toDeliver);
      LOGGER.debug(
          "Successfully delivered {} event to listener '{}' ({})",
          event.type(),
          listenerName,
          listener);
    } catch (Exception e) {
      LOGGER.error(
          "Error while delivering {} event to listener '{}' ({})",
          event.type(),
          listenerName,
          listener,
          e);
    }
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
    public void execute(@NonNull Runnable task) {
      if (!queue.offer(task)) {
        throw new RejectedExecutionException("Event listener queue is full");
      }
      if (draining.compareAndSet(false, true)) {
        delegate.execute(this::drain);
      }
    }

    private void drain() {
      try {
        Runnable task;
        while ((task = queue.poll()) != null) {
          task.run();
        }
      } finally {
        draining.set(false);
        // A producer may have enqueued a task between the last poll() and the set(false) above,
        // and seen draining=true so it did not schedule a drain. Catch that here.
        if (!queue.isEmpty() && draining.compareAndSet(false, true)) {
          delegate.execute(this::drain); // re-schedule for fairness
        }
      }
    }
  }
}
