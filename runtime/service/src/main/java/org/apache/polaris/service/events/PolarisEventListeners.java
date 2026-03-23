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
import java.util.HashSet;
import java.util.Set;
import org.apache.polaris.service.events.listeners.PolarisEventListener;

@ApplicationScoped
public class PolarisEventListeners {
  @Inject EventBus eventBus;
  @Inject @Any Instance<PolarisEventListener> eventListeners;
  @Inject PolarisEventListenerConfiguration configuration;

  public void onStartup(@Observes StartupEvent event) {
    Set<String> listenerTypeSet = configuration.types().orElseGet(HashSet::new);
    for (String enabledEventListener : listenerTypeSet) {
      PolarisEventListener listener =
          eventListeners.select(Identifier.Literal.of(enabledEventListener)).get();
      Handler<Message<PolarisEvent>> handler = e -> listener.onEvent(e.body());
      eventBus.localConsumer(POLARIS_EVENT_CHANNEL, handler);
    }
  }
}
