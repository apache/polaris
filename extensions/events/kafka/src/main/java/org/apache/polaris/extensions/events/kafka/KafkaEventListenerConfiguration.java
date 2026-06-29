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
package org.apache.polaris.extensions.events.kafka;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;

/** Configuration interface for the Kafka Event Listener. */
@StaticInitSafe
@ConfigMapping(prefix = "polaris.event-listener.kafka")
@ApplicationScoped
public interface KafkaEventListenerConfiguration {

  /**
   * The Kafka topic to send Polaris events to.
   *
   * <p>Configuration property: {@code polaris.event-listener.kafka.topic}
   */
  @WithName("topic")
  @WithDefault("polaris-catalog-events")
  String topic();

  /**
   * Returns the synchronous mode setting for sending events to Kafka.
   *
   * <p>When set to "true", log events are sent to Kafka synchronously, which may impact application
   * performance but ensures immediate delivery. When set to "false" (default), log events are sent
   * asynchronously for better performance.
   *
   * <p>Configuration property: {@code polaris.event-listener.kafka.synchronous-mode}
   *
   * @return a boolean value indicating the synchronous mode setting
   */
  @WithName("synchronous-mode")
  @WithDefault("false")
  boolean synchronousMode();

  /**
   * Kafka properties to pass to the Kafka producer, for example bootstap.servers. This can be used
   * to configure authentication (e.g., SASL, SSL) or other producer properties.
   *
   * <p>Configuration property: {@code polaris.event-listener.kafka.properties}
   */
  Map<String, String> properties();
}
