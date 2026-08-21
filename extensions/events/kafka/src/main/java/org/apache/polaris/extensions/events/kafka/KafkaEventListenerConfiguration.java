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
import java.util.Map;

/**
 * Configuration for the Kafka Event Listener.
 *
 * <p>Publishes Polaris events to a Kafka topic. The key for the Kafka record is the UUID of the
 * Polaris event. To preserve FIFO behavior in the Kafka stream of the Polaris events a single
 * partition should be used.
 *
 * <p>The value of Kafka record is JSON encoded as a String. It consists of a set of key values. The
 * set of keys included depends on the Polaris event type and are listed below. The type of value is
 * String except for keys source, destination, table_identifier, view_identifier, namespace and
 * activated_roles which are all encoded as an array of Strings.
 *
 * <ul>
 *   <li>event_type
 *   <li>timestamp (ISO-8601 representation)
 *   <li>source (source of table rename if the event is a table rename)
 *   <li>destination (destination of table rename if the event is a table rename)
 *   <li>table_name
 *   <li>namespace
 *   <li>table_identifier
 *   <li>view_identifier
 *   <li>view_name
 *   <li>namespace_name
 *   <li>catalog_name
 *   <li>realm_id
 *   <li>principal
 *   <li>activated_roles
 *   <li>request_id
 * </ul>
 */
@StaticInitSafe
@ConfigMapping(prefix = "polaris.event-listener.kafka")
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
   * The synchronous mode setting for sending events to Kafka.
   *
   * <p>When set to "true", when events are sent to Kafka the event handling thread will wait for
   * the events to be delivered to Kafka, which may impact performance. When set to "false"
   * (default), events are sent asynchronously.
   *
   * <p>Configuration property: {@code polaris.event-listener.kafka.synchronous-mode}
   *
   * @return a boolean value indicating the synchronous mode setting
   */
  @WithName("synchronous-mode")
  @WithDefault("false")
  boolean synchronousMode();

  /**
   * Kafka properties to pass to the Kafka client producer, for example bootstrap.servers is
   * required. This can be used to configure authentication (e.g., SASL, SSL) or other Kafka
   * producer properties.
   *
   * <p>Configuration property: {@code polaris.event-listener.kafka.properties}
   */
  Map<String, String> properties();
}
