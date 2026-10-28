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

import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Event Listener that pushes Polaris events to Kafka. */
@ApplicationScoped
@Identifier("kafka")
public class KafkaEventListener implements PolarisEventListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEventListener.class);

  private KafkaProducer<UUID, String> producer;
  private final String topic;
  private final boolean synchronousMode;
  private final Map<String, String> kafkaProperties;
  private final EventToJsonMapper eventToJsonMapper;

  @Inject
  public KafkaEventListener(KafkaEventListenerConfiguration configuration) {
    this.synchronousMode = configuration.synchronousMode();
    this.topic = configuration.topic();
    this.kafkaProperties = new HashMap<>(configuration.properties());
    this.eventToJsonMapper = new EventToJsonMapper();
  }

  @PostConstruct
  void start() {
    LOGGER.debug("Starting KafkaEventListener.");
    Properties props = new Properties();
    props.putAll(this.kafkaProperties);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, UUIDSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    this.producer = new KafkaProducer<>(props);
  }

  @PreDestroy
  void shutdown() {
    if (producer != null) {
      producer.close(Duration.ofSeconds(10));
      producer = null;
    }
  }

  @Override
  public void onEvent(PolarisEvent event) {
    String eventAsJson = eventToJsonMapper.eventToJson(event);
    ProducerRecord<UUID, String> record =
        new ProducerRecord<>(topic, event.metadata().eventId(), eventAsJson);
    try {
      Future<RecordMetadata> future =
          producer.send(
              record,
              synchronousMode
                  ? null
                  : (metadata, exception) -> {
                    if (exception != null) {
                      LOGGER.error("Failed to send event to kafka", exception);
                    } else {
                      LOGGER.debug("Sent event {} to kafka topic {}", event.type(), topic);
                    }
                  });
      if (synchronousMode) {
        future.get();
        LOGGER.debug("Sent event {} to kafka topic {}", event.type(), topic);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error("Interrupted while sending event", e);
    } catch (Exception exception) {
      LOGGER.error("Failed to send event.", exception);
    }
  }
}
