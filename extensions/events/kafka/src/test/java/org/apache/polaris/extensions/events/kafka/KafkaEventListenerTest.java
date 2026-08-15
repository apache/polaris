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

import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;
import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.collection.ImmutableAttributeMap;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.ImmutablePolarisEventMetadata;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers
@ExtendWith(MockitoExtension.class)
class KafkaEventListenerTest {
  @Container
  private static final KafkaContainer KAFKA_CONTAINER =
      new KafkaContainer(
          containerSpecHelper("kafka", KafkaEventListenerTest.class)
              .dockerImageName(null)
              .asCompatibleSubstituteFor("kafka-native"));

  private static final String TOPIC = "polaris-events";
  private static final String REALM = "test-realm";
  private static final String TEST_USER = "test-user";
  private static final String KAFKA_GROUP_ID = "polaris-group-id";
  private static final String TEST_CATALOG = "test_catalog";
  private static final PolarisPrincipal PRINCIPAL =
      PolarisPrincipal.of(TEST_USER, Map.of(), Set.of("role1", "role2"));
  private static final TableIdentifier TEST_TABLE_IDENTIFIER =
      TableIdentifier.of("test_namespace", "test_table");
  private static final JsonMapper OBJECT_MAPPER = JsonMapper.shared();

  @Mock private KafkaEventListenerConfiguration config;

  private KafkaConsumer<UUID, String> getKafkaConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, UUIDDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
    // Get events from the beginning.
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP_ID);
    return new KafkaConsumer<>(props);
  }

  private PolarisEvent getEvent() {
    return new PolarisEvent(
        PolarisEventType.AFTER_REFRESH_TABLE,
        ImmutablePolarisEventMetadata.builder().realmId(REALM).user(PRINCIPAL).build(),
        ImmutableAttributeMap.builder()
            .put(EventAttributes.CATALOG_NAME, TEST_CATALOG)
            .put(EventAttributes.TABLE_IDENTIFIER, TEST_TABLE_IDENTIFIER)
            .build());
  }

  @Test
  void testNoBootStrapServers() {
    // Configure the mocks & create listener
    when(config.topic()).thenReturn(TOPIC);
    // No bootstrap servers.
    when(config.properties()).thenReturn(Map.of());
    when(config.synchronousMode()).thenReturn(true);
    KafkaEventListener listener = new KafkaEventListener(config);
    // Exception is thrown as Kafka is not configured properly.
    assertThatThrownBy(
            () -> {
              listener.start();
            })
        .isInstanceOf(org.apache.kafka.common.config.ConfigException.class)
        .hasMessage(
            "Missing required configuration \"bootstrap.servers\" which has no default value.");
  }

  @ParameterizedTest
  @MethodSource("provideConfigForTest")
  void publishEvent(String testname, boolean synchronousMode) throws JacksonException {
    // Configure the mocks & create listener
    when(config.topic()).thenReturn(TOPIC);
    when(config.properties())
        .thenReturn(
            Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers()));
    when(config.synchronousMode()).thenReturn(synchronousMode);
    KafkaEventListener listener = new KafkaEventListener(config);

    listener.start();
    try (KafkaConsumer<UUID, String> kafkaConsumer = getKafkaConsumer()) {
      // Generate PolarisEvent.
      PolarisEvent event = getEvent();
      // Pass event to listener, push to Kafka - will automatically create the kafka topic.
      listener.onEvent(event);
      // Consumer subscribe to topic
      kafkaConsumer.subscribe(List.of(TOPIC));
      // Consumer get event, will time out after 30 seconds.
      ConsumerRecords<UUID, String> records = kafkaConsumer.poll(Duration.ofSeconds(30));
      // Check the event from kafka matches the PolarisEvent.
      assertThat(records).hasSize(1);
      ConsumerRecord<UUID, String> record = records.iterator().next();
      assertThat(record.key()).isEqualTo(event.metadata().eventId());
      Map<String, Object> eventMap =
          OBJECT_MAPPER.readValue(record.value(), new TypeReference<>() {});
      assertThat(eventMap)
          .containsEntry("timestamp", event.metadata().timestamp().toString())
          .containsEntry("event_type", event.type().toString())
          .containsEntry("principal", PRINCIPAL.getName())
          .containsEntry("catalog_name", TEST_CATALOG)
          .containsEntry("realm_id", REALM);
      assertThat(eventMap)
          .extractingByKey("table_identifier", as(LIST))
          .containsExactly("test_namespace", "test_table");
      assertThat(eventMap)
          .extractingByKey("activated_roles", as(LIST))
          .containsExactlyInAnyOrder("role1", "role2");
    } finally {
      listener.shutdown();
    }
  }

  private static Stream<Arguments> provideConfigForTest() {
    return Stream.of(
        Arguments.of("KafkaEventListener synchronous.", true),
        Arguments.of("KafkaEventListener asynchronous.", false));
  }
}
