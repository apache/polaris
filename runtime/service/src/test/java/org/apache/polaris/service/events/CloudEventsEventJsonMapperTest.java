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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.collection.AttributeMap;
import org.apache.polaris.core.collection.MutableAttributeMap;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class CloudEventsEventJsonMapperTest {

  private static final UUID EVENT_ID = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
  private static final Instant TIMESTAMP = Instant.parse("2026-10-04T12:34:56.789Z");

  private final CloudEventsEventJsonMapper mapper = new CloudEventsEventJsonMapper();

  @Test
  void cloudEventsTypeIsLowercaseReverseDns() {
    assertThat(CloudEventsEventJsonMapper.cloudEventsType(PolarisEventType.AFTER_CREATE_TABLE))
        .isEqualTo("org.apache.polaris.after_create_table");
  }

  @Test
  void eventToJsonUsesCloudEventsContextAndKafkaParityData() throws Exception {
    PolarisEventMetadata metadata =
        ImmutablePolarisEventMetadata.builder()
            .eventId(EVENT_ID)
            .timestamp(TIMESTAMP)
            .realmId("default-realm")
            .user(PolarisPrincipal.of("alice", AttributeMap.EMPTY, Set.of("catalog_admin")))
            .requestId("req-123")
            .build();
    MutableAttributeMap attributes =
        MutableAttributeMap.builder()
            .put(EventAttributes.CATALOG_NAME, "sales")
            .put(EventAttributes.NAMESPACE, Namespace.of("analytics"))
            .put(EventAttributes.TABLE_NAME, "orders")
            .put(EventAttributes.TABLE_IDENTIFIER, TableIdentifier.of("analytics", "orders"))
            .build();
    PolarisEvent event =
        new PolarisEvent(PolarisEventType.AFTER_CREATE_TABLE, metadata, attributes);

    JsonNode root = JsonMapper.shared().readTree(mapper.eventToJson(event));

    assertThat(root.get("specversion").asString()).isEqualTo("1.0");
    assertThat(root.get("id").asString()).isEqualTo(EVENT_ID.toString());
    assertThat(root.get("type").asString()).isEqualTo("org.apache.polaris.after_create_table");
    assertThat(root.get("source").asString()).isEqualTo("org.apache.polaris");
    assertThat(root.get("time").asString()).isEqualTo(TIMESTAMP.toString());
    assertThat(root.get("datacontenttype").asString()).isEqualTo("application/json");

    JsonNode data = root.get("data");
    assertThat(data.get("event_type").asString()).isEqualTo("AFTER_CREATE_TABLE");
    assertThat(data.get("event_id").asString()).isEqualTo(EVENT_ID.toString());
    assertThat(data.get("timestamp").asString()).isEqualTo(TIMESTAMP.toString());
    assertThat(data.get("realm_id").asString()).isEqualTo("default-realm");
    assertThat(data.get("principal").asString()).isEqualTo("alice");
    assertThat(data.get("activated_roles").get(0).asString()).isEqualTo("catalog_admin");
    assertThat(data.get("request_id").asString()).isEqualTo("req-123");
    assertThat(data.get("catalog_name").asString()).isEqualTo("sales");
    assertThat(data.get("namespace").get(0).asString()).isEqualTo("analytics");
    assertThat(data.get("table_name").asString()).isEqualTo("orders");
    assertThat(data.get("table_identifier").get(0).asString()).isEqualTo("analytics");
    assertThat(data.get("table_identifier").get(1).asString()).isEqualTo("orders");

    // CE context attrs only at the top — no Polaris snake_case keys as CE extensions
    assertThat(root.has("event_type")).isFalse();
    assertThat(root.has("activated_roles")).isFalse();
    assertThat(root.has("realm_id")).isFalse();
  }

  @Test
  void eventToDataNodeOmitsAbsentOptionalFields() {
    PolarisEventMetadata metadata =
        ImmutablePolarisEventMetadata.builder()
            .eventId(EVENT_ID)
            .timestamp(TIMESTAMP)
            .realmId("default-realm")
            .build();
    PolarisEvent event = new PolarisEvent(PolarisEventType.AFTER_CREATE_TABLE, metadata);

    JsonNode data = mapper.eventToDataNode(event);

    assertThat(data.get("event_type").asString()).isEqualTo("AFTER_CREATE_TABLE");
    assertThat(data.get("realm_id").asString()).isEqualTo("default-realm");
    assertThat(data.has("principal")).isFalse();
    assertThat(data.has("request_id")).isFalse();
    assertThat(data.has("catalog_name")).isFalse();
    assertThat(data.has("table_identifier")).isFalse();
  }
}
