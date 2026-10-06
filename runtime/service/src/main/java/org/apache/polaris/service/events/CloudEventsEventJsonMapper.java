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

import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Shared CloudEvents 1.0 JSON mapper for Polaris transport listeners (Kafka, webhook, CloudWatch).
 *
 * <p>Context attributes stay at the top level ({@code specversion}, {@code id}, {@code type},
 * {@code source}, {@code time}, {@code datacontenttype}). Polaris fields live in {@code data} and
 * match the historical Kafka flat payload (snake_case {@link EventAttributes} keys) for v1 parity.
 *
 * <p>{@code type} is a reverse-DNS name with a lowercased {@link PolarisEventType} suffix, e.g.
 * {@code org.apache.polaris.after_create_table}.
 */
public final class CloudEventsEventJsonMapper {

  public static final String SPEC_VERSION = "1.0";
  public static final String DATA_CONTENT_TYPE = "application/json";
  public static final String SOURCE = "org.apache.polaris";
  public static final String TYPE_PREFIX = "org.apache.polaris.";

  private static final ObjectMapper MAPPER = JsonMapper.shared();

  /**
   * Encode a Polaris event as a CloudEvents 1.0 JSON object string.
   *
   * @param event event to encode
   * @return JSON string
   */
  public String eventToJson(PolarisEvent event) {
    return MAPPER.writeValueAsString(eventToObjectNode(event));
  }

  /**
   * Encode a Polaris event as a CloudEvents 1.0 JSON object tree.
   *
   * @param event event to encode
   * @return JSON object node
   */
  public ObjectNode eventToObjectNode(PolarisEvent event) {
    ObjectNode root = MAPPER.createObjectNode();
    root.put("specversion", SPEC_VERSION);
    root.put("id", event.metadata().eventId().toString());
    root.put("type", cloudEventsType(event.type()));
    root.put("source", SOURCE);
    root.put("time", event.metadata().timestamp().toString());
    root.put("datacontenttype", DATA_CONTENT_TYPE);
    root.set("data", eventToDataNode(event));
    return root;
  }

  /**
   * Build the Polaris {@code data} object (Kafka-parity flat body).
   *
   * @param event event to encode
   * @return JSON object for the {@code data} field
   */
  public ObjectNode eventToDataNode(PolarisEvent event) {
    ObjectNode data = MAPPER.createObjectNode();

    data.put("event_type", event.type().name());
    data.put("event_id", event.metadata().eventId().toString());
    data.put("timestamp", event.metadata().timestamp().toString());
    event
        .attributes()
        .getOptional(EventAttributes.RENAME_TABLE_REQUEST)
        .ifPresent(
            renameTableRequest -> {
              data.set("source", MAPPER.valueToTree(tableIdToList(renameTableRequest.source())));
              data.set(
                  "destination",
                  MAPPER.valueToTree(tableIdToList(renameTableRequest.destination())));
            });
    event
        .attributes()
        .getOptional(EventAttributes.TABLE_NAME)
        .ifPresent(name -> data.put("table_name", name));
    event
        .attributes()
        .getOptional(EventAttributes.NAMESPACE)
        .map(Namespace::levels)
        .ifPresent(namespace -> data.set("namespace", MAPPER.valueToTree(namespace)));
    event
        .attributes()
        .getOptional(EventAttributes.TABLE_IDENTIFIER)
        .map(this::tableIdToList)
        .ifPresent(idAsList -> data.set("table_identifier", MAPPER.valueToTree(idAsList)));
    event
        .attributes()
        .getOptional(EventAttributes.VIEW_IDENTIFIER)
        .map(this::tableIdToList)
        .ifPresent(idAsList -> data.set("view_identifier", MAPPER.valueToTree(idAsList)));
    event
        .attributes()
        .getOptional(EventAttributes.VIEW_NAME)
        .ifPresent(name -> data.put("view_name", name));
    event
        .attributes()
        .getOptional(EventAttributes.NAMESPACE_NAME)
        .ifPresent(id -> data.put("namespace_name", id));
    event
        .attributes()
        .getOptional(EventAttributes.CATALOG_NAME)
        .ifPresent(id -> data.put("catalog_name", id));
    data.put("realm_id", event.metadata().realmId());
    event
        .metadata()
        .user()
        .ifPresent(
            user -> {
              data.put("principal", user.getName());
              data.set("activated_roles", MAPPER.valueToTree(user.getRoles()));
            });
    event.metadata().requestId().ifPresent(id -> data.put("request_id", id));

    return data;
  }

  /** CloudEvents {@code type} value for a Polaris event type. */
  public static String cloudEventsType(PolarisEventType type) {
    return TYPE_PREFIX + type.name().toLowerCase(Locale.ROOT);
  }

  private List<String> tableIdToList(TableIdentifier id) {
    return Stream.concat(
            id.hasNamespace() ? Stream.of(id.namespace().levels()) : Stream.empty(),
            Stream.of(id.name()))
        .toList();
  }
}
