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

import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

public final class EventToJsonMapper {
  private static final ObjectMapper MAPPER = JsonMapper.shared();

  /**
   * Extract useful information from PolarisEvent and encode as JSON String.
   *
   * @param event PolarisEvent to encode.
   * @return String containing JSON representation of event details.
   */
  public String eventToJson(PolarisEvent event) {
    ObjectNode rootNode = MAPPER.createObjectNode();

    rootNode.put("event_type", event.type().name());
    rootNode.put("event_id", event.metadata().eventId().toString());
    // Instant.toString returns an ISO-8601 representation.
    rootNode.put("timestamp", event.metadata().timestamp().toString());
    event
        .attributes()
        .get(EventAttributes.RENAME_TABLE_REQUEST)
        .ifPresent(
            renameTableRequest -> {
              rootNode.set(
                  "source", MAPPER.valueToTree(tableIdToList(renameTableRequest.source())));
              rootNode.set(
                  "destination",
                  MAPPER.valueToTree(tableIdToList(renameTableRequest.destination())));
            });
    event
        .attributes()
        .get(EventAttributes.TABLE_NAME)
        .ifPresent(name -> rootNode.put("table_name", name));
    event
        .attributes()
        .get(EventAttributes.NAMESPACE)
        .map(Namespace::levels)
        .ifPresent(namespace -> rootNode.set("namespace", MAPPER.valueToTree(namespace)));
    event
        .attributes()
        .get(EventAttributes.TABLE_IDENTIFIER)
        .map((id) -> tableIdToList(id))
        .ifPresent(idAsList -> rootNode.set("table_identifier", MAPPER.valueToTree(idAsList)));
    event
        .attributes()
        .get(EventAttributes.VIEW_IDENTIFIER)
        .map((id) -> tableIdToList(id))
        .ifPresent(idAsList -> rootNode.set("view_identifier", MAPPER.valueToTree(idAsList)));
    event
        .attributes()
        .get(EventAttributes.VIEW_NAME)
        .ifPresent(name -> rootNode.put("view_name", name));
    event
        .attributes()
        .get(EventAttributes.NAMESPACE_NAME)
        .ifPresent(id -> rootNode.put("namespace_name", id));
    event
        .attributes()
        .get(EventAttributes.CATALOG_NAME)
        .ifPresent(id -> rootNode.put("catalog_name", id));
    rootNode.put("realm_id", event.metadata().realmId());
    event
        .metadata()
        .user()
        .ifPresent(
            user -> {
              rootNode.put("principal", user.getName());
              rootNode.set("activated_roles", MAPPER.valueToTree(user.getRoles()));
            });
    event.metadata().requestId().ifPresent(id -> rootNode.put("request_id", id));

    return MAPPER.writeValueAsString(rootNode);
  }

  private List<String> tableIdToList(TableIdentifier id) {
    return Stream.concat(
            id.hasNamespace() ? Stream.of(id.namespace().levels()) : Stream.empty(),
            Stream.of(id.name()))
        .toList();
  }
}
