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

package org.apache.polaris.service.events.listeners;

import com.google.common.collect.ImmutableMap;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventType;

final class TableEventHandler implements PersistenceEventHandler {

  private final Function<PolarisEvent, String> resourceIdentifierResolver;
  private final Function<PolarisEvent, Map<String, String>> additionalPropertiesResolver;

  private TableEventHandler(
      Function<PolarisEvent, String> resourceIdentifierResolver,
      Function<PolarisEvent, Map<String, String>> additionalPropertiesResolver) {
    this.resourceIdentifierResolver = resourceIdentifierResolver;
    this.additionalPropertiesResolver = additionalPropertiesResolver;
  }

  static Map<PolarisEventType, PersistenceEventHandler> createHandlers() {
    EnumMap<PolarisEventType, PersistenceEventHandler> handlers =
        new EnumMap<>(PolarisEventType.class);

    handlers.put(
        PolarisEventType.BEFORE_CREATE_TABLE,
        new TableEventHandler(
            TableEventHandler::namespaceAndCreateTableName,
            TableEventHandler::noAdditionalProperties));
    handlers.put(
        PolarisEventType.AFTER_CREATE_TABLE,
        new TableEventHandler(
            TableEventHandler::namespaceAndTableName,
            TableEventHandler::loadTableResponseProperties));
    handlers.put(
        PolarisEventType.BEFORE_LIST_TABLES,
        new TableEventHandler(
            TableEventHandler::namespaceOnly, TableEventHandler::noAdditionalProperties));
    handlers.put(
        PolarisEventType.AFTER_LIST_TABLES,
        new TableEventHandler(
            TableEventHandler::namespaceOnly, TableEventHandler::noAdditionalProperties));
    handlers.put(
        PolarisEventType.BEFORE_LOAD_TABLE,
        new TableEventHandler(
            TableEventHandler::namespaceAndTableName, TableEventHandler::noAdditionalProperties));
    handlers.put(
        PolarisEventType.AFTER_LOAD_TABLE,
        new TableEventHandler(
            TableEventHandler::namespaceAndTableName,
            TableEventHandler::loadTableResponseProperties));
    handlers.put(
        PolarisEventType.BEFORE_CHECK_EXISTS_TABLE,
        new TableEventHandler(
            TableEventHandler::namespaceAndTableName, TableEventHandler::noAdditionalProperties));
    handlers.put(
        PolarisEventType.AFTER_CHECK_EXISTS_TABLE,
        new TableEventHandler(
            TableEventHandler::namespaceAndTableName, TableEventHandler::noAdditionalProperties));
    handlers.put(
        PolarisEventType.BEFORE_DROP_TABLE,
        new TableEventHandler(
            TableEventHandler::namespaceAndTableName, TableEventHandler::dropTableProperties));
    handlers.put(
        PolarisEventType.AFTER_DROP_TABLE,
        new TableEventHandler(
            TableEventHandler::namespaceAndTableName, TableEventHandler::dropTableProperties));
    handlers.put(
        PolarisEventType.BEFORE_REGISTER_TABLE,
        new TableEventHandler(
            TableEventHandler::namespaceAndRegisterTableName,
            TableEventHandler::noAdditionalProperties));
    handlers.put(
        PolarisEventType.AFTER_REGISTER_TABLE,
        new TableEventHandler(
            TableEventHandler::namespaceAndTableName,
            TableEventHandler::loadTableResponseProperties));
    handlers.put(
        PolarisEventType.BEFORE_RENAME_TABLE,
        new TableEventHandler(
            TableEventHandler::renameSourceIdentifier, TableEventHandler::renameProperties));
    handlers.put(
        PolarisEventType.AFTER_RENAME_TABLE,
        new TableEventHandler(
            TableEventHandler::renameDestinationIdentifier, TableEventHandler::renameProperties));
    handlers.put(
        PolarisEventType.BEFORE_UPDATE_TABLE,
        new TableEventHandler(
            TableEventHandler::namespaceAndTableName, TableEventHandler::noAdditionalProperties));
    handlers.put(
        PolarisEventType.AFTER_UPDATE_TABLE,
        new TableEventHandler(
            TableEventHandler::namespaceAndTableName,
            TableEventHandler::tableMetadataPropertiesFromAttribute));
    handlers.put(
        PolarisEventType.BEFORE_REFRESH_TABLE,
        new TableEventHandler(
            TableEventHandler::tableIdentifierAttribute,
            TableEventHandler::noAdditionalProperties));
    handlers.put(
        PolarisEventType.AFTER_REFRESH_TABLE,
        new TableEventHandler(
            TableEventHandler::tableIdentifierAttribute,
            TableEventHandler::noAdditionalProperties));

    return handlers;
  }

  @Override
  public void handle(PolarisEvent event, String realmId, PolarisPersistenceEventListener parent) {
    String catalogName = event.attributes().getRequired(EventAttributes.CATALOG_NAME);
    parent.persistEvent(
        event,
        realmId,
        org.apache.polaris.core.entity.PolarisEvent.ResourceType.TABLE,
        catalogName,
        resourceIdentifierResolver.apply(event),
        additionalPropertiesResolver.apply(event));
  }

  private static String namespaceAndCreateTableName(PolarisEvent event) {
    Namespace namespace = event.attributes().getRequired(EventAttributes.NAMESPACE);
    var createTableRequest = event.attributes().getRequired(EventAttributes.CREATE_TABLE_REQUEST);
    return TableIdentifier.of(namespace, createTableRequest.name()).toString();
  }

  private static String namespaceAndRegisterTableName(PolarisEvent event) {
    Namespace namespace = event.attributes().getRequired(EventAttributes.NAMESPACE);
    var registerTableRequest =
        event.attributes().getRequired(EventAttributes.REGISTER_TABLE_REQUEST);
    return TableIdentifier.of(namespace, registerTableRequest.name()).toString();
  }

  private static String namespaceAndTableName(PolarisEvent event) {
    Namespace namespace = event.attributes().getRequired(EventAttributes.NAMESPACE);
    String tableName = event.attributes().getRequired(EventAttributes.TABLE_NAME);
    return TableIdentifier.of(namespace, tableName).toString();
  }

  private static String namespaceOnly(PolarisEvent event) {
    Namespace namespace = event.attributes().getRequired(EventAttributes.NAMESPACE);
    return namespace.toString();
  }

  private static String tableIdentifierAttribute(PolarisEvent event) {
    return String.valueOf(event.attributes().getRequired(EventAttributes.TABLE_IDENTIFIER));
  }

  private static String renameSourceIdentifier(PolarisEvent event) {
    var renameTableRequest = event.attributes().getRequired(EventAttributes.RENAME_TABLE_REQUEST);
    return renameTableRequest.source().toString();
  }

  private static String renameDestinationIdentifier(PolarisEvent event) {
    var renameTableRequest = event.attributes().getRequired(EventAttributes.RENAME_TABLE_REQUEST);
    return renameTableRequest.destination().toString();
  }

  private static Map<String, String> noAdditionalProperties(PolarisEvent event) {
    return Map.of();
  }

  private static Map<String, String> renameProperties(PolarisEvent event) {
    var renameTableRequest = event.attributes().getRequired(EventAttributes.RENAME_TABLE_REQUEST);
    return ImmutableMap.<String, String>builder()
        .put("source-table", renameTableRequest.source().toString())
        .put("destination-table", renameTableRequest.destination().toString())
        .build();
  }

  private static Map<String, String> dropTableProperties(PolarisEvent event) {
    return event
        .attributes()
        .get(EventAttributes.PURGE_REQUESTED)
        .map(purgeRequested -> Map.of("purge-requested", purgeRequested.toString()))
        .orElseGet(Map::of);
  }

  private static Map<String, String> loadTableResponseProperties(PolarisEvent event) {
    var loadTableResponse = event.attributes().getRequired(EventAttributes.LOAD_TABLE_RESPONSE);
    return tableMetadataProperties(loadTableResponse.tableMetadata());
  }

  private static Map<String, String> tableMetadataPropertiesFromAttribute(PolarisEvent event) {
    return event
        .attributes()
        .get(EventAttributes.TABLE_METADATA)
        .map(TableEventHandler::tableMetadataProperties)
        .orElseGet(Map::of);
  }

  private static Map<String, String> tableMetadataProperties(TableMetadata tableMetadata) {
    return ImmutableMap.<String, String>builder()
        .put("table-uuid", tableMetadata.uuid())
        .put("metadata", TableMetadataParser.toJson(tableMetadata))
        .build();
  }
}
