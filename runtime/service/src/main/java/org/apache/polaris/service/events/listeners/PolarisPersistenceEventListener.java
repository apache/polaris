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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.entity.EventEntity.ResourceType;
import org.apache.polaris.service.events.AttributeKey;
import org.apache.polaris.service.events.EventAttributeMap;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventType;

public abstract class PolarisPersistenceEventListener implements PolarisEventListener {

  protected PolarisPersistenceEventListener() {}

  @Override
  public void onEvent(PolarisEvent event) {
    String catalogName = resolveCatalogName(event);
    ResourceType resourceType = resolveResourceType(event.type());
    String resourceIdentifier = resolveResourceIdentifier(event, resourceType, catalogName);

    EventEntity polarisEvent =
        new EventEntity(
            catalogName,
            event.metadata().eventId().toString(),
            event.metadata().requestId().orElse(null),
            event.type().name(),
            event.metadata().timestamp().toEpochMilli(),
            event.metadata().user().map(PolarisPrincipal::getName).orElse(null),
            resourceType,
            resourceIdentifier);

    Map<String, String> additionalProperties = buildAdditionalProperties(event);
    if (!additionalProperties.isEmpty()) {
      polarisEvent.setAdditionalProperties(additionalProperties);
    }

    processEvent(event.metadata().realmId(), polarisEvent);
  }

  private static String resolveCatalogName(PolarisEvent event) {
    return event.attributes().get(EventAttributes.CATALOG_NAME).orElse(EventEntity.REALM_SCOPED);
  }

  /**
   * Maps an event's category to a {@link ResourceType} for indexing. Exhaustive over all {@link
   * PolarisEventType.Category} values: any new category added to the enum must be classified here
   * explicitly. The {@code default} arm is intentionally absent so that adding a category produces
   * a compile-time error rather than silently routing to {@link ResourceType#REALM}.
   */
  private static ResourceType resolveResourceType(PolarisEventType eventType) {
    return switch (eventType.category()) {
      case TABLE, GENERIC_TABLE -> ResourceType.TABLE;
      case VIEW -> ResourceType.VIEW;
      case NAMESPACE -> ResourceType.NAMESPACE;
      case CATALOG -> ResourceType.CATALOG;
      case POLICY,
          PRINCIPAL,
          PRINCIPAL_ROLE,
          CATALOG_ROLE,
          CREDENTIAL,
          TRANSACTION,
          NOTIFICATION,
          CONFIG,
          TASK_EXECUTION,
          RATE_LIMITING ->
          ResourceType.REALM;
    };
  }

  private static String resolveResourceIdentifier(
      PolarisEvent event, ResourceType resourceType, String catalogName) {
    return switch (resourceType) {
      case TABLE -> resolveTableResourceIdentifier(event, catalogName);
      case VIEW -> resolveViewResourceIdentifier(event, catalogName);
      case NAMESPACE -> resolveNamespaceResourceIdentifier(event, catalogName);
      case CATALOG -> resolveCatalogResourceIdentifier(event, catalogName);
      case REALM -> resolveRealmResourceIdentifier(event);
    };
  }

  private static String resolveTableResourceIdentifier(PolarisEvent event, String catalogName) {
    EventAttributeMap attributes = event.attributes();

    Optional<String> identifierFromTableAttribute =
        attributes.get(EventAttributes.TABLE_IDENTIFIER).map(TableIdentifier::toString);
    if (identifierFromTableAttribute.isPresent()) {
      return identifierFromTableAttribute.get();
    }

    Optional<String> renameIdentifier = resolveRenameIdentifier(event);
    if (renameIdentifier.isPresent()) {
      return renameIdentifier.get();
    }

    Optional<String> namespaceAndTableName = resolveNamespaceAndTableName(attributes);
    if (namespaceAndTableName.isPresent()) {
      return namespaceAndTableName.get();
    }

    Optional<String> namespaceAndCreateTableName = resolveCreateTableIdentifier(attributes);
    if (namespaceAndCreateTableName.isPresent()) {
      return namespaceAndCreateTableName.get();
    }

    Optional<String> namespaceAndRegisterTableName = resolveRegisterTableIdentifier(attributes);
    if (namespaceAndRegisterTableName.isPresent()) {
      return namespaceAndRegisterTableName.get();
    }

    Optional<String> namespaceIdentifier =
        attributes.get(EventAttributes.NAMESPACE).map(Namespace::toString);
    if (namespaceIdentifier.isPresent()) {
      return namespaceIdentifier.get();
    }

    return attributes
        .get(EventAttributes.TABLE_NAME)
        .orElseGet(() -> fallbackResourceIdentifier(event, catalogName));
  }

  private static String resolveViewResourceIdentifier(PolarisEvent event, String catalogName) {
    EventAttributeMap attributes = event.attributes();
    return attributes
        .get(EventAttributes.VIEW_IDENTIFIER)
        .map(TableIdentifier::toString)
        .or(() -> resolveRenameIdentifier(event))
        .or(
            () ->
                attributes
                    .get(EventAttributes.NAMESPACE)
                    .flatMap(
                        namespace ->
                            attributes
                                .get(EventAttributes.VIEW_NAME)
                                .map(
                                    viewName ->
                                        TableIdentifier.of(namespace, viewName).toString())))
        .or(() -> attributes.get(EventAttributes.VIEW_NAME))
        .orElseGet(() -> fallbackResourceIdentifier(event, catalogName));
  }

  private static Optional<String> resolveRenameIdentifier(PolarisEvent event) {
    return event
        .attributes()
        .get(EventAttributes.RENAME_TABLE_REQUEST)
        .map(
            request ->
                event.type().name().startsWith("AFTER_")
                    ? request.destination().toString()
                    : request.source().toString());
  }

  private static Optional<String> resolveNamespaceAndTableName(EventAttributeMap attributes) {
    return attributes
        .get(EventAttributes.NAMESPACE)
        .flatMap(
            namespace ->
                attributes
                    .get(EventAttributes.TABLE_NAME)
                    .map(tableName -> TableIdentifier.of(namespace, tableName).toString()));
  }

  private static Optional<String> resolveCreateTableIdentifier(EventAttributeMap attributes) {
    return attributes
        .get(EventAttributes.NAMESPACE)
        .flatMap(
            namespace ->
                attributes
                    .get(EventAttributes.CREATE_TABLE_REQUEST)
                    .map(CreateTableRequest::name)
                    .map(tableName -> TableIdentifier.of(namespace, tableName).toString()));
  }

  private static Optional<String> resolveRegisterTableIdentifier(EventAttributeMap attributes) {
    return attributes
        .get(EventAttributes.NAMESPACE)
        .flatMap(
            namespace ->
                attributes
                    .get(EventAttributes.REGISTER_TABLE_REQUEST)
                    .map(RegisterTableRequest::name)
                    .map(tableName -> TableIdentifier.of(namespace, tableName).toString()));
  }

  private static String resolveNamespaceResourceIdentifier(PolarisEvent event, String catalogName) {
    EventAttributeMap attributes = event.attributes();
    return attributes
        .get(EventAttributes.NAMESPACE)
        .map(Namespace::toString)
        .or(() -> attributes.get(EventAttributes.NAMESPACE_FQN))
        .or(() -> attributes.get(EventAttributes.PARENT_NAMESPACE_FQN))
        .orElseGet(() -> fallbackResourceIdentifier(event, catalogName));
  }

  private static String resolveCatalogResourceIdentifier(PolarisEvent event, String catalogName) {
    return event
        .attributes()
        .get(EventAttributes.CATALOG_NAME)
        .orElseGet(() -> fallbackResourceIdentifier(event, catalogName));
  }

  private static String resolveRealmResourceIdentifier(PolarisEvent event) {
    return event.type().name();
  }

  private static String fallbackResourceIdentifier(PolarisEvent event, String catalogName) {
    if (!EventEntity.REALM_SCOPED.equals(catalogName)) {
      return catalogName;
    }
    return event.type().name();
  }

  private static Map<String, String> buildAdditionalProperties(PolarisEvent event) {
    Map<String, String> additionalProperties =
        new LinkedHashMap<>(event.metadata().openTelemetryContext());
    event.attributes().forEach((key, value) -> additionalProperties.putAll(prune(key, value)));
    return additionalProperties;
  }

  /**
   * Reduces an attribute value to a bounded string representation suitable for storage in the
   * persistence event row. Inlined here (rather than exposed as a swappable bean) because pruning
   * is a serialization concern of this listener only — other listeners are free to render the same
   * value differently.
   */
  private static Map<String, String> prune(AttributeKey<?> key, Object value) {
    if (value instanceof String || value instanceof Number || value instanceof Boolean) {
      return Map.of(key.name(), value.toString());
    }

    if (value instanceof Namespace namespace) {
      return Map.of(key.name(), namespace.toString());
    }

    if (value instanceof TableIdentifier tableIdentifier) {
      return Map.of(key.name(), tableIdentifier.toString());
    }

    if (key.equals(EventAttributes.TABLE_METADATA) && value instanceof TableMetadata metadata) {
      return pruneTableMetadata(metadata);
    }

    if (key.equals(EventAttributes.LOAD_TABLE_RESPONSE)
        && value instanceof LoadTableResponse response) {
      return pruneTableMetadata(response.tableMetadata());
    }

    if (key.equals(EventAttributes.RENAME_TABLE_REQUEST)
        && value instanceof RenameTableRequest request) {
      return Map.ofEntries(
          Map.entry("rename_source", request.source().toString()),
          Map.entry("rename_destination", request.destination().toString()));
    }

    return Map.of(key.name(), value.toString());
  }

  private static Map<String, String> pruneTableMetadata(TableMetadata metadata) {
    if (metadata == null) {
      return Map.of();
    }
    Map<String, String> summary = new LinkedHashMap<>();
    if (metadata.uuid() != null) {
      summary.put("table_uuid", metadata.uuid());
    }
    summary.put("table_location", metadata.location());
    summary.put("table_format_version", String.valueOf(metadata.formatVersion()));
    summary.put(
        "table_current_snapshot_id",
        String.valueOf(
            metadata.currentSnapshot() != null ? metadata.currentSnapshot().snapshotId() : -1));
    summary.put("table_schema", metadata.schema().toString());
    summary.put("table_last_updated_ms", String.valueOf(metadata.lastUpdatedMillis()));
    return summary;
  }

  protected abstract void processEvent(String realmId, EventEntity event);
}
