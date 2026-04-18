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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.events.AttributeKey;
import org.apache.polaris.service.events.EventAttributeMap;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventType;

public abstract class PolarisPersistenceEventListener implements PolarisEventListener {

  private static final ObjectMapper MAPPER = JsonMapper.builder().build();

  private static final String UNKNOWN_CATALOG = "unknown";

  private static final Set<AttributeKey<?>> CONFIGURATION_DENYLIST_FOR_SENSITIVE_ATTRIBUTES =
      Set.of(
          EventAttributes.PRINCIPAL,
          EventAttributes.UPDATE_PRINCIPAL_REQUEST,
          EventAttributes.CATALOG,
          EventAttributes.NOTIFICATION_REQUEST);

  // Only keys in this allowlist are persisted to additionalProperties.
  static final Set<AttributeKey<?>> DEFAULT_ALLOWLIST_FOR_PERSISTENCE =
      Set.of(
          EventAttributes.CATALOG_NAME,
          EventAttributes.NAMESPACE,
          EventAttributes.NAMESPACE_FQN,
          EventAttributes.PARENT_NAMESPACE_FQN,
          EventAttributes.TABLE_NAME,
          EventAttributes.TABLE_IDENTIFIER,
          EventAttributes.TABLE_METADATA,
          EventAttributes.RENAME_TABLE_REQUEST,
          EventAttributes.PURGE_REQUESTED,
          EventAttributes.VIEW_NAME,
          EventAttributes.VIEW_IDENTIFIER,
          EventAttributes.NAMESPACE_NAME,
          EventAttributes.GENERIC_TABLE_NAME);

  @Inject PolarisPersistenceEventListenerConfiguration persistenceListenerConfiguration;

  private Set<AttributeKey<?>> allowlistForPersistence;

  protected PolarisPersistenceEventListener() {
    this(DEFAULT_ALLOWLIST_FOR_PERSISTENCE);
  }

  PolarisPersistenceEventListener(Set<AttributeKey<?>> allowlistForPersistence) {
    this.allowlistForPersistence = validateConfiguredAllowlist(allowlistForPersistence);
  }

  @PostConstruct
  void initializeConfiguredAllowlist() {
    if (persistenceListenerConfiguration == null) {
      return;
    }

    Set<String> configuredAllowlistNames =
        persistenceListenerConfiguration.allowlistedAttributes().orElse(Collections.emptySet());
    this.allowlistForPersistence = resolveConfiguredAllowlist(configuredAllowlistNames);
  }

  static Set<AttributeKey<?>> resolveConfiguredAllowlist(Set<String> configuredAllowlistNames) {
    if (configuredAllowlistNames.isEmpty()) {
      return DEFAULT_ALLOWLIST_FOR_PERSISTENCE;
    }

    Set<AttributeKey<?>> resolvedAllowlist = new LinkedHashSet<>();
    List<String> unknownAttributeNames = new ArrayList<>();

    for (String attributeName : configuredAllowlistNames) {
      EventAttributes.findByName(attributeName)
          .ifPresentOrElse(resolvedAllowlist::add, () -> unknownAttributeNames.add(attributeName));
    }

    if (!unknownAttributeNames.isEmpty()) {
      throw new IllegalArgumentException(
          "Unknown event attributes in persistence allowlist configuration: "
              + unknownAttributeNames);
    }

    return validateConfiguredAllowlist(resolvedAllowlist);
  }

  private static Set<AttributeKey<?>> validateConfiguredAllowlist(
      Set<AttributeKey<?>> configuredAllowlist) {
    Set<AttributeKey<?>> deniedAttributes =
        configuredAllowlist.stream()
            .filter(CONFIGURATION_DENYLIST_FOR_SENSITIVE_ATTRIBUTES::contains)
            .collect(Collectors.toCollection(LinkedHashSet::new));

    if (!deniedAttributes.isEmpty()) {
      throw new IllegalArgumentException(
          "Sensitive attributes are not allowed in persistence allowlist configuration: "
              + deniedAttributes.stream().map(AttributeKey::name).toList());
    }

    return Set.copyOf(configuredAllowlist);
  }

  @Override
  public void onEvent(PolarisEvent event) {
    String catalogName = resolveCatalogName(event);
    org.apache.polaris.core.entity.PolarisEvent.ResourceType resourceType =
        resolveResourceType(event.type());
    String resourceIdentifier = resolveResourceIdentifier(event, resourceType, catalogName);

    org.apache.polaris.core.entity.PolarisEvent polarisEvent =
        new org.apache.polaris.core.entity.PolarisEvent(
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
    return event
        .attributes()
        .get(EventAttributes.CATALOG_NAME)
        .or(
            () ->
                event
                    .attributes()
                    .get(EventAttributes.CATALOG)
                    .map(Catalog::getName)
                    .filter(name -> !name.isBlank()))
        .orElse(UNKNOWN_CATALOG);
  }

  private static org.apache.polaris.core.entity.PolarisEvent.ResourceType resolveResourceType(
      PolarisEventType eventType) {
    int code = eventType.code();
    if (isInRange(
            code,
            PolarisEventType.BEFORE_CREATE_TABLE.code(),
            PolarisEventType.AFTER_REFRESH_TABLE.code())
        || isInRange(
            code,
            PolarisEventType.BEFORE_CREATE_GENERIC_TABLE.code(),
            PolarisEventType.AFTER_LOAD_GENERIC_TABLE.code())) {
      return org.apache.polaris.core.entity.PolarisEvent.ResourceType.TABLE;
    }

    if (isInRange(
        code,
        PolarisEventType.BEFORE_CREATE_VIEW.code(),
        PolarisEventType.AFTER_REFRESH_VIEW.code())) {
      return org.apache.polaris.core.entity.PolarisEvent.ResourceType.VIEW;
    }

    if (isInRange(
        code,
        PolarisEventType.BEFORE_CREATE_NAMESPACE.code(),
        PolarisEventType.AFTER_UPDATE_NAMESPACE_PROPERTIES.code())) {
      return org.apache.polaris.core.entity.PolarisEvent.ResourceType.NAMESPACE;
    }

    return org.apache.polaris.core.entity.PolarisEvent.ResourceType.CATALOG;
  }

  private static boolean isInRange(int value, int startInclusive, int endInclusive) {
    return value >= startInclusive && value <= endInclusive;
  }

  private static String resolveResourceIdentifier(
      PolarisEvent event,
      org.apache.polaris.core.entity.PolarisEvent.ResourceType resourceType,
      String catalogName) {
    return switch (resourceType) {
      case TABLE -> resolveTableResourceIdentifier(event, catalogName);
      case VIEW -> resolveViewResourceIdentifier(event, catalogName);
      case NAMESPACE -> resolveNamespaceResourceIdentifier(event, catalogName);
      case CATALOG -> resolveCatalogResourceIdentifier(event, catalogName);
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

  private static String resolveViewResourceIdentifier(PolarisEvent event, String catalogName) {
    EventAttributeMap attributes = event.attributes();
    return attributes
        .get(EventAttributes.VIEW_IDENTIFIER)
        .map(TableIdentifier::toString)
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
        .or(
            () ->
                event
                    .attributes()
                    .get(EventAttributes.CATALOG)
                    .map(Catalog::getName)
                    .filter(name -> !name.isBlank()))
        .orElseGet(() -> fallbackResourceIdentifier(event, catalogName));
  }

  private static String fallbackResourceIdentifier(PolarisEvent event, String catalogName) {
    if (!catalogName.equals(UNKNOWN_CATALOG)) {
      return catalogName;
    }
    return event.type().name();
  }

  private Map<String, String> buildAdditionalProperties(PolarisEvent event) {
    Map<String, String> additionalProperties =
        new LinkedHashMap<>(event.metadata().openTelemetryContext());
    event
        .attributes()
        .forEach((key, value) -> addIfSafe(event.type(), key, value, additionalProperties));
    return additionalProperties;
  }

  private void addIfSafe(
      PolarisEventType eventType,
      AttributeKey<?> key,
      Object value,
      Map<String, String> additionalProperties) {
    if (!allowlistForPersistence.contains(key)) {
      return;
    }

    additionalProperties.put(key.name(), serializeAttributeValue(eventType, key, value));
  }

  private static String serializeAttributeValue(
      PolarisEventType eventType, AttributeKey<?> key, Object value) {
    if (value instanceof String || value instanceof Number || value instanceof Boolean) {
      return value.toString();
    }

    if (value instanceof Namespace namespace) {
      return namespace.toString();
    }

    if (value instanceof TableIdentifier tableIdentifier) {
      return tableIdentifier.toString();
    }

    if (key.equals(EventAttributes.TABLE_METADATA) && value instanceof TableMetadata tableMetadata) {
      return TableMetadataParser.toJson(tableMetadata);
    }

    if (key.equals(EventAttributes.RENAME_TABLE_REQUEST)
        && value instanceof RenameTableRequest renameTableRequest) {
      try {
        return MAPPER.writeValueAsString(
            Map.of(
                "source", String.valueOf(renameTableRequest.source()),
                "destination", String.valueOf(renameTableRequest.destination())));
      } catch (JsonProcessingException ex) {
        throw new IllegalStateException(
            "Failed to serialize persistence-safe attribute "
                + key.name()
                + " for event "
                + eventType,
            ex);
      }
    }

    try {
      return MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException ex) {
      throw new IllegalStateException(
          "Failed to serialize persistence-safe attribute "
              + key.name()
              + " for event "
              + eventType,
          ex);
    }
  }

  protected abstract void processEvent(
      String realmId, org.apache.polaris.core.entity.PolarisEvent event);
}
