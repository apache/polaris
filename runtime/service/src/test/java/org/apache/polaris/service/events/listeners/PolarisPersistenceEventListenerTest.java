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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.events.AttributeKey;
import org.apache.polaris.service.events.EventAttributeMap;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventMetadata;
import org.apache.polaris.service.events.PolarisEventType;
import org.junit.jupiter.api.Test;

class PolarisPersistenceEventListenerTest {

  private static final String REALM_ID = "test-realm";
  private static final String CATALOG_NAME = "test-catalog";
  private static final Namespace NAMESPACE = Namespace.of("db", "team");
  private static final String TABLE_NAME = "table1";
  private static final String CREATE_TABLE_NAME = "created_table";
  private static final String REGISTER_TABLE_NAME = "registered_table";
  private static final TableIdentifier RENAME_SOURCE = TableIdentifier.of(NAMESPACE, "source");
  private static final TableIdentifier RENAME_DESTINATION =
      TableIdentifier.of(NAMESPACE, "destination");
  private static final TableIdentifier REFRESH_TABLE_IDENTIFIER =
      TableIdentifier.of(NAMESPACE, "refresh_target");

  private static final TableMetadata TABLE_METADATA =
      TableMetadata.buildFromEmpty()
          .assignUUID()
          .setLocation("file:///tmp/test-table")
          .addSchema(new Schema(List.of(Types.NestedField.required(1, "id", Types.LongType.get()))))
          .addPartitionSpec(PartitionSpec.unpartitioned())
          .addSortOrder(SortOrder.unsorted())
          .build();

  private static final List<PolarisEventType> TABLE_EVENT_TYPES =
      List.of(
          PolarisEventType.BEFORE_CREATE_TABLE,
          PolarisEventType.AFTER_CREATE_TABLE,
          PolarisEventType.BEFORE_LIST_TABLES,
          PolarisEventType.AFTER_LIST_TABLES,
          PolarisEventType.BEFORE_LOAD_TABLE,
          PolarisEventType.AFTER_LOAD_TABLE,
          PolarisEventType.BEFORE_CHECK_EXISTS_TABLE,
          PolarisEventType.AFTER_CHECK_EXISTS_TABLE,
          PolarisEventType.BEFORE_DROP_TABLE,
          PolarisEventType.AFTER_DROP_TABLE,
          PolarisEventType.BEFORE_REGISTER_TABLE,
          PolarisEventType.AFTER_REGISTER_TABLE,
          PolarisEventType.BEFORE_RENAME_TABLE,
          PolarisEventType.AFTER_RENAME_TABLE,
          PolarisEventType.BEFORE_UPDATE_TABLE,
          PolarisEventType.AFTER_UPDATE_TABLE,
          PolarisEventType.BEFORE_REFRESH_TABLE,
          PolarisEventType.AFTER_REFRESH_TABLE);

  @Test
  void shouldPersistAllTableEventsWithGenericResolution() {
    CapturingPersistenceListener listener = new CapturingPersistenceListener();

    TABLE_EVENT_TYPES.forEach(eventType -> listener.onEvent(tableEvent(eventType)));

    assertThat(listener.persistedEventsByType()).hasSize(TABLE_EVENT_TYPES.size());

    for (PolarisEventType eventType : TABLE_EVENT_TYPES) {
      org.apache.polaris.core.entity.PolarisEvent persisted = listener.persistedEvent(eventType);
      assertThat(listener.persistedRealm(eventType)).isEqualTo(REALM_ID);
      assertThat(persisted.getCatalogId()).isEqualTo(CATALOG_NAME);
      assertThat(persisted.getEventType()).isEqualTo(eventType.name());
      assertThat(persisted.getResourceType())
          .isEqualTo(org.apache.polaris.core.entity.PolarisEvent.ResourceType.TABLE);
      assertThat(persisted.getResourceIdentifier())
          .isEqualTo(expectedResourceIdentifier(eventType));
    }

    assertThat(additionalProperties(listener.persistedEvent(PolarisEventType.AFTER_UPDATE_TABLE)))
        .containsKey(EventAttributes.TABLE_METADATA.name());
    assertThat(additionalProperties(listener.persistedEvent(PolarisEventType.BEFORE_RENAME_TABLE)))
        .containsKey(EventAttributes.RENAME_TABLE_REQUEST.name());
  }

  @Test
  void shouldSerializeAllowlistedAttributesAndIgnoreSensitiveOnes() {
    CapturingPersistenceListener listener = new CapturingPersistenceListener();

    listener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_LIST_TABLES,
            metadataWithOpenTelemetry(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, CATALOG_NAME)
                .put(EventAttributes.NAMESPACE, NAMESPACE)
                .put(EventAttributes.PRINCIPAL_NAME, "alice")
                .put(EventAttributes.HTTP_METHOD, "GET")));

    Map<String, String> properties =
        additionalProperties(listener.persistedEvent(PolarisEventType.BEFORE_LIST_TABLES));
    assertThat(properties)
        .containsEntry(EventAttributes.CATALOG_NAME.name(), CATALOG_NAME)
        .containsKey(EventAttributes.NAMESPACE.name())
        .containsEntry("otel.trace_id", "trace-123")
        .containsEntry("otel.span_id", "span-456")
        .doesNotContainKeys(EventAttributes.PRINCIPAL_NAME.name(), EventAttributes.HTTP_METHOD.name());
  }

      @Test
      void shouldApplyConfiguredAllowlistFromAttributeNames() {
      Set<AttributeKey<?>> configuredAllowlist =
        PolarisPersistenceEventListener.resolveConfiguredAllowlist(
          Set.of(EventAttributes.CATALOG_NAME.name(), EventAttributes.NAMESPACE.name()));
      CapturingPersistenceListener listener = new CapturingPersistenceListener(configuredAllowlist);

      listener.onEvent(beforeListTablesEvent(metadataWithOpenTelemetry()));

      Map<String, String> properties =
        additionalProperties(listener.persistedEvent(PolarisEventType.BEFORE_LIST_TABLES));
      assertThat(properties)
        .containsEntry(EventAttributes.CATALOG_NAME.name(), CATALOG_NAME)
        .containsKey(EventAttributes.NAMESPACE.name())
        .containsEntry("otel.trace_id", "trace-123")
        .containsEntry("otel.span_id", "span-456");
      }

      @Test
      void shouldRejectSensitiveAttributesInConfiguredAllowlist() {
      assertThatThrownBy(
          () ->
            PolarisPersistenceEventListener.resolveConfiguredAllowlist(
              Set.of(EventAttributes.CATALOG_NAME.name(), EventAttributes.PRINCIPAL.name())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(EventAttributes.PRINCIPAL.name());
      }

  @Test
  void shouldPersistCatalogEventUsingCatalogAttributeWhenCatalogNameMissing() {
    CapturingPersistenceListener listener = new CapturingPersistenceListener();

    Catalog catalog = mock(Catalog.class);
    when(catalog.getName()).thenReturn(CATALOG_NAME);

    listener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_CREATE_CATALOG,
            metadata(),
            new EventAttributeMap().put(EventAttributes.CATALOG, catalog)));

    org.apache.polaris.core.entity.PolarisEvent persisted =
        listener.persistedEvent(PolarisEventType.AFTER_CREATE_CATALOG);
    assertThat(listener.persistedRealm(PolarisEventType.AFTER_CREATE_CATALOG)).isEqualTo(REALM_ID);
    assertThat(persisted.getCatalogId()).isEqualTo(CATALOG_NAME);
    assertThat(persisted.getResourceType())
        .isEqualTo(org.apache.polaris.core.entity.PolarisEvent.ResourceType.CATALOG);
    assertThat(persisted.getResourceIdentifier()).isEqualTo(CATALOG_NAME);
    assertThat(additionalProperties(persisted)).isEmpty();
  }

  @Test
  void shouldFallbackWhenNoCatalogOrResourceAttributesExist() {
    CapturingPersistenceListener listener = new CapturingPersistenceListener();

    listener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_LIMIT_REQUEST_RATE,
            metadata(),
            new EventAttributeMap()
                .put(EventAttributes.HTTP_METHOD, "GET")
                .put(EventAttributes.REQUEST_URI, "/v1/catalogs")));

    org.apache.polaris.core.entity.PolarisEvent persisted =
        listener.persistedEvent(PolarisEventType.BEFORE_LIMIT_REQUEST_RATE);
    assertThat(persisted.getCatalogId()).isEqualTo("unknown");
    assertThat(persisted.getResourceType())
        .isEqualTo(org.apache.polaris.core.entity.PolarisEvent.ResourceType.CATALOG);
    assertThat(persisted.getResourceIdentifier())
        .isEqualTo(PolarisEventType.BEFORE_LIMIT_REQUEST_RATE.name());
    assertThat(additionalProperties(persisted)).isEmpty();
  }

  @Test
  void shouldPersistRequestUserAndTimestampMetadataFields() {
    CapturingPersistenceListener listener = new CapturingPersistenceListener();
    Instant timestamp = Instant.parse("2024-01-02T03:04:05Z");
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), java.util.Set.of("role1"));
    PolarisEventMetadata metadata =
        PolarisEventMetadata.builder()
            .realmId(REALM_ID)
            .requestId("request-123")
            .user(principal)
            .timestamp(timestamp)
            .build();

    listener.onEvent(beforeListTablesEvent(metadata));

    org.apache.polaris.core.entity.PolarisEvent persisted =
        listener.persistedEvent(PolarisEventType.BEFORE_LIST_TABLES);
    assertThat(persisted.getRequestId()).isEqualTo("request-123");
    assertThat(persisted.getPrincipalName()).isEqualTo("alice");
    assertThat(persisted.getTimestampMs()).isEqualTo(timestamp.toEpochMilli());
  }

  @Test
  void shouldWrapJsonSerializationFailures() {
    CapturingPersistenceListener listener = new CapturingPersistenceListener();

    EventAttributeMap attributes =
        new EventAttributeMap()
            .put(EventAttributes.CATALOG_NAME, CATALOG_NAME)
            .put(EventAttributes.NAMESPACE, NAMESPACE)
            .put(EventAttributes.TABLE_NAME, TABLE_NAME);
    putUntyped(attributes, EventAttributes.TABLE_METADATA, new Object());

    assertThatThrownBy(
            () ->
                listener.onEvent(
                    new PolarisEvent(PolarisEventType.BEFORE_UPDATE_TABLE, metadata(), attributes)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("table_metadata")
        .hasMessageContaining(PolarisEventType.BEFORE_UPDATE_TABLE.name());
  }

  @Test
  void shouldPropagateProcessEventErrors() {
    PolarisPersistenceEventListener listener =
        new PolarisPersistenceEventListener() {
          @Override
          protected void processEvent(
              String realmId, org.apache.polaris.core.entity.PolarisEvent event) {
            throw new IllegalStateException("persist failure");
          }
        };

    assertThatThrownBy(() -> listener.onEvent(beforeListTablesEvent(metadata())))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("persist failure");
  }

  private static PolarisEvent tableEvent(PolarisEventType eventType) {
    EventAttributeMap attributes =
        new EventAttributeMap().put(EventAttributes.CATALOG_NAME, CATALOG_NAME);

    switch (eventType) {
      case BEFORE_CREATE_TABLE ->
          attributes
              .put(EventAttributes.NAMESPACE, NAMESPACE)
              .put(EventAttributes.CREATE_TABLE_REQUEST, createTableRequest());
      case AFTER_CREATE_TABLE,
          BEFORE_LOAD_TABLE,
          AFTER_LOAD_TABLE,
          BEFORE_CHECK_EXISTS_TABLE,
          AFTER_CHECK_EXISTS_TABLE,
          BEFORE_DROP_TABLE,
          BEFORE_UPDATE_TABLE ->
          attributes
              .put(EventAttributes.NAMESPACE, NAMESPACE)
              .put(EventAttributes.TABLE_NAME, TABLE_NAME);
      case BEFORE_LIST_TABLES, AFTER_LIST_TABLES ->
          attributes.put(EventAttributes.NAMESPACE, NAMESPACE);
      case AFTER_DROP_TABLE ->
          attributes
              .put(EventAttributes.NAMESPACE, NAMESPACE)
              .put(EventAttributes.TABLE_NAME, TABLE_NAME)
              .put(EventAttributes.PURGE_REQUESTED, true);
      case BEFORE_REGISTER_TABLE ->
          attributes
              .put(EventAttributes.NAMESPACE, NAMESPACE)
              .put(EventAttributes.REGISTER_TABLE_REQUEST, registerTableRequest());
      case AFTER_REGISTER_TABLE ->
          attributes
              .put(EventAttributes.NAMESPACE, NAMESPACE)
              .put(EventAttributes.TABLE_NAME, REGISTER_TABLE_NAME);
      case BEFORE_RENAME_TABLE, AFTER_RENAME_TABLE ->
          attributes.put(EventAttributes.RENAME_TABLE_REQUEST, renameTableRequest());
      case AFTER_UPDATE_TABLE ->
          attributes
              .put(EventAttributes.NAMESPACE, NAMESPACE)
              .put(EventAttributes.TABLE_NAME, TABLE_NAME)
              .put(EventAttributes.TABLE_METADATA, TABLE_METADATA);
      case BEFORE_REFRESH_TABLE, AFTER_REFRESH_TABLE ->
          attributes.put(EventAttributes.TABLE_IDENTIFIER, REFRESH_TABLE_IDENTIFIER);
      default -> throw new IllegalArgumentException("Unexpected table event type " + eventType);
    }

    return new PolarisEvent(eventType, metadata(), attributes);
  }

  private static PolarisEventMetadata metadata() {
    return PolarisEventMetadata.builder().realmId(REALM_ID).build();
  }

  private static PolarisEventMetadata metadataWithOpenTelemetry() {
    return PolarisEventMetadata.builder()
        .realmId(REALM_ID)
        .openTelemetryContext(Map.of("otel.trace_id", "trace-123", "otel.span_id", "span-456"))
        .build();
  }

  private static PolarisEvent beforeListTablesEvent(PolarisEventMetadata metadata) {
    return new PolarisEvent(
        PolarisEventType.BEFORE_LIST_TABLES,
        metadata,
        new EventAttributeMap()
            .put(EventAttributes.CATALOG_NAME, CATALOG_NAME)
            .put(EventAttributes.NAMESPACE, NAMESPACE));
  }

  private static CreateTableRequest createTableRequest() {
    return CreateTableRequest.builder()
        .withName(CREATE_TABLE_NAME)
        .withSchema(TABLE_METADATA.schema())
        .build();
  }

  private static RegisterTableRequest registerTableRequest() {
    RegisterTableRequest request = mock(RegisterTableRequest.class);
    when(request.name()).thenReturn(REGISTER_TABLE_NAME);
    return request;
  }

  private static RenameTableRequest renameTableRequest() {
    return RenameTableRequest.builder()
        .withSource(RENAME_SOURCE)
        .withDestination(RENAME_DESTINATION)
        .build();
  }

  private static String expectedResourceIdentifier(PolarisEventType eventType) {
    return switch (eventType) {
      case BEFORE_CREATE_TABLE -> TableIdentifier.of(NAMESPACE, CREATE_TABLE_NAME).toString();
      case AFTER_CREATE_TABLE,
          BEFORE_LOAD_TABLE,
          AFTER_LOAD_TABLE,
          BEFORE_CHECK_EXISTS_TABLE,
          AFTER_CHECK_EXISTS_TABLE,
          BEFORE_DROP_TABLE,
          AFTER_DROP_TABLE,
          BEFORE_UPDATE_TABLE,
          AFTER_UPDATE_TABLE ->
          TableIdentifier.of(NAMESPACE, TABLE_NAME).toString();
      case BEFORE_LIST_TABLES, AFTER_LIST_TABLES -> NAMESPACE.toString();
      case BEFORE_REGISTER_TABLE, AFTER_REGISTER_TABLE ->
          TableIdentifier.of(NAMESPACE, REGISTER_TABLE_NAME).toString();
      case BEFORE_RENAME_TABLE -> RENAME_SOURCE.toString();
      case AFTER_RENAME_TABLE -> RENAME_DESTINATION.toString();
      case BEFORE_REFRESH_TABLE, AFTER_REFRESH_TABLE -> REFRESH_TABLE_IDENTIFIER.toString();
      default -> throw new IllegalArgumentException("Unexpected table event type " + eventType);
    };
  }

  private static Map<String, String> additionalProperties(
      org.apache.polaris.core.entity.PolarisEvent event) {
    return event.getAdditionalPropertiesAsMap();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void putUntyped(
      EventAttributeMap attributes, AttributeKey key, Object value) {
    attributes.put(key, value);
  }

  private static final class CapturingPersistenceListener extends PolarisPersistenceEventListener {
    private final Map<PolarisEventType, org.apache.polaris.core.entity.PolarisEvent>
        persistedEventsByType = new LinkedHashMap<>();
    private final Map<PolarisEventType, String> persistedRealmsByType = new LinkedHashMap<>();

    private CapturingPersistenceListener() {}

    private CapturingPersistenceListener(Set<AttributeKey<?>> allowlistForPersistence) {
      super(allowlistForPersistence);
    }

    @Override
    protected void processEvent(String realmId, org.apache.polaris.core.entity.PolarisEvent event) {
      PolarisEventType eventType = PolarisEventType.valueOf(event.getEventType());
      persistedEventsByType.put(eventType, event);
      persistedRealmsByType.put(eventType, realmId);
    }

    Map<PolarisEventType, org.apache.polaris.core.entity.PolarisEvent> persistedEventsByType() {
      return persistedEventsByType;
    }

    org.apache.polaris.core.entity.PolarisEvent persistedEvent(PolarisEventType eventType) {
      return persistedEventsByType.get(eventType);
    }

    String persistedRealm(PolarisEventType eventType) {
      return persistedRealmsByType.get(eventType);
    }
  }
}
