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
package org.apache.polaris.service.events.listeners.opentelemetry;

import static io.opentelemetry.api.common.AttributeKey.booleanKey;
import static io.opentelemetry.api.common.AttributeKey.stringArrayKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.apache.polaris.service.events.PolarisEventMetadata.OPEN_TELEMETRY_SAMPLED_KEY;
import static org.apache.polaris.service.events.PolarisEventMetadata.OPEN_TELEMETRY_SPAN_ID_KEY;
import static org.apache.polaris.service.events.PolarisEventMetadata.OPEN_TELEMETRY_TRACE_FLAGS_KEY;
import static org.apache.polaris.service.events.PolarisEventMetadata.OPEN_TELEMETRY_TRACE_ID_KEY;
import static org.apache.polaris.service.events.listeners.opentelemetry.OpenTelemetryEventListener.ACTOR_NAME_ATTRIBUTE_NAME;
import static org.apache.polaris.service.events.listeners.opentelemetry.OpenTelemetryEventListener.ACTOR_ROLES_ATTRIBUTE_NAME;
import static org.apache.polaris.service.events.listeners.opentelemetry.OpenTelemetryEventListener.ADD_GRANT_REQUEST_ATTRIBUTE_NAME;
import static org.apache.polaris.service.events.listeners.opentelemetry.OpenTelemetryEventListener.CATALOG_NAME_ATTRIBUTE_NAME;
import static org.apache.polaris.service.events.listeners.opentelemetry.OpenTelemetryEventListener.CATALOG_ROLE_NAME_ATTRIBUTE_NAME;
import static org.apache.polaris.service.events.listeners.opentelemetry.OpenTelemetryEventListener.EVENT_CATEGORY_ATTRIBUTE_NAME;
import static org.apache.polaris.service.events.listeners.opentelemetry.OpenTelemetryEventListener.EVENT_TYPE_ATTRIBUTE_NAME;
import static org.apache.polaris.service.events.listeners.opentelemetry.OpenTelemetryEventListener.GRANT_RESOURCE_ATTRIBUTE_NAME;
import static org.apache.polaris.service.events.listeners.opentelemetry.OpenTelemetryEventListener.GRANT_RESOURCE_TYPE_ATTRIBUTE_NAME;
import static org.apache.polaris.service.events.listeners.opentelemetry.OpenTelemetryEventListener.PRINCIPAL_NAME_ATTRIBUTE_NAME;
import static org.apache.polaris.service.events.listeners.opentelemetry.OpenTelemetryEventListener.PRIVILEGE_ATTRIBUTE_NAME;
import static org.apache.polaris.service.events.listeners.opentelemetry.OpenTelemetryEventListener.PURGE_REQUESTED_ATTRIBUTE_NAME;
import static org.apache.polaris.service.events.listeners.opentelemetry.OpenTelemetryEventListener.REALM_ID_ATTRIBUTE_NAME;
import static org.apache.polaris.service.events.listeners.opentelemetry.OpenTelemetryEventListener.REQUEST_ID_ATTRIBUTE_NAME;
import static org.apache.polaris.service.events.listeners.opentelemetry.OpenTelemetryEventListener.TABLE_IDENTIFIER_ATTRIBUTE_NAME;
import static org.apache.polaris.service.events.listeners.opentelemetry.OpenTelemetryEventListener.TABLE_NAME_ATTRIBUTE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.TableGrant;
import org.apache.polaris.core.admin.model.TablePrivilege;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.service.events.EventAttributeMap;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventMetadata;
import org.apache.polaris.service.events.PolarisEventType;
import org.junit.jupiter.api.Test;

class OpenTelemetryEventListenerTest {
  private static final String TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
  private static final String SPAN_ID = "00f067aa0ba902b7";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  void shouldEmitCreateTableEventAttributes() {
    CapturingLogRecordExporter exporter = new CapturingLogRecordExporter();
    OpenTelemetryEventListener listener = createListener(exporter);

    listener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_CREATE_TABLE,
            metadata(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, "test_catalog")
                .put(EventAttributes.NAMESPACE, Namespace.of("test_namespace"))
                .put(EventAttributes.TABLE_NAME, "test_table")
                .put(EventAttributes.PURGE_REQUESTED, true)));

    LogRecordData record = singleRecord(exporter);

    assertThat(record.getBodyValue().asString()).isEqualTo("AFTER_CREATE_TABLE");
    assertThat(record.getEventName()).isEqualTo("AFTER_CREATE_TABLE");
    assertThat(record.getSeverity()).isEqualTo(Severity.INFO);
    assertThat(record.getSpanContext().getTraceId()).isEqualTo(TRACE_ID);
    assertThat(record.getSpanContext().getSpanId()).isEqualTo(SPAN_ID);
    assertThat(record.getAttributes().get(stringKey(OPEN_TELEMETRY_TRACE_ID_KEY)))
        .isEqualTo(TRACE_ID);
    assertThat(record.getAttributes().get(stringKey(OPEN_TELEMETRY_SPAN_ID_KEY)))
        .isEqualTo(SPAN_ID);
    assertThat(record.getAttributes().get(stringKey(OPEN_TELEMETRY_TRACE_FLAGS_KEY)))
        .isEqualTo("01");
    assertThat(record.getAttributes().get(stringKey(OPEN_TELEMETRY_SAMPLED_KEY))).isEqualTo("true");
    assertThat(record.getAttributes().get(stringKey(EVENT_TYPE_ATTRIBUTE_NAME)))
        .isEqualTo("AFTER_CREATE_TABLE");
    assertThat(record.getAttributes().get(stringKey(EVENT_CATEGORY_ATTRIBUTE_NAME)))
        .isEqualTo("TABLE");
    assertThat(record.getAttributes().get(stringKey(REALM_ID_ATTRIBUTE_NAME)))
        .isEqualTo("test_realm");
    assertThat(record.getAttributes().get(stringKey(REQUEST_ID_ATTRIBUTE_NAME)))
        .isEqualTo("request-1");
    assertThat(record.getAttributes().get(stringKey(ACTOR_NAME_ATTRIBUTE_NAME)))
        .isEqualTo("test_user");
    assertThat(record.getAttributes().get(stringArrayKey(ACTOR_ROLES_ATTRIBUTE_NAME)))
        .containsExactlyInAnyOrder("role1", "role2");
    assertThat(record.getAttributes().get(stringKey(CATALOG_NAME_ATTRIBUTE_NAME)))
        .isEqualTo("test_catalog");
    assertThat(record.getAttributes().get(stringKey(TABLE_NAME_ATTRIBUTE_NAME)))
        .isEqualTo("test_table");
    assertThat(record.getAttributes().get(stringKey(TABLE_IDENTIFIER_ATTRIBUTE_NAME))).isNull();
    assertThat(record.getAttributes().get(booleanKey(PURGE_REQUESTED_ATTRIBUTE_NAME))).isTrue();
  }

  @Test
  void shouldEmitExplicitTableIdentifier() {
    CapturingLogRecordExporter exporter = new CapturingLogRecordExporter();
    OpenTelemetryEventListener listener = createListener(exporter);

    listener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_CREATE_TABLE,
            metadata(),
            new EventAttributeMap()
                .put(EventAttributes.NAMESPACE, Namespace.of("test_namespace"))
                .put(EventAttributes.TABLE_NAME, "test_table")
                .put(
                    EventAttributes.TABLE_IDENTIFIER,
                    TableIdentifier.of(Namespace.of("explicit_namespace"), "explicit_table"))));

    LogRecordData record = singleRecord(exporter);

    assertThat(record.getAttributes().get(stringKey(TABLE_IDENTIFIER_ATTRIBUTE_NAME)))
        .isEqualTo("explicit_namespace.explicit_table");
  }

  @Test
  void shouldEmitPrincipalEventAttributesWithoutOverwritingActor() {
    CapturingLogRecordExporter exporter = new CapturingLogRecordExporter();
    OpenTelemetryEventListener listener = createListener(exporter);

    listener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_GET_PRINCIPAL,
            metadata(),
            new EventAttributeMap().put(EventAttributes.PRINCIPAL_NAME, "target_principal")));

    LogRecordData record = singleRecord(exporter);

    assertThat(record.getAttributes().get(stringKey(ACTOR_NAME_ATTRIBUTE_NAME)))
        .isEqualTo("test_user");
    assertThat(record.getAttributes().get(stringKey(PRINCIPAL_NAME_ATTRIBUTE_NAME)))
        .isEqualTo("target_principal");
  }

  @Test
  void shouldEmitGrantEventAttributes() throws Exception {
    CapturingLogRecordExporter exporter = new CapturingLogRecordExporter();
    OpenTelemetryEventListener listener = createListener(exporter);
    TableGrant grant =
        new TableGrant(
            List.of("test_namespace"),
            "test_table",
            TablePrivilege.TABLE_WRITE_DATA,
            GrantResource.TypeEnum.TABLE);

    listener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_ADD_GRANT_TO_CATALOG_ROLE,
            metadata(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, "test_catalog")
                .put(EventAttributes.CATALOG_ROLE_NAME, "test_catalog_role")
                .put(EventAttributes.PRIVILEGE, PolarisPrivilege.TABLE_WRITE_DATA)
                .put(EventAttributes.GRANT_RESOURCE, grant)));

    LogRecordData record = singleRecord(exporter);

    assertThat(record.getAttributes().get(stringKey(EVENT_TYPE_ATTRIBUTE_NAME)))
        .isEqualTo("AFTER_ADD_GRANT_TO_CATALOG_ROLE");
    assertThat(record.getAttributes().get(stringKey(CATALOG_NAME_ATTRIBUTE_NAME)))
        .isEqualTo("test_catalog");
    assertThat(record.getAttributes().get(stringKey(CATALOG_ROLE_NAME_ATTRIBUTE_NAME)))
        .isEqualTo("test_catalog_role");
    assertThat(record.getAttributes().get(stringKey(PRIVILEGE_ATTRIBUTE_NAME)))
        .isEqualTo("TABLE_WRITE_DATA");
    assertThat(record.getAttributes().get(stringKey(GRANT_RESOURCE_TYPE_ATTRIBUTE_NAME)))
        .isEqualTo("table");
    JsonNode grantJson =
        OBJECT_MAPPER.readTree(
            record.getAttributes().get(stringKey(GRANT_RESOURCE_ATTRIBUTE_NAME)));
    assertTableGrantJson(grantJson);
  }

  @Test
  void shouldEmitBeforeGrantRequestAttributes() throws Exception {
    CapturingLogRecordExporter exporter = new CapturingLogRecordExporter();
    OpenTelemetryEventListener listener = createListener(exporter);
    TableGrant grant =
        new TableGrant(
            List.of("test_namespace"),
            "test_table",
            TablePrivilege.TABLE_WRITE_DATA,
            GrantResource.TypeEnum.TABLE);

    listener.onEvent(
        new PolarisEvent(
            PolarisEventType.BEFORE_ADD_GRANT_TO_CATALOG_ROLE,
            metadata(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, "test_catalog")
                .put(EventAttributes.CATALOG_ROLE_NAME, "test_catalog_role")
                .put(
                    EventAttributes.ADD_GRANT_REQUEST,
                    AddGrantRequest.builder().setGrant(grant).build())));

    LogRecordData record = singleRecord(exporter);

    assertThat(record.getAttributes().get(stringKey(EVENT_TYPE_ATTRIBUTE_NAME)))
        .isEqualTo("BEFORE_ADD_GRANT_TO_CATALOG_ROLE");
    assertThat(record.getAttributes().get(stringKey(CATALOG_ROLE_NAME_ATTRIBUTE_NAME)))
        .isEqualTo("test_catalog_role");
    JsonNode requestJson =
        OBJECT_MAPPER.readTree(
            record.getAttributes().get(stringKey(ADD_GRANT_REQUEST_ATTRIBUTE_NAME)));
    assertTableGrantJson(requestJson.path("grant"));
  }

  @Test
  void shouldEmitEventWithoutSpanContextWhenOpenTelemetryContextIsEmpty() {
    LogRecordData record = emitEventWithOpenTelemetryContext(Map.of());

    assertNoOpenTelemetryContext(record);
  }

  @Test
  void shouldEmitEventWithoutSpanContextWhenTraceFlagsAreShort() {
    LogRecordData record =
        emitEventWithOpenTelemetryContext(
            Map.of(
                OPEN_TELEMETRY_TRACE_ID_KEY,
                TRACE_ID,
                OPEN_TELEMETRY_SPAN_ID_KEY,
                SPAN_ID,
                OPEN_TELEMETRY_TRACE_FLAGS_KEY,
                "0"));

    assertNoOpenTelemetryContext(record);
  }

  @Test
  void shouldEmitEventWithoutSpanContextWhenTraceContextIsMalformed() {
    LogRecordData record =
        emitEventWithOpenTelemetryContext(
            Map.of(
                OPEN_TELEMETRY_TRACE_ID_KEY,
                "not-a-trace-id",
                OPEN_TELEMETRY_SPAN_ID_KEY,
                SPAN_ID,
                OPEN_TELEMETRY_TRACE_FLAGS_KEY,
                "01"));

    assertNoOpenTelemetryContext(record);
  }

  private static OpenTelemetryEventListener createListener(CapturingLogRecordExporter exporter) {
    SdkLoggerProvider loggerProvider =
        SdkLoggerProvider.builder()
            .addLogRecordProcessor(SimpleLogRecordProcessor.create(exporter))
            .build();
    OpenTelemetry openTelemetry =
        OpenTelemetrySdk.builder().setLoggerProvider(loggerProvider).build();
    return new OpenTelemetryEventListener(openTelemetry, OBJECT_MAPPER);
  }

  private static PolarisEventMetadata metadata() {
    return metadata(
        Map.of(
            OPEN_TELEMETRY_TRACE_ID_KEY,
            TRACE_ID,
            OPEN_TELEMETRY_SPAN_ID_KEY,
            SPAN_ID,
            OPEN_TELEMETRY_TRACE_FLAGS_KEY,
            "01",
            OPEN_TELEMETRY_SAMPLED_KEY,
            "true"));
  }

  private static PolarisEventMetadata metadata(Map<String, String> openTelemetryContext) {
    return PolarisEventMetadata.builder()
        .timestamp(Instant.parse("2026-06-19T00:00:00Z"))
        .realmId("test_realm")
        .requestId("request-1")
        .user(PolarisPrincipal.of("test_user", Map.of(), Set.of("role1", "role2")))
        .openTelemetryContext(openTelemetryContext)
        .build();
  }

  private static LogRecordData emitEventWithOpenTelemetryContext(
      Map<String, String> openTelemetryContext) {
    CapturingLogRecordExporter exporter = new CapturingLogRecordExporter();
    OpenTelemetryEventListener listener = createListener(exporter);

    listener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_GET_PRINCIPAL,
            metadata(openTelemetryContext),
            new EventAttributeMap().put(EventAttributes.PRINCIPAL_NAME, "target_principal")));

    return singleRecord(exporter);
  }

  private static LogRecordData singleRecord(CapturingLogRecordExporter exporter) {
    assertThat(exporter.records()).hasSize(1);
    return exporter.records().getFirst();
  }

  private static void assertNoOpenTelemetryContext(LogRecordData record) {
    assertThat(record.getSpanContext().isValid()).isFalse();
    assertThat(record.getAttributes().get(stringKey(OPEN_TELEMETRY_TRACE_ID_KEY))).isNull();
    assertThat(record.getAttributes().get(stringKey(OPEN_TELEMETRY_SPAN_ID_KEY))).isNull();
    assertThat(record.getAttributes().get(stringKey(OPEN_TELEMETRY_TRACE_FLAGS_KEY))).isNull();
    assertThat(record.getAttributes().get(stringKey(OPEN_TELEMETRY_SAMPLED_KEY))).isNull();
  }

  private static void assertTableGrantJson(JsonNode grantJson) {
    assertThat(grantJson.path("namespace").get(0).asText()).isEqualTo("test_namespace");
    assertThat(grantJson.path("tableName").asText()).isEqualTo("test_table");
    assertThat(grantJson.path("privilege").asText()).isEqualTo("TABLE_WRITE_DATA");
    assertThat(grantJson.path("type").asText()).isEqualTo("table");
  }

  private static class CapturingLogRecordExporter implements LogRecordExporter {
    private final List<LogRecordData> records = new ArrayList<>();

    @Override
    public CompletableResultCode export(Collection<LogRecordData> records) {
      this.records.addAll(records);
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
      return CompletableResultCode.ofSuccess();
    }

    List<LogRecordData> records() {
      return records;
    }
  }
}
