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
import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringArrayKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.apache.polaris.service.events.PolarisEventMetadata.OPEN_TELEMETRY_SAMPLED_KEY;
import static org.apache.polaris.service.events.PolarisEventMetadata.OPEN_TELEMETRY_SPAN_ID_KEY;
import static org.apache.polaris.service.events.PolarisEventMetadata.OPEN_TELEMETRY_TRACE_FLAGS_KEY;
import static org.apache.polaris.service.events.PolarisEventMetadata.OPEN_TELEMETRY_TRACE_ID_KEY;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.iceberg.catalog.Namespace;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.service.events.AttributeKey;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventMetadata;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.slf4j.LoggerFactory;

@ApplicationScoped
@Identifier("opentelemetry")
public class OpenTelemetryEventListener implements PolarisEventListener {
  static final String INSTRUMENTATION_SCOPE_NAME = "org.apache.polaris.events";

  static final String EVENT_TYPE_ATTRIBUTE_NAME = "polaris.event.type";
  static final String EVENT_CATEGORY_ATTRIBUTE_NAME = "polaris.event.category";
  static final String REALM_ID_ATTRIBUTE_NAME = "polaris.realm.id";
  static final String REQUEST_ID_ATTRIBUTE_NAME = "polaris.request.id";
  static final String ACTOR_NAME_ATTRIBUTE_NAME = "polaris.actor.name";
  static final String ACTOR_ROLES_ATTRIBUTE_NAME = "polaris.actor.roles";
  static final String PRINCIPAL_NAME_ATTRIBUTE_NAME = "polaris.principal.name";

  static final String CATALOG_NAME_ATTRIBUTE_NAME = "polaris.catalog.name";
  static final String NAMESPACE_ATTRIBUTE_NAME = "polaris.namespace";
  static final String NAMESPACE_FQN_ATTRIBUTE_NAME = "polaris.namespace.fqn";
  static final String PARENT_NAMESPACE_FQN_ATTRIBUTE_NAME = "polaris.parent_namespace.fqn";
  static final String TABLE_NAME_ATTRIBUTE_NAME = "polaris.table.name";
  static final String TABLE_IDENTIFIER_ATTRIBUTE_NAME = "polaris.table.identifier";
  static final String VIEW_NAME_ATTRIBUTE_NAME = "polaris.view.name";
  static final String VIEW_IDENTIFIER_ATTRIBUTE_NAME = "polaris.view.identifier";
  static final String PRINCIPAL_ROLE_NAME_ATTRIBUTE_NAME = "polaris.principal_role.name";
  static final String CATALOG_ROLE_NAME_ATTRIBUTE_NAME = "polaris.catalog_role.name";
  static final String PRIVILEGE_ATTRIBUTE_NAME = "polaris.privilege";
  static final String GRANT_RESOURCE_ATTRIBUTE_NAME = "polaris.grant.resource";
  static final String GRANT_RESOURCE_TYPE_ATTRIBUTE_NAME = "polaris.grant.resource.type";
  static final String ADD_GRANT_REQUEST_ATTRIBUTE_NAME = "polaris.grant.add_request";
  static final String REVOKE_GRANT_REQUEST_ATTRIBUTE_NAME = "polaris.grant.revoke_request";
  static final String CASCADE_ATTRIBUTE_NAME = "polaris.cascade";
  static final String WAREHOUSE_ATTRIBUTE_NAME = "polaris.warehouse";
  static final String ACCESS_DELEGATION_MODE_ATTRIBUTE_NAME = "polaris.access_delegation_mode";
  static final String IF_NONE_MATCH_ATTRIBUTE_NAME = "polaris.if_none_match";
  static final String SNAPSHOTS_ATTRIBUTE_NAME = "polaris.snapshots";
  static final String PURGE_REQUESTED_ATTRIBUTE_NAME = "polaris.purge_requested";
  static final String TASK_ENTITY_ID_ATTRIBUTE_NAME = "polaris.task.entity_id";
  static final String TASK_ATTEMPT_ATTRIBUTE_NAME = "polaris.task.attempt";
  static final String TASK_SUCCESS_ATTRIBUTE_NAME = "polaris.task.success";
  static final String HTTP_METHOD_ATTRIBUTE_NAME = "polaris.http.method";
  static final String REQUEST_URI_ATTRIBUTE_NAME = "polaris.http.request_uri";
  static final String NAMESPACE_NAME_ATTRIBUTE_NAME = "polaris.namespace.name";
  static final String GENERIC_TABLE_NAME_ATTRIBUTE_NAME = "polaris.generic_table.name";
  static final String POLICY_NAME_ATTRIBUTE_NAME = "polaris.policy.name";
  static final String POLICY_TYPE_ATTRIBUTE_NAME = "polaris.policy.type";
  static final String TARGET_NAME_ATTRIBUTE_NAME = "polaris.target.name";
  static final String DETACH_ALL_ATTRIBUTE_NAME = "polaris.detach_all";

  private static final io.opentelemetry.api.common.AttributeKey<String> EVENT_TYPE_ATTRIBUTE_KEY =
      stringKey(EVENT_TYPE_ATTRIBUTE_NAME);
  private static final io.opentelemetry.api.common.AttributeKey<String>
      EVENT_CATEGORY_ATTRIBUTE_KEY = stringKey(EVENT_CATEGORY_ATTRIBUTE_NAME);
  private static final io.opentelemetry.api.common.AttributeKey<String> REALM_ID_ATTRIBUTE_KEY =
      stringKey(REALM_ID_ATTRIBUTE_NAME);
  private static final io.opentelemetry.api.common.AttributeKey<String> REQUEST_ID_ATTRIBUTE_KEY =
      stringKey(REQUEST_ID_ATTRIBUTE_NAME);
  private static final io.opentelemetry.api.common.AttributeKey<String> ACTOR_NAME_ATTRIBUTE_KEY =
      stringKey(ACTOR_NAME_ATTRIBUTE_NAME);
  private static final io.opentelemetry.api.common.AttributeKey<List<String>>
      ACTOR_ROLES_ATTRIBUTE_KEY = stringArrayKey(ACTOR_ROLES_ATTRIBUTE_NAME);
  private static final io.opentelemetry.api.common.AttributeKey<String>
      GRANT_RESOURCE_ATTRIBUTE_KEY = stringKey(GRANT_RESOURCE_ATTRIBUTE_NAME);
  private static final io.opentelemetry.api.common.AttributeKey<String>
      GRANT_RESOURCE_TYPE_ATTRIBUTE_KEY = stringKey(GRANT_RESOURCE_TYPE_ATTRIBUTE_NAME);
  private static final io.opentelemetry.api.common.AttributeKey<String>
      OPEN_TELEMETRY_TRACE_ID_ATTRIBUTE_KEY = stringKey(OPEN_TELEMETRY_TRACE_ID_KEY);
  private static final io.opentelemetry.api.common.AttributeKey<String>
      OPEN_TELEMETRY_SPAN_ID_ATTRIBUTE_KEY = stringKey(OPEN_TELEMETRY_SPAN_ID_KEY);
  private static final io.opentelemetry.api.common.AttributeKey<String>
      OPEN_TELEMETRY_TRACE_FLAGS_ATTRIBUTE_KEY = stringKey(OPEN_TELEMETRY_TRACE_FLAGS_KEY);
  private static final io.opentelemetry.api.common.AttributeKey<String>
      OPEN_TELEMETRY_SAMPLED_ATTRIBUTE_KEY = stringKey(OPEN_TELEMETRY_SAMPLED_KEY);

  private static final org.slf4j.Logger LOGGER =
      LoggerFactory.getLogger(OpenTelemetryEventListener.class);

  private final Logger openTelemetryLogger;
  private final ObjectMapper objectMapper;

  @Inject
  public OpenTelemetryEventListener(OpenTelemetry openTelemetry, ObjectMapper objectMapper) {
    this.openTelemetryLogger =
        openTelemetry.getLogsBridge().loggerBuilder(INSTRUMENTATION_SCOPE_NAME).build();
    this.objectMapper = objectMapper;
  }

  @Override
  public void onEvent(PolarisEvent event) {
    Optional<Context> openTelemetryContext = toOpenTelemetryContext(event);
    var logRecordBuilder =
        openTelemetryLogger
            .logRecordBuilder()
            .setTimestamp(event.metadata().timestamp())
            .setSeverity(Severity.INFO)
            .setSeverityText("INFO")
            .setEventName(event.type().name())
            .setBody(event.type().name())
            .setAllAttributes(toLogAttributes(event, openTelemetryContext.isPresent()));
    openTelemetryContext.ifPresent(logRecordBuilder::setContext);
    logRecordBuilder.emit();
  }

  private Attributes toLogAttributes(PolarisEvent event, boolean includeOpenTelemetryContext) {
    AttributesBuilder attributes = Attributes.builder();
    attributes.put(EVENT_TYPE_ATTRIBUTE_KEY, event.type().name());
    attributes.put(EVENT_CATEGORY_ATTRIBUTE_KEY, event.type().category().name());

    PolarisEventMetadata metadata = event.metadata();
    attributes.put(REALM_ID_ATTRIBUTE_KEY, metadata.realmId());
    metadata
        .requestId()
        .ifPresent(requestId -> attributes.put(REQUEST_ID_ATTRIBUTE_KEY, requestId));
    metadata
        .user()
        .ifPresent(
            principal -> {
              attributes.put(ACTOR_NAME_ATTRIBUTE_KEY, principal.getName());
              attributes.put(ACTOR_ROLES_ATTRIBUTE_KEY, List.copyOf(principal.getRoles()));
            });
    if (includeOpenTelemetryContext) {
      putOpenTelemetryContextAttributes(attributes, metadata.openTelemetryContext());
    }

    for (PolarisOtelAttribute attribute : PolarisOtelAttribute.VALUES) {
      attribute.forward(this, attributes, event);
    }

    return attributes.build();
  }

  private static void putOpenTelemetryContextAttributes(
      AttributesBuilder attributes, Map<String, String> contextValues) {
    putIfPresent(
        attributes,
        OPEN_TELEMETRY_TRACE_ID_ATTRIBUTE_KEY,
        contextValues.get(OPEN_TELEMETRY_TRACE_ID_KEY));
    putIfPresent(
        attributes,
        OPEN_TELEMETRY_SPAN_ID_ATTRIBUTE_KEY,
        contextValues.get(OPEN_TELEMETRY_SPAN_ID_KEY));
    putIfPresent(
        attributes,
        OPEN_TELEMETRY_TRACE_FLAGS_ATTRIBUTE_KEY,
        contextValues.get(OPEN_TELEMETRY_TRACE_FLAGS_KEY));
    putIfPresent(
        attributes,
        OPEN_TELEMETRY_SAMPLED_ATTRIBUTE_KEY,
        contextValues.get(OPEN_TELEMETRY_SAMPLED_KEY));
  }

  private static void putIfPresent(
      AttributesBuilder attributes,
      io.opentelemetry.api.common.AttributeKey<String> attributeKey,
      String value) {
    if (value != null) {
      attributes.put(attributeKey, value);
    }
  }

  private enum PolarisOtelAttribute {
    CATALOG_NAME(string(CATALOG_NAME_ATTRIBUTE_NAME, EventAttributes.CATALOG_NAME)),
    NAMESPACE(string(NAMESPACE_ATTRIBUTE_NAME, EventAttributes.NAMESPACE, Namespace::toString)),
    NAMESPACE_FQN(string(NAMESPACE_FQN_ATTRIBUTE_NAME, EventAttributes.NAMESPACE_FQN)),
    PARENT_NAMESPACE_FQN(
        string(PARENT_NAMESPACE_FQN_ATTRIBUTE_NAME, EventAttributes.PARENT_NAMESPACE_FQN)),
    TABLE_NAME(string(TABLE_NAME_ATTRIBUTE_NAME, EventAttributes.TABLE_NAME)),
    TABLE_IDENTIFIER(string(TABLE_IDENTIFIER_ATTRIBUTE_NAME, EventAttributes.TABLE_IDENTIFIER)),
    VIEW_NAME(string(VIEW_NAME_ATTRIBUTE_NAME, EventAttributes.VIEW_NAME)),
    VIEW_IDENTIFIER(string(VIEW_IDENTIFIER_ATTRIBUTE_NAME, EventAttributes.VIEW_IDENTIFIER)),
    PRINCIPAL_NAME(string(PRINCIPAL_NAME_ATTRIBUTE_NAME, EventAttributes.PRINCIPAL_NAME)),
    PRINCIPAL_ROLE_NAME(
        string(PRINCIPAL_ROLE_NAME_ATTRIBUTE_NAME, EventAttributes.PRINCIPAL_ROLE_NAME)),
    CATALOG_ROLE_NAME(string(CATALOG_ROLE_NAME_ATTRIBUTE_NAME, EventAttributes.CATALOG_ROLE_NAME)),
    PRIVILEGE(string(PRIVILEGE_ATTRIBUTE_NAME, EventAttributes.PRIVILEGE, value -> value.name())),
    CASCADE(booleanAttribute(CASCADE_ATTRIBUTE_NAME, EventAttributes.CASCADE)),
    WAREHOUSE(string(WAREHOUSE_ATTRIBUTE_NAME, EventAttributes.WAREHOUSE)),
    ACCESS_DELEGATION_MODE(
        string(ACCESS_DELEGATION_MODE_ATTRIBUTE_NAME, EventAttributes.ACCESS_DELEGATION_MODE)),
    IF_NONE_MATCH(string(IF_NONE_MATCH_ATTRIBUTE_NAME, EventAttributes.IF_NONE_MATCH_STRING)),
    SNAPSHOTS(string(SNAPSHOTS_ATTRIBUTE_NAME, EventAttributes.SNAPSHOTS)),
    PURGE_REQUESTED(
        booleanAttribute(PURGE_REQUESTED_ATTRIBUTE_NAME, EventAttributes.PURGE_REQUESTED)),
    TASK_ENTITY_ID(longAttribute(TASK_ENTITY_ID_ATTRIBUTE_NAME, EventAttributes.TASK_ENTITY_ID)),
    TASK_ATTEMPT(longAttribute(TASK_ATTEMPT_ATTRIBUTE_NAME, EventAttributes.TASK_ATTEMPT)),
    TASK_SUCCESS(booleanAttribute(TASK_SUCCESS_ATTRIBUTE_NAME, EventAttributes.TASK_SUCCESS)),
    HTTP_METHOD(string(HTTP_METHOD_ATTRIBUTE_NAME, EventAttributes.HTTP_METHOD)),
    REQUEST_URI(string(REQUEST_URI_ATTRIBUTE_NAME, EventAttributes.REQUEST_URI)),
    NAMESPACE_NAME(string(NAMESPACE_NAME_ATTRIBUTE_NAME, EventAttributes.NAMESPACE_NAME)),
    GENERIC_TABLE_NAME(
        string(GENERIC_TABLE_NAME_ATTRIBUTE_NAME, EventAttributes.GENERIC_TABLE_NAME)),
    POLICY_NAME(string(POLICY_NAME_ATTRIBUTE_NAME, EventAttributes.POLICY_NAME)),
    POLICY_TYPE(string(POLICY_TYPE_ATTRIBUTE_NAME, EventAttributes.POLICY_TYPE)),
    TARGET_NAME(string(TARGET_NAME_ATTRIBUTE_NAME, EventAttributes.TARGET_NAME)),
    DETACH_ALL(booleanAttribute(DETACH_ALL_ATTRIBUTE_NAME, EventAttributes.DETACH_ALL)),
    GRANT_RESOURCE(OpenTelemetryEventListener::putGrantResource),
    ADD_GRANT_REQUEST(json(ADD_GRANT_REQUEST_ATTRIBUTE_NAME, EventAttributes.ADD_GRANT_REQUEST)),
    REVOKE_GRANT_REQUEST(
        json(REVOKE_GRANT_REQUEST_ATTRIBUTE_NAME, EventAttributes.REVOKE_GRANT_REQUEST));

    private static final PolarisOtelAttribute[] VALUES = values();

    private final AttributeForwarder forwarder;

    PolarisOtelAttribute(AttributeForwarder forwarder) {
      this.forwarder = forwarder;
    }

    void forward(
        OpenTelemetryEventListener listener, AttributesBuilder attributes, PolarisEvent event) {
      forwarder.forward(listener, attributes, event);
    }

    private static <T> AttributeForwarder string(
        String logAttributeName, AttributeKey<T> eventAttributeKey) {
      return string(logAttributeName, eventAttributeKey, Object::toString);
    }

    private static <T> AttributeForwarder string(
        String logAttributeName, AttributeKey<T> eventAttributeKey, Function<T, String> mapper) {
      io.opentelemetry.api.common.AttributeKey<String> logAttributeKey =
          stringKey(logAttributeName);
      return (listener, attributes, event) ->
          listener.putStringAttribute(
              attributes, event, logAttributeKey, eventAttributeKey, mapper);
    }

    private static AttributeForwarder booleanAttribute(
        String logAttributeName, AttributeKey<Boolean> eventAttributeKey) {
      io.opentelemetry.api.common.AttributeKey<Boolean> logAttributeKey =
          booleanKey(logAttributeName);
      return (listener, attributes, event) ->
          listener.putBooleanAttribute(attributes, event, logAttributeKey, eventAttributeKey);
    }

    private static <T extends Number> AttributeForwarder longAttribute(
        String logAttributeName, AttributeKey<T> eventAttributeKey) {
      io.opentelemetry.api.common.AttributeKey<Long> logAttributeKey = longKey(logAttributeName);
      return (listener, attributes, event) ->
          listener.putLongAttribute(attributes, event, logAttributeKey, eventAttributeKey);
    }

    private static <T> AttributeForwarder json(
        String logAttributeName, AttributeKey<T> eventAttributeKey) {
      io.opentelemetry.api.common.AttributeKey<String> logAttributeKey =
          stringKey(logAttributeName);
      return (listener, attributes, event) ->
          listener.putJsonAttribute(attributes, event, logAttributeKey, eventAttributeKey);
    }
  }

  @FunctionalInterface
  private interface AttributeForwarder {
    void forward(
        OpenTelemetryEventListener listener, AttributesBuilder attributes, PolarisEvent event);
  }

  private <T> void putStringAttribute(
      AttributesBuilder attributes,
      PolarisEvent event,
      io.opentelemetry.api.common.AttributeKey<String> logAttributeKey,
      AttributeKey<T> eventAttributeKey,
      Function<T, String> mapper) {
    event
        .attributes()
        .get(eventAttributeKey)
        .map(mapper)
        .ifPresent(value -> attributes.put(logAttributeKey, value));
  }

  private void putBooleanAttribute(
      AttributesBuilder attributes,
      PolarisEvent event,
      io.opentelemetry.api.common.AttributeKey<Boolean> logAttributeKey,
      AttributeKey<Boolean> eventAttributeKey) {
    event
        .attributes()
        .get(eventAttributeKey)
        .ifPresent(value -> attributes.put(logAttributeKey, value));
  }

  private <T extends Number> void putLongAttribute(
      AttributesBuilder attributes,
      PolarisEvent event,
      io.opentelemetry.api.common.AttributeKey<Long> logAttributeKey,
      AttributeKey<T> eventAttributeKey) {
    event
        .attributes()
        .get(eventAttributeKey)
        .ifPresent(value -> attributes.put(logAttributeKey, value.longValue()));
  }

  private <T> void putJsonAttribute(
      AttributesBuilder attributes,
      PolarisEvent event,
      io.opentelemetry.api.common.AttributeKey<String> logAttributeKey,
      AttributeKey<T> eventAttributeKey) {
    event
        .attributes()
        .get(eventAttributeKey)
        .flatMap(this::toJsonString)
        .ifPresent(value -> attributes.put(logAttributeKey, value));
  }

  private void putGrantResource(AttributesBuilder attributes, PolarisEvent event) {
    Optional<GrantResource> grantResource = event.attributes().get(EventAttributes.GRANT_RESOURCE);
    grantResource
        .flatMap(this::toJsonString)
        .ifPresent(value -> attributes.put(GRANT_RESOURCE_ATTRIBUTE_KEY, value));
    grantResource
        .map(GrantResource::getType)
        .map(Object::toString)
        .ifPresent(value -> attributes.put(GRANT_RESOURCE_TYPE_ATTRIBUTE_KEY, value));
  }

  private Optional<String> toJsonString(Object value) {
    try {
      return Optional.of(objectMapper.writeValueAsString(value));
    } catch (JsonProcessingException e) {
      LOGGER.warn("Could not serialize Polaris event attribute {}", value, e);
      return Optional.empty();
    }
  }

  private Optional<Context> toOpenTelemetryContext(PolarisEvent event) {
    Map<String, String> contextValues = event.metadata().openTelemetryContext();
    String traceId = contextValues.get(OPEN_TELEMETRY_TRACE_ID_KEY);
    String spanId = contextValues.get(OPEN_TELEMETRY_SPAN_ID_KEY);
    if (traceId == null || spanId == null) {
      return Optional.empty();
    }

    try {
      TraceFlags traceFlags =
          Optional.ofNullable(contextValues.get(OPEN_TELEMETRY_TRACE_FLAGS_KEY))
              .map(OpenTelemetryEventListener::traceFlagsFromHex)
              .orElseGet(TraceFlags::getDefault);
      SpanContext spanContext =
          SpanContext.create(traceId, spanId, traceFlags, TraceState.getDefault());
      if (spanContext.isValid()) {
        return Optional.of(Span.wrap(spanContext).storeInContext(Context.root()));
      }
      LOGGER.warn(
          "Could not attach invalid OpenTelemetry context to Polaris event {}", event.type());
    } catch (IllegalArgumentException e) {
      LOGGER.warn("Could not attach OpenTelemetry context to Polaris event {}", event.type(), e);
    }
    return Optional.empty();
  }

  private static TraceFlags traceFlagsFromHex(String flags) {
    if (flags.length() != 2) {
      throw new IllegalArgumentException("OpenTelemetry trace_flags must be 2 hex characters");
    }
    return TraceFlags.fromHex(flags, 0);
  }
}
