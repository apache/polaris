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

package org.apache.polaris.service;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.test.commons.NoSqlInMemoryProfile;

public final class Profiles {

  private Profiles() {}

  public static final Map<String, String> DEFAULT_PROFILE_CONFIG_OVERRIDES =
      Map.of(
          "polaris.features.\"ALLOW_SPECIFYING_FILE_IO_IMPL\"",
          "true",
          "polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"",
          "true",
          "polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"",
          "[\"FILE\",\"S3\"]",
          "polaris.event-listener.type",
          "test",
          "polaris.readiness.ignore-severe-issues",
          "true");

  public static class DefaultProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return DEFAULT_PROFILE_CONFIG_OVERRIDES;
    }
  }

  public static class DefaultNoSqlProfile extends DefaultProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .putAll(NoSqlInMemoryProfile.NOSQL_PERSISTENCE)
          .build();
    }
  }

  public static class DefaultIcebergCatalogProfile extends DefaultProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("polaris.features.\"ALLOW_TABLE_LOCATION_OVERLAP\"", "true")
          .put("polaris.features.\"LIST_PAGINATION_ENABLED\"", "true")
          .put("polaris.features.\"ALLOW_WILDCARD_LOCATION\"", "true")
          .put("polaris.features.\"SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION\"", "true")
          .put("polaris.behavior-changes.\"ALLOW_NAMESPACE_CUSTOM_LOCATION\"", "true")
          .put("polaris.test.rootAugmentor.enabled", "true")
          .build();
    }
  }

  public static class NoSqlIcebergCatalogProfile extends DefaultIcebergCatalogProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .putAll(NoSqlInMemoryProfile.NOSQL_PERSISTENCE)
          .build();
    }
  }

  public static class PolarisAuthzBaseProfile extends DefaultProfile {
    @Override
    public Set<Class<?>> getEnabledAlternatives() {
      return Set.of(PolarisAuthzTestBase.TestPolarisLocalCatalogFactory.class);
    }

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("polaris.features.\"ALLOW_EXTERNAL_METADATA_FILE_LOCATION\"", "true")
          .put("polaris.features.\"ENABLE_GENERIC_TABLES\"", "true")
          .put(
              "polaris.features.\"ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING\"",
              "true")
          .put("polaris.features.\"DROP_WITH_PURGE_ENABLED\"", "true")
          .put("polaris.behavior-changes.\"ALLOW_NAMESPACE_CUSTOM_LOCATION\"", "true")
          .put("polaris.features.\"ENABLE_CATALOG_FEDERATION\"", "true")
          .build();
    }
  }

  public static class IcebergCatalogHandlerNoSqlAuthzProfile extends PolarisAuthzBaseProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .putAll(NoSqlInMemoryProfile.NOSQL_PERSISTENCE)
          .build();
    }
  }

  static final Map<String, String> IN_MEMORY_BUFFER_EVENT_LISTENER_BASE_CONFIG =
      ImmutableMap.<String, String>builder()
          .put("polaris.realm-context.realms", "test1,test2")
          .put("polaris.persistence.type", "relational-jdbc")
          .put("polaris.persistence.auto-bootstrap-types", "relational-jdbc")
          .put("quarkus.datasource.db-kind", "h2")
          .put(
              "quarkus.datasource.jdbc.url",
              "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE")
          .put("polaris.event-listener.type", "persistence-in-memory-buffer")
          .put(
              "quarkus.fault-tolerance.\"org.apache.polaris.service.events.listeners.inmemory.InMemoryBufferEventListener/flush\".retry.max-retries",
              "1")
          .put(
              "quarkus.fault-tolerance.\"org.apache.polaris.service.events.listeners.inmemory.InMemoryBufferEventListener/flush\".retry.delay",
              "10")
          .build();

  public static class InMemoryBufferEventListenerBufferSizeProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(IN_MEMORY_BUFFER_EVENT_LISTENER_BASE_CONFIG)
          .put("polaris.event-listener.persistence-in-memory-buffer.buffer-time", "60s")
          .put("polaris.event-listener.persistence-in-memory-buffer.max-buffer-size", "10")
          .build();
    }
  }

  public static class InMemoryBufferEventListenerBufferTimeProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(IN_MEMORY_BUFFER_EVENT_LISTENER_BASE_CONFIG)
          .put("polaris.event-listener.persistence-in-memory-buffer.buffer-time", "100ms")
          .put("polaris.event-listener.persistence-in-memory-buffer.max-buffer-size", "1000")
          .build();
    }
  }

  public static class RealmIdTagEnabledMetricsProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.metrics.tags.environment",
          "prod",
          "polaris.realm-context.type",
          "test",
          "polaris.metrics.realm-id-tag.enable-in-api-metrics",
          "true",
          "polaris.metrics.realm-id-tag.enable-in-http-metrics",
          "true",
          "polaris.metrics.user-principal-tag.enable-in-api-metrics",
          "false");
    }
  }

  public static class UserPrincipalTagEnabledMetricsProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.metrics.tags.environment",
          "prod",
          "polaris.metrics.user-principal-tag.enable-in-api-metrics",
          "true",
          "polaris.metrics.realm-id-tag.enable-in-api-metrics",
          "false",
          "polaris.metrics.realm-id-tag.enable-in-http-metrics",
          "false");
    }
  }

  public static class TagsDisabledMetricsProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.metrics.tags.environment", "prod", "polaris.realm-context.type", "test");
    }
  }

  public static class PolarisEventListenersTestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put(
              "polaris.event-listener.types",
              "after-send-listener,before-send-listener,consume-all-listener,consume-all-listener-2,consume-only-catalog-listener,consume-catalog-and-after-notification-listener")
          .put(
              "polaris.event-listener.after-send-listener.enabled-event-types",
              "AFTER_SEND_NOTIFICATION")
          .put(
              "polaris.event-listener.before-send-listener.enabled-event-types",
              "BEFORE_SEND_NOTIFICATION")
          .put(
              "polaris.event-listener.consume-only-catalog-listener.enabled-event-categories",
              "CATALOG")
          .put(
              "polaris.event-listener.consume-catalog-and-after-notification-listener.enabled-event-categories",
              "CATALOG")
          .put(
              "polaris.event-listener.consume-catalog-and-after-notification-listener.enabled-event-types",
              "AFTER_SEND_NOTIFICATION")
          .build();
    }
  }

  public static class InMemoryBufferEventListenerIntegrationProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("polaris.persistence.type", "relational-jdbc")
          .put("polaris.persistence.auto-bootstrap-types", "relational-jdbc")
          .put("quarkus.datasource.db-kind", "h2")
          .put("quarkus.otel.sdk.disabled", "false")
          .put(
              "quarkus.datasource.jdbc.url",
              "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE")
          .put("polaris.event-listener.type", "persistence-in-memory-buffer")
          .put("polaris.event-listener.persistence-in-memory-buffer.buffer-time", "100ms")
          .put("polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"", "true")
          .put("polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"", "[\"FILE\",\"S3\"]")
          .put("polaris.readiness.ignore-severe-issues", "true")
          .build();
    }
  }
}
