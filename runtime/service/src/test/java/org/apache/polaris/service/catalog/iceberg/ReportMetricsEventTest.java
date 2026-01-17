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
package org.apache.polaris.service.catalog.iceberg;

import static org.apache.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.Response;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.ImmutableScanReport;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.listeners.TestPolarisEventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for verifying that reportMetrics() emits BEFORE_REPORT_METRICS and
 * AFTER_REPORT_METRICS events.
 */
public class ReportMetricsEventTest {
  private static final String NAMESPACE = "test_ns";
  private static final String CATALOG = "test-catalog";
  private static final String TABLE = "test-table";

  private String catalogLocation;

  @BeforeEach
  public void setUp(@TempDir Path tempDir) {
    catalogLocation = tempDir.toAbsolutePath().toUri().toString();
    if (catalogLocation.endsWith("/")) {
      catalogLocation = catalogLocation.substring(0, catalogLocation.length() - 1);
    }
  }

  @Test
  void testReportMetricsEmitsBeforeAndAfterEventsWhenEnabled() {
    // Create test services with ENABLE_METRICS_EVENT_EMISSION enabled
    TestServices testServices = createTestServicesWithMetricsEmissionEnabled(true);
    createCatalogAndNamespace(testServices);
    createTable(testServices, TABLE);

    // Create a ScanReport for testing
    ImmutableScanReport scanReport =
        ImmutableScanReport.builder()
            .schemaId(0)
            .tableName(NAMESPACE + "." + TABLE)
            .snapshotId(100L)
            .addProjectedFieldIds(1)
            .addProjectedFieldNames("id")
            .filter(Expressions.alwaysTrue())
            .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()))
            .build();

    ReportMetricsRequest request = ReportMetricsRequest.of(scanReport);

    // Call reportMetrics
    try (Response response =
        testServices
            .restApi()
            .reportMetrics(
                CATALOG,
                NAMESPACE,
                TABLE,
                request,
                testServices.realmContext(),
                testServices.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    // Verify that BEFORE_REPORT_METRICS and AFTER_REPORT_METRICS events were emitted
    TestPolarisEventListener testEventListener =
        (TestPolarisEventListener) testServices.polarisEventListener();

    PolarisEvent beforeEvent = testEventListener.getLatest(PolarisEventType.BEFORE_REPORT_METRICS);
    assertThat(beforeEvent).isNotNull();
    assertThat(beforeEvent.attributes().getRequired(EventAttributes.CATALOG_NAME))
        .isEqualTo(CATALOG);
    assertThat(beforeEvent.attributes().getRequired(EventAttributes.NAMESPACE))
        .isEqualTo(Namespace.of(NAMESPACE));
    assertThat(beforeEvent.attributes().getRequired(EventAttributes.TABLE_NAME)).isEqualTo(TABLE);
    assertThat(beforeEvent.attributes().getRequired(EventAttributes.REPORT_METRICS_REQUEST))
        .isNotNull();

    PolarisEvent afterEvent = testEventListener.getLatest(PolarisEventType.AFTER_REPORT_METRICS);
    assertThat(afterEvent).isNotNull();
    assertThat(afterEvent.attributes().getRequired(EventAttributes.CATALOG_NAME))
        .isEqualTo(CATALOG);
    assertThat(afterEvent.attributes().getRequired(EventAttributes.NAMESPACE))
        .isEqualTo(Namespace.of(NAMESPACE));
    assertThat(afterEvent.attributes().getRequired(EventAttributes.TABLE_NAME)).isEqualTo(TABLE);
    assertThat(afterEvent.attributes().getRequired(EventAttributes.REPORT_METRICS_REQUEST))
        .isNotNull();
  }

  @Test
  void testReportMetricsDoesNotEmitEventsWhenDisabled() {
    // Create test services with ENABLE_METRICS_EVENT_EMISSION disabled (default)
    TestServices testServices = createTestServicesWithMetricsEmissionEnabled(false);
    createCatalogAndNamespace(testServices);
    createTable(testServices, TABLE);

    // Create a ScanReport for testing
    ImmutableScanReport scanReport =
        ImmutableScanReport.builder()
            .schemaId(0)
            .tableName(NAMESPACE + "." + TABLE)
            .snapshotId(100L)
            .addProjectedFieldIds(1)
            .addProjectedFieldNames("id")
            .filter(Expressions.alwaysTrue())
            .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()))
            .build();

    ReportMetricsRequest request = ReportMetricsRequest.of(scanReport);

    // Call reportMetrics
    try (Response response =
        testServices
            .restApi()
            .reportMetrics(
                CATALOG,
                NAMESPACE,
                TABLE,
                request,
                testServices.realmContext(),
                testServices.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    // Verify that BEFORE_REPORT_METRICS and AFTER_REPORT_METRICS events were NOT emitted
    TestPolarisEventListener testEventListener =
        (TestPolarisEventListener) testServices.polarisEventListener();

    assertThat(testEventListener.hasEvent(PolarisEventType.BEFORE_REPORT_METRICS)).isFalse();
    assertThat(testEventListener.hasEvent(PolarisEventType.AFTER_REPORT_METRICS)).isFalse();
  }

  private TestServices createTestServicesWithMetricsEmissionEnabled(boolean enabled) {
    Map<String, Object> config =
        Map.of(
            "ALLOW_INSECURE_STORAGE_TYPES",
            "true",
            "SUPPORTED_CATALOG_STORAGE_TYPES",
            List.of("FILE"),
            "ENABLE_METRICS_EVENT_EMISSION",
            String.valueOf(enabled));
    return TestServices.builder().config(config).withEventDelegator(true).build();
  }

  private void createCatalogAndNamespace(TestServices services) {
    CatalogProperties.Builder propertiesBuilder =
        CatalogProperties.builder()
            .setDefaultBaseLocation(String.format("%s/%s", catalogLocation, CATALOG));

    StorageConfigInfo config =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .build();
    Catalog catalogObject =
        new Catalog(
            Catalog.TypeEnum.INTERNAL, CATALOG, propertiesBuilder.build(), 0L, 0L, 1, config);
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalogObject),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }

    CreateNamespaceRequest createNamespaceRequest =
        CreateNamespaceRequest.builder().withNamespace(Namespace.of(NAMESPACE)).build();
    try (Response response =
        services
            .restApi()
            .createNamespace(
                CATALOG,
                createNamespaceRequest,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  private void createTable(TestServices services, String tableName) {
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder()
            .withName(tableName)
            .withLocation(
                String.format("%s/%s/%s/%s", catalogLocation, CATALOG, NAMESPACE, tableName))
            .withSchema(SCHEMA)
            .build();
    services
        .restApi()
        .createTable(
            CATALOG,
            NAMESPACE,
            createTableRequest,
            null,
            services.realmContext(),
            services.securityContext());
  }
}
