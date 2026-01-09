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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.ImmutableScanReport;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents;
import org.apache.polaris.service.events.PolarisEventMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link PolarisPersistenceEventListener} focusing on null-safety in metrics
 * extraction.
 */
class PolarisPersistenceEventListenerTest {

  private TestPolarisPersistenceEventListener listener;

  @BeforeEach
  void setUp() {
    listener = new TestPolarisPersistenceEventListener();
  }

  @Test
  void testScanReportWithNullMetadataValues() {
    // Use mocks to simulate a ScanReport with null values in metadata
    // (Iceberg's ImmutableScanReport.Builder doesn't allow nulls, but JSON deserialization might)
    Map<String, String> metadataWithNull = new HashMap<>();
    metadataWithNull.put("trace-id", "valid-trace-id");
    metadataWithNull.put("null-value-key", null);

    ScanReport mockScanReport = mock(ScanReport.class);
    when(mockScanReport.snapshotId()).thenReturn(123L);
    when(mockScanReport.schemaId()).thenReturn(0);
    when(mockScanReport.metadata()).thenReturn(metadataWithNull);
    when(mockScanReport.scanMetrics()).thenReturn(null);

    ReportMetricsRequest mockRequest = mock(ReportMetricsRequest.class);
    when(mockRequest.report()).thenReturn(mockScanReport);

    IcebergRestCatalogEvents.AfterReportMetricsEvent event =
        createAfterReportMetricsEvent(mockRequest);

    // Should not throw NPE
    assertThatCode(() -> listener.onAfterReportMetrics(event)).doesNotThrowAnyException();

    // Verify the valid metadata entry was captured, nulls were skipped
    PolarisEvent persistedEvent = listener.getLastEvent();
    assertThat(persistedEvent).isNotNull();
    Map<String, String> additionalProps = persistedEvent.getAdditionalPropertiesAsMap();
    assertThat(additionalProps).containsEntry("report.trace-id", "valid-trace-id");
    assertThat(additionalProps).doesNotContainKey("report.null-value-key");
  }

  @Test
  void testCommitReportWithNullOperation() {
    // Use mock to simulate a CommitReport with null operation
    CommitReport mockCommitReport = mock(CommitReport.class);
    when(mockCommitReport.snapshotId()).thenReturn(456L);
    when(mockCommitReport.sequenceNumber()).thenReturn(1L);
    when(mockCommitReport.operation()).thenReturn(null); // null operation
    when(mockCommitReport.metadata()).thenReturn(ImmutableMap.of());
    when(mockCommitReport.commitMetrics()).thenReturn(null);

    ReportMetricsRequest mockRequest = mock(ReportMetricsRequest.class);
    when(mockRequest.report()).thenReturn(mockCommitReport);

    IcebergRestCatalogEvents.AfterReportMetricsEvent event =
        createAfterReportMetricsEvent(mockRequest);

    // Should not throw NPE
    assertThatCode(() -> listener.onAfterReportMetrics(event)).doesNotThrowAnyException();

    // Verify operation is not in additional properties (since it was null)
    PolarisEvent persistedEvent = listener.getLastEvent();
    assertThat(persistedEvent).isNotNull();
    Map<String, String> additionalProps = persistedEvent.getAdditionalPropertiesAsMap();
    assertThat(additionalProps)
        .containsEntry("report_type", "commit")
        .containsEntry("snapshot_id", "456")
        .doesNotContainKey("operation");
  }

  @Test
  void testCommitReportWithNullMetadataValues() {
    // Use mock to simulate a CommitReport with null values in metadata
    Map<String, String> metadataWithNull = new HashMap<>();
    metadataWithNull.put("trace-id", "commit-trace-id");
    metadataWithNull.put("null-value-key", null);

    CommitReport mockCommitReport = mock(CommitReport.class);
    when(mockCommitReport.snapshotId()).thenReturn(789L);
    when(mockCommitReport.sequenceNumber()).thenReturn(2L);
    when(mockCommitReport.operation()).thenReturn("append");
    when(mockCommitReport.metadata()).thenReturn(metadataWithNull);
    when(mockCommitReport.commitMetrics()).thenReturn(null);

    ReportMetricsRequest mockRequest = mock(ReportMetricsRequest.class);
    when(mockRequest.report()).thenReturn(mockCommitReport);

    IcebergRestCatalogEvents.AfterReportMetricsEvent event =
        createAfterReportMetricsEvent(mockRequest);

    // Should not throw NPE
    assertThatCode(() -> listener.onAfterReportMetrics(event)).doesNotThrowAnyException();

    // Verify valid entries are captured, nulls are skipped
    PolarisEvent persistedEvent = listener.getLastEvent();
    assertThat(persistedEvent).isNotNull();
    Map<String, String> additionalProps = persistedEvent.getAdditionalPropertiesAsMap();
    assertThat(additionalProps)
        .containsEntry("report.trace-id", "commit-trace-id")
        .containsEntry("operation", "append")
        .doesNotContainKey("report.null-value-key");
  }

  @Test
  void testScanReportWithEmptyMetadata() {
    ImmutableScanReport scanReport =
        ImmutableScanReport.builder()
            .schemaId(0)
            .tableName("test_ns.test_table")
            .snapshotId(100L)
            .addProjectedFieldIds(1)
            .addProjectedFieldNames("id")
            .filter(Expressions.alwaysTrue())
            .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()))
            // Empty metadata map
            .build();

    ReportMetricsRequest request = ReportMetricsRequest.of(scanReport);
    IcebergRestCatalogEvents.AfterReportMetricsEvent event = createAfterReportMetricsEvent(request);

    // Should not throw any exception
    assertThatCode(() -> listener.onAfterReportMetrics(event)).doesNotThrowAnyException();

    PolarisEvent persistedEvent = listener.getLastEvent();
    assertThat(persistedEvent).isNotNull();
    Map<String, String> additionalProps = persistedEvent.getAdditionalPropertiesAsMap();
    assertThat(additionalProps)
        .containsEntry("report_type", "scan")
        .containsEntry("snapshot_id", "100");
  }

  private IcebergRestCatalogEvents.AfterReportMetricsEvent createAfterReportMetricsEvent(
      ReportMetricsRequest request) {
    PolarisEventMetadata metadata =
        new PolarisEventMetadata() {
          @Override
          public UUID eventId() {
            return UUID.randomUUID();
          }

          @Override
          public Instant timestamp() {
            return Instant.now();
          }

          @Override
          public Optional<PolarisPrincipal> user() {
            return Optional.empty();
          }

          @Override
          public Optional<String> requestId() {
            return Optional.of("test-request-id");
          }

          @Override
          public Map<String, String> openTelemetryContext() {
            return ImmutableMap.of();
          }

          @Override
          public String realmId() {
            return "test-realm";
          }
        };

    return new IcebergRestCatalogEvents.AfterReportMetricsEvent(
        metadata, "test-catalog", Namespace.of("test_ns"), "test_table", request);
  }

  /** Concrete test implementation that captures persisted events for verification. */
  private static class TestPolarisPersistenceEventListener extends PolarisPersistenceEventListener {
    private final Map<String, PolarisEvent> events = new ConcurrentHashMap<>();
    private PolarisEvent lastEvent;

    @Override
    protected void processEvent(String realmId, PolarisEvent event) {
      events.put(event.getId(), event);
      lastEvent = event;
    }

    public PolarisEvent getLastEvent() {
      return lastEvent;
    }
  }
}
