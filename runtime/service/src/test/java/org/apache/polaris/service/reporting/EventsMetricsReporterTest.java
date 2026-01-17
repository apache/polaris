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
package org.apache.polaris.service.reporting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.enterprise.inject.Instance;
import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class EventsMetricsReporterTest {

  private MetaStoreManagerFactory metaStoreManagerFactory;
  private RealmContext realmContext;
  private BasePersistence persistence;
  private ObjectMapper objectMapper;

  @SuppressWarnings("unchecked")
  private Instance<SecurityIdentity> securityIdentityInstance = mock(Instance.class);

  private EventsMetricsReporter reporter;

  @BeforeEach
  void setUp() {
    metaStoreManagerFactory = mock(MetaStoreManagerFactory.class);
    realmContext = mock(RealmContext.class);
    persistence = mock(BasePersistence.class);
    objectMapper = new ObjectMapper();

    when(realmContext.getRealmIdentifier()).thenReturn("test-realm");
    when(metaStoreManagerFactory.getOrCreateSession(any())).thenReturn(persistence);
    when(securityIdentityInstance.isResolvable()).thenReturn(false);

    reporter =
        new EventsMetricsReporter(
            metaStoreManagerFactory, realmContext, objectMapper, securityIdentityInstance);
  }

  @Test
  void testReportScanMetrics() {
    ScanReport scanReport = mock(ScanReport.class);
    TableIdentifier table = TableIdentifier.of("db", "test_table");

    reporter.reportMetric("test-catalog", table, scanReport);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<PolarisEvent>> captor = ArgumentCaptor.forClass(List.class);
    verify(persistence).writeEvents(captor.capture());

    List<PolarisEvent> events = captor.getValue();
    assertThat(events).hasSize(1);

    PolarisEvent event = events.get(0);
    assertThat(event.getEventType()).isEqualTo(EventsMetricsReporter.EVENT_TYPE_SCAN_REPORT);
    assertThat(event.getCatalogId()).isEqualTo("test-catalog");
    assertThat(event.getResourceType()).isEqualTo(PolarisEvent.ResourceType.TABLE);
    assertThat(event.getResourceIdentifier()).isEqualTo("db.test_table");
  }

  @Test
  void testReportCommitMetrics() {
    CommitReport commitReport = mock(CommitReport.class);
    TableIdentifier table = TableIdentifier.of("db", "test_table");

    reporter.reportMetric("test-catalog", table, commitReport);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<PolarisEvent>> captor = ArgumentCaptor.forClass(List.class);
    verify(persistence).writeEvents(captor.capture());

    List<PolarisEvent> events = captor.getValue();
    assertThat(events).hasSize(1);

    PolarisEvent event = events.get(0);
    assertThat(event.getEventType()).isEqualTo(EventsMetricsReporter.EVENT_TYPE_COMMIT_REPORT);
    assertThat(event.getCatalogId()).isEqualTo("test-catalog");
    assertThat(event.getResourceType()).isEqualTo(PolarisEvent.ResourceType.TABLE);
    assertThat(event.getResourceIdentifier()).isEqualTo("db.test_table");
  }

  @Test
  void testUnknownMetricsReportTypeIsIgnored() {
    MetricsReport unknownReport = mock(MetricsReport.class);
    TableIdentifier table = TableIdentifier.of("db", "test_table");

    reporter.reportMetric("test-catalog", table, unknownReport);

    verify(persistence, never()).writeEvents(any());
  }

  @Test
  void testEventContainsSerializedMetrics() {
    // Create a mock ScanReport
    ScanReport scanReport = mock(ScanReport.class);
    when(scanReport.tableName()).thenReturn("test_table");
    when(scanReport.snapshotId()).thenReturn(12345L);

    TableIdentifier table = TableIdentifier.of("db", "test_table");

    reporter.reportMetric("test-catalog", table, scanReport);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<PolarisEvent>> captor = ArgumentCaptor.forClass(List.class);
    verify(persistence).writeEvents(captor.capture());

    PolarisEvent event = captor.getValue().get(0);
    String additionalProps = event.getAdditionalProperties();
    // Should contain JSON (at minimum an empty object or serialized report)
    assertThat(additionalProps).isNotNull();
  }

  @Test
  void testPersistenceErrorDoesNotThrow() {
    ScanReport scanReport = mock(ScanReport.class);
    TableIdentifier table = TableIdentifier.of("db", "test_table");

    when(metaStoreManagerFactory.getOrCreateSession(any()))
        .thenThrow(new RuntimeException("Database error"));

    // Should not throw
    reporter.reportMetric("test-catalog", table, scanReport);
  }
}
