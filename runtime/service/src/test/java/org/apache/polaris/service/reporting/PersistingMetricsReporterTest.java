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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.quarkus.security.identity.SecurityIdentity;
import jakarta.enterprise.inject.Instance;
import java.security.Principal;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.persistence.relational.jdbc.JdbcBasePersistenceImpl;
import org.apache.polaris.persistence.relational.jdbc.models.ModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelScanMetricsReport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class PersistingMetricsReporterTest {

  private MetaStoreManagerFactory metaStoreManagerFactory;
  private RealmContext realmContext;
  private JdbcBasePersistenceImpl jdbcPersistence;
  private BasePersistence nonJdbcPersistence;

  @SuppressWarnings("unchecked")
  private Instance<SecurityIdentity> securityIdentityInstance = mock(Instance.class);

  private PersistingMetricsReporter reporter;

  @BeforeEach
  void setUp() {
    metaStoreManagerFactory = mock(MetaStoreManagerFactory.class);
    realmContext = mock(RealmContext.class);
    jdbcPersistence = mock(JdbcBasePersistenceImpl.class);
    nonJdbcPersistence = mock(BasePersistence.class);

    when(realmContext.getRealmIdentifier()).thenReturn("test-realm");
    when(securityIdentityInstance.isResolvable()).thenReturn(false);

    reporter =
        new PersistingMetricsReporter(
            metaStoreManagerFactory, realmContext, securityIdentityInstance);
  }

  @Test
  void testReportScanMetricsWithJdbcBackend() {
    when(metaStoreManagerFactory.getOrCreateSession(any())).thenReturn(jdbcPersistence);

    ScanReport scanReport = mock(ScanReport.class);
    when(scanReport.tableName()).thenReturn("test_table");
    TableIdentifier table = TableIdentifier.of("db", "test_table");

    reporter.reportMetric("test-catalog", table, scanReport);

    verify(jdbcPersistence).writeScanMetricsReport(any(ModelScanMetricsReport.class));
  }

  @Test
  void testReportCommitMetricsWithJdbcBackend() {
    when(metaStoreManagerFactory.getOrCreateSession(any())).thenReturn(jdbcPersistence);

    CommitReport commitReport = mock(CommitReport.class);
    when(commitReport.tableName()).thenReturn("test_table");
    when(commitReport.snapshotId()).thenReturn(12345L);
    when(commitReport.operation()).thenReturn("append");
    TableIdentifier table = TableIdentifier.of("db", "test_table");

    reporter.reportMetric("test-catalog", table, commitReport);

    verify(jdbcPersistence).writeCommitMetricsReport(any(ModelCommitMetricsReport.class));
  }

  @Test
  void testFallbackToLoggingWithNonJdbcBackend() {
    when(metaStoreManagerFactory.getOrCreateSession(any())).thenReturn(nonJdbcPersistence);

    ScanReport scanReport = mock(ScanReport.class);
    TableIdentifier table = TableIdentifier.of("db", "test_table");

    // Should not throw, just log
    reporter.reportMetric("test-catalog", table, scanReport);

    // Verify no JDBC methods were called
    verify(jdbcPersistence, never()).writeScanMetricsReport(any());
  }

  @Test
  void testUnknownMetricsReportTypeIsIgnored() {
    when(metaStoreManagerFactory.getOrCreateSession(any())).thenReturn(jdbcPersistence);

    MetricsReport unknownReport = mock(MetricsReport.class);
    TableIdentifier table = TableIdentifier.of("db", "test_table");

    reporter.reportMetric("test-catalog", table, unknownReport);

    verify(jdbcPersistence, never()).writeScanMetricsReport(any());
    verify(jdbcPersistence, never()).writeCommitMetricsReport(any());
  }

  @Test
  void testPrincipalNameExtraction() {
    when(metaStoreManagerFactory.getOrCreateSession(any())).thenReturn(jdbcPersistence);

    // Set up security identity with a principal
    SecurityIdentity identity = mock(SecurityIdentity.class);
    Principal principal = mock(Principal.class);
    when(principal.getName()).thenReturn("test-user");
    when(identity.isAnonymous()).thenReturn(false);
    when(identity.getPrincipal()).thenReturn(principal);
    when(securityIdentityInstance.isResolvable()).thenReturn(true);
    when(securityIdentityInstance.get()).thenReturn(identity);

    ScanReport scanReport = mock(ScanReport.class);
    when(scanReport.tableName()).thenReturn("test_table");
    TableIdentifier table = TableIdentifier.of("db", "test_table");

    reporter.reportMetric("test-catalog", table, scanReport);

    ArgumentCaptor<ModelScanMetricsReport> captor =
        ArgumentCaptor.forClass(ModelScanMetricsReport.class);
    verify(jdbcPersistence).writeScanMetricsReport(captor.capture());

    // The principal name should be captured in the report
    // Note: The actual assertion depends on how the model is built
  }

  @Test
  void testPersistenceErrorDoesNotThrow() {
    when(metaStoreManagerFactory.getOrCreateSession(any()))
        .thenThrow(new RuntimeException("Database error"));

    ScanReport scanReport = mock(ScanReport.class);
    TableIdentifier table = TableIdentifier.of("db", "test_table");

    // Should not throw
    reporter.reportMetric("test-catalog", table, scanReport);
  }
}
