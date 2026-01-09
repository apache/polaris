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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.ScanReport;
import org.junit.jupiter.api.Test;

class CompositeMetricsReporterTest {

  @Test
  void testDelegatesToAllReporters() {
    PolarisMetricsReporter reporter1 = mock(PolarisMetricsReporter.class);
    PolarisMetricsReporter reporter2 = mock(PolarisMetricsReporter.class);
    PolarisMetricsReporter reporter3 = mock(PolarisMetricsReporter.class);

    CompositeMetricsReporter composite =
        new CompositeMetricsReporter(List.of(reporter1, reporter2, reporter3));

    ScanReport scanReport = mock(ScanReport.class);
    TableIdentifier table = TableIdentifier.of("db", "test_table");

    composite.reportMetric("test-catalog", table, scanReport);

    verify(reporter1).reportMetric("test-catalog", table, scanReport);
    verify(reporter2).reportMetric("test-catalog", table, scanReport);
    verify(reporter3).reportMetric("test-catalog", table, scanReport);
  }

  @Test
  void testContinuesOnDelegateFailure() {
    PolarisMetricsReporter reporter1 = mock(PolarisMetricsReporter.class);
    PolarisMetricsReporter reporter2 = mock(PolarisMetricsReporter.class);
    PolarisMetricsReporter reporter3 = mock(PolarisMetricsReporter.class);

    // Make reporter2 throw an exception
    doThrow(new RuntimeException("Reporter 2 failed"))
        .when(reporter2)
        .reportMetric(any(), any(), any());

    CompositeMetricsReporter composite =
        new CompositeMetricsReporter(List.of(reporter1, reporter2, reporter3));

    ScanReport scanReport = mock(ScanReport.class);
    TableIdentifier table = TableIdentifier.of("db", "test_table");

    // Should not throw
    composite.reportMetric("test-catalog", table, scanReport);

    // All reporters should still be called
    verify(reporter1).reportMetric("test-catalog", table, scanReport);
    verify(reporter2).reportMetric("test-catalog", table, scanReport);
    verify(reporter3).reportMetric("test-catalog", table, scanReport);
  }

  @Test
  void testEmptyDelegatesList() {
    CompositeMetricsReporter composite = new CompositeMetricsReporter(List.of());

    ScanReport scanReport = mock(ScanReport.class);
    TableIdentifier table = TableIdentifier.of("db", "test_table");

    // Should not throw
    composite.reportMetric("test-catalog", table, scanReport);

    assertThat(composite.getDelegates()).isEmpty();
  }

  @Test
  void testSingleDelegate() {
    PolarisMetricsReporter reporter = mock(PolarisMetricsReporter.class);
    CompositeMetricsReporter composite = new CompositeMetricsReporter(List.of(reporter));

    ScanReport scanReport = mock(ScanReport.class);
    TableIdentifier table = TableIdentifier.of("db", "test_table");

    composite.reportMetric("test-catalog", table, scanReport);

    verify(reporter).reportMetric("test-catalog", table, scanReport);
    assertThat(composite.getDelegates()).hasSize(1);
  }

  @Test
  void testGetDelegatesReturnsUnmodifiableList() {
    PolarisMetricsReporter reporter = mock(PolarisMetricsReporter.class);
    CompositeMetricsReporter composite = new CompositeMetricsReporter(List.of(reporter));

    List<PolarisMetricsReporter> delegates = composite.getDelegates();

    // Should be unmodifiable
    assertThat(delegates).hasSize(1);
    org.junit.jupiter.api.Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> delegates.add(mock(PolarisMetricsReporter.class)));
  }

  @Test
  void testNullMetricsReportDoesNotThrow() {
    PolarisMetricsReporter reporter = mock(PolarisMetricsReporter.class);
    CompositeMetricsReporter composite = new CompositeMetricsReporter(List.of(reporter));

    TableIdentifier table = TableIdentifier.of("db", "test_table");

    // Should not throw even with null report
    composite.reportMetric("test-catalog", table, null);

    verify(reporter).reportMetric(eq("test-catalog"), eq(table), eq(null));
  }
}
