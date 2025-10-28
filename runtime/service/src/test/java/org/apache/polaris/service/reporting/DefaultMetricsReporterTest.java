package org.apache.polaris.service.reporting;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.commons.lang3.function.TriConsumer;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReport;
import org.junit.jupiter.api.Test;

public class DefaultMetricsReporterTest {

  @Test
  void testLogging() {
    TriConsumer<String, TableIdentifier, MetricsReport> mockConsumer = mock(TriConsumer.class);
    DefaultMetricsReporter reporter = new DefaultMetricsReporter(mockConsumer);
    String warehouse = "testWarehouse";
    TableIdentifier table = TableIdentifier.of("testNamespace", "testTable");
    MetricsReport metricsReport = mock(MetricsReport.class);

    reporter.reportMetric(warehouse, table, metricsReport);

    verify(mockConsumer).accept(warehouse, table, metricsReport);
  }
}
