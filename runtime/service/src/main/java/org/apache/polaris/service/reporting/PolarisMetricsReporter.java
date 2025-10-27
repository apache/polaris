package org.apache.polaris.service.reporting;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReport;

public interface PolarisMetricsReporter {
  public void reportMetric(String warehouse, TableIdentifier table, MetricsReport metricsReport);
}
