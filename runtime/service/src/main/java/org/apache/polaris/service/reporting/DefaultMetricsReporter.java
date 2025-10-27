package org.apache.polaris.service.reporting;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReport;
@RequestScoped
@Identifier("default")
public class DefaultMetricsReporter implements PolarisMetricsReporter {

  @Override
  public void reportMetric(
      String warehouse, TableIdentifier table, MetricsReport metricsReport) {
    // Do Nothing
  }
}
