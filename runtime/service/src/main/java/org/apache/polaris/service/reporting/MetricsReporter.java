package org.apache.polaris.service.reporting;

import org.apache.iceberg.rest.requests.ReportMetricsRequest;

public interface MetricsReporter {
  public void reportMetric(
      String prefix, String namespace, String table, ReportMetricsRequest reportMetricsRequest);
}
