package org.apache.polaris.service.reporting;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
@Identifier("default")
public class DefaultMetricsReporter implements MetricsReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMetricsReporter.class);

  @Override
  public void reportMetric(
      String prefix, String namespace, String table, ReportMetricsRequest reportMetricsRequest) {
    // Do Nothing
  }
}
