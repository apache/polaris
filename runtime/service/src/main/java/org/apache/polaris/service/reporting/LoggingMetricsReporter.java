package org.apache.polaris.service.reporting;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
@Identifier("logging")
public class LoggingMetricsReporter implements MetricsReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoggingMetricsReporter.class);

  @Override
  public void reportMetric(
      String prefix, String namespace, String table, ReportMetricsRequest reportMetricsRequest) {
    LOGGER.info(prefix);
    LOGGER.info(namespace);
    LOGGER.info(table);
    LOGGER.info(reportMetricsRequest.toString());
  }
}
