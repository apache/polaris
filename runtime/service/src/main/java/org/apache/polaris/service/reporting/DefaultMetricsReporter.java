package org.apache.polaris.service.reporting;

import com.google.common.annotations.VisibleForTesting;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;
import org.apache.commons.lang3.function.TriConsumer;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
@Identifier("default")
public class DefaultMetricsReporter implements PolarisMetricsReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMetricsReporter.class);

  private final TriConsumer<String, TableIdentifier, MetricsReport> reportConsumer;

  public DefaultMetricsReporter() {
    this(
        (warehouse, table, metricsReport) ->
            LOGGER.info("{}.{}: {}", warehouse, table, metricsReport));
  }

  @VisibleForTesting
  DefaultMetricsReporter(TriConsumer<String, TableIdentifier, MetricsReport> reportConsumer) {
    this.reportConsumer = reportConsumer;
  }

  @Override
  public void reportMetric(String warehouse, TableIdentifier table, MetricsReport metricsReport) {
    reportConsumer.accept(warehouse, table, metricsReport);
  }
}
