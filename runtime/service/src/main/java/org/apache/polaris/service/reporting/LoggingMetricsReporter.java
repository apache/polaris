package org.apache.polaris.service.reporting;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
@Identifier("logging")
public class LoggingMetricsReporter implements PolarisMetricsReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoggingMetricsReporter.class);

  @Override
  public void reportMetric(
      String warehouse, TableIdentifier table, MetricsReport metricsReport) {
    StringBuilder reportString = new StringBuilder();
    if(metricsReport instanceof ScanReport){
      ScanReport report = (ScanReport) metricsReport;
      reportString.append(report.tableName());
      reportString.append(report.snapshotId());
      reportString.append(report.filter());
      reportString.append(report.schemaId());
      reportString.append(report.projectedFieldIds());
      reportString.append(report.projectedFieldNames());
      reportString.append(report.scanMetrics());
      reportString.append(report.metadata());
    } else if (metricsReport instanceof CommitReport) {
      CommitReport report = (CommitReport) metricsReport;
      reportString.append(report.tableName());
      reportString.append(report.snapshotId());
      reportString.append(report.sequenceNumber());
      reportString.append(report.operation());
      reportString.append(report.commitMetrics());
      reportString.append(report.metadata());
    }
    LOGGER.info("{}.{}.{}: {}", warehouse, table.name(), reportString.toString());
  }
}
