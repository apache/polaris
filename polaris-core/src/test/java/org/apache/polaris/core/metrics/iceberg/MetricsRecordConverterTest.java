package org.apache.polaris.core.metrics.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.CommitMetrics;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.ImmutableCommitReport;
import org.apache.iceberg.metrics.ImmutableScanReport;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.junit.jupiter.api.Test;

class MetricsRecordConverterTest {

  @Test
  void testScanReportConversion() {
    ScanReport scanReport =
        ImmutableScanReport.builder()
            .tableName("db.schema.test_table")
            .snapshotId(123456789L)
            .schemaId(1)
            .filter(Expressions.alwaysTrue())
            .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()))
            .build();

    ScanMetricsRecord record =
        MetricsRecordConverter.forScanReport(scanReport).catalogId(100L).tableId(200L).build();

    assertThat(record.catalogId()).isEqualTo(100L);
    assertThat(record.tableId()).isEqualTo(200L);
    assertThat(record.snapshotId()).contains(123456789L);
    assertThat(record.schemaId()).contains(1);
    assertThat(record.filterExpression()).isPresent();
  }

  @Test
  void testCommitReportConversion() {
    CommitMetrics commitMetrics =
        CommitMetrics.of(new org.apache.iceberg.metrics.DefaultMetricsContext());
    CommitMetricsResult metricsResult = CommitMetricsResult.from(commitMetrics, Map.of());

    CommitReport commitReport =
        ImmutableCommitReport.builder()
            .tableName("db.schema.test_table")
            .snapshotId(987654321L)
            .sequenceNumber(5L)
            .operation("append")
            .commitMetrics(metricsResult)
            .build();

    CommitMetricsRecord record =
        MetricsRecordConverter.forCommitReport(commitReport).catalogId(100L).tableId(200L).build();

    assertThat(record.catalogId()).isEqualTo(100L);
    assertThat(record.tableId()).isEqualTo(200L);
    assertThat(record.snapshotId()).isEqualTo(987654321L);
    assertThat(record.sequenceNumber()).contains(5L);
    assertThat(record.operation()).isEqualTo("append");
  }
}
