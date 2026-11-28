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
package org.apache.polaris.extension.metrics.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.ImmutableScanReport;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.junit.jupiter.api.Test;

class MetricsRecordConverterTest {

  private static ScanReport scanReportWithFilter(Expression filter) {
    ImmutableScanReport.Builder builder =
        ImmutableScanReport.builder()
            .tableName("db.tbl")
            .snapshotId(1L)
            .schemaId(0)
            .projectedFieldIds(List.of(1))
            .projectedFieldNames(List.of("ssn"))
            .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()));
    if (filter != null) {
      builder.filter(filter);
    }
    return builder.build();
  }

  @Test
  void filterLiteralsAreNotPersisted() {
    ScanMetricsRecord record =
        MetricsRecordConverter.forScanReport(
                scanReportWithFilter(Expressions.equal("ssn", "123-45-6789")))
            .catalogId(1L)
            .tableId(2L)
            .build();

    assertThat(record.filterExpression()).isPresent();
    assertThat(record.filterExpression().get()).contains("ssn").doesNotContain("123-45-6789");
  }
}
