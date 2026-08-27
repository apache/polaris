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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jakarta.enterprise.inject.Instance;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
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
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.service.metrics.MetricType;
import org.apache.polaris.service.metrics.MetricsReportEnvelope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

/**
 * Metrics reporting is best-effort telemetry: a failing {@link MetricsPersistence} must never
 * propagate an exception out of {@link PersistingMetricsReporter#reportMetric}, which runs
 * synchronously on the client request path.
 */
class PersistingMetricsReporterTest {

  private static final MetricsPersistence FAILING_PERSISTENCE =
      new MetricsPersistence() {
        @Override
        public void writeScanReport(ScanMetricsRecord record) {
          throw new RuntimeException("simulated persistence failure");
        }

        @Override
        public void writeCommitReport(CommitMetricsRecord record) {
          throw new RuntimeException("simulated persistence failure");
        }
      };

  private ListAppender<ILoggingEvent> logAppender;
  private Logger reporterLogger;

  @BeforeEach
  void attachLogAppender() {
    reporterLogger = (Logger) LoggerFactory.getLogger(PersistingMetricsReporter.class);
    logAppender = new ListAppender<>();
    logAppender.start();
    reporterLogger.addAppender(logAppender);
  }

  @AfterEach
  void detachLogAppender() {
    reporterLogger.detachAppender(logAppender);
    logAppender.stop();
  }

  private static PersistingMetricsReporter newReporter(MetricsPersistence persistence) {
    return new PersistingMetricsReporter(persistence, unsatisfiedInstance(), unsatisfiedInstance());
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> unsatisfiedInstance() {
    Instance<T> instance = Mockito.mock(Instance.class);
    when(instance.isResolvable()).thenReturn(false);
    return instance;
  }

  private static ScanReport scanReport() {
    return ImmutableScanReport.builder()
        .tableName("t")
        .snapshotId(123L)
        .schemaId(456)
        .projectedFieldIds(List.of(1, 2, 3))
        .projectedFieldNames(List.of("f1", "f2", "f3"))
        .filter(Expressions.alwaysTrue())
        .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()))
        .build();
  }

  private static CommitReport commitReport() {
    return ImmutableCommitReport.builder()
        .tableName("t")
        .snapshotId(23L)
        .operation("DELETE")
        .sequenceNumber(4L)
        .commitMetrics(CommitMetricsResult.from(CommitMetrics.noop(), Map.of()))
        .build();
  }

  private static MetricsReportEnvelope scanEnvelope() {
    return new MetricsReportEnvelope(
        "catalog",
        1L,
        TableIdentifier.of("ns", "t"),
        2L,
        MetricType.SCAN,
        scanReport(),
        Instant.now());
  }

  private static MetricsReportEnvelope commitEnvelope() {
    return new MetricsReportEnvelope(
        "catalog",
        1L,
        TableIdentifier.of("ns", "t"),
        2L,
        MetricType.COMMIT,
        commitReport(),
        Instant.now());
  }

  @Test
  void scanReportPersistenceFailureDoesNotPropagate() {
    assertThatCode(() -> newReporter(FAILING_PERSISTENCE).reportMetric(scanEnvelope()))
        .doesNotThrowAnyException();

    assertThat(logAppender.list)
        .anySatisfy(
            event -> {
              assertThat(event.getLevel()).isEqualTo(Level.ERROR);
              assertThat(event.getFormattedMessage())
                  .contains("Failed to persist scan metrics for catalog.ns.t");
              assertThat(event.getThrowableProxy()).isNotNull();
            });
  }

  @Test
  void commitReportPersistenceFailureDoesNotPropagate() {
    assertThatCode(() -> newReporter(FAILING_PERSISTENCE).reportMetric(commitEnvelope()))
        .doesNotThrowAnyException();

    assertThat(logAppender.list)
        .anySatisfy(
            event -> {
              assertThat(event.getLevel()).isEqualTo(Level.ERROR);
              assertThat(event.getFormattedMessage())
                  .contains("Failed to persist commit metrics for catalog.ns.t");
              assertThat(event.getThrowableProxy()).isNotNull();
            });
  }

  @Test
  void scanReportSuccessWritesPersistence() {
    MetricsPersistence persistence = mock(MetricsPersistence.class);

    newReporter(persistence).reportMetric(scanEnvelope());

    verify(persistence).writeScanReport(any(ScanMetricsRecord.class));
    assertThat(logAppender.list).noneMatch(event -> event.getLevel().isGreaterOrEqual(Level.ERROR));
  }

  @Test
  void commitReportSuccessWritesPersistence() {
    MetricsPersistence persistence = mock(MetricsPersistence.class);

    newReporter(persistence).reportMetric(commitEnvelope());

    verify(persistence).writeCommitReport(any(CommitMetricsRecord.class));
    assertThat(logAppender.list).noneMatch(event -> event.getLevel().isGreaterOrEqual(Level.ERROR));
  }
}
