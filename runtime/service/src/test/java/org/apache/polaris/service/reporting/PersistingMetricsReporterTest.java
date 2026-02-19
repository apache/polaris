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
package org.apache.polaris.service.reporting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.CommitMetrics;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.ImmutableCommitReport;
import org.apache.iceberg.metrics.ImmutableScanReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class PersistingMetricsReporterTest {

  private static final String CATALOG_NAME = "test-catalog";
  private static final long CATALOG_ID = 12345L;
  private static final long TABLE_ID = 67890L;
  private static final String TABLE_NAME = "test_table";
  private static final List<String> NAMESPACE = Arrays.asList("db", "schema");
  private static final TableIdentifier TABLE_IDENTIFIER =
      TableIdentifier.of(Namespace.of("db", "schema"), TABLE_NAME);

  private CallContext callContext;
  private PolarisCallContext polarisCallContext;
  private PolarisMetaStoreManager metaStoreManager;
  private MetricsPersistence metricsPersistence;
  private PersistingMetricsReporter reporter;

  @BeforeEach
  void setUp() {
    polarisCallContext = mock(PolarisCallContext.class);
    callContext = mock(CallContext.class);
    when(callContext.getPolarisCallContext()).thenReturn(polarisCallContext);

    metaStoreManager = mock(PolarisMetaStoreManager.class);
    metricsPersistence = mock(MetricsPersistence.class);

    reporter = new PersistingMetricsReporter(callContext, metaStoreManager, metricsPersistence);
  }

  @Test
  void testReportScanMetrics() {
    // Setup catalog lookup
    PolarisBaseEntity catalogEntity = createCatalogEntity(CATALOG_ID, CATALOG_NAME);
    when(metaStoreManager.readEntityByName(
            eq(polarisCallContext),
            eq(null),
            eq(PolarisEntityType.CATALOG),
            eq(PolarisEntitySubType.ANY_SUBTYPE),
            eq(CATALOG_NAME)))
        .thenReturn(new EntityResult(catalogEntity));

    // Setup namespace lookups - "db" and "schema"
    PolarisBaseEntity dbNamespaceEntity = createNamespaceEntity(11111L, "db", CATALOG_ID);
    when(metaStoreManager.readEntityByName(
            eq(polarisCallContext),
            any(),
            eq(PolarisEntityType.NAMESPACE),
            eq(PolarisEntitySubType.ANY_SUBTYPE),
            eq("db")))
        .thenReturn(new EntityResult(dbNamespaceEntity));

    PolarisBaseEntity schemaNamespaceEntity = createNamespaceEntity(22222L, "schema", CATALOG_ID);
    when(metaStoreManager.readEntityByName(
            eq(polarisCallContext),
            any(),
            eq(PolarisEntityType.NAMESPACE),
            eq(PolarisEntitySubType.ANY_SUBTYPE),
            eq("schema")))
        .thenReturn(new EntityResult(schemaNamespaceEntity));

    // Setup table lookup
    PolarisBaseEntity tableEntity = createTableEntity(TABLE_ID, TABLE_NAME, CATALOG_ID);
    when(metaStoreManager.readEntityByName(
            eq(polarisCallContext),
            any(),
            eq(PolarisEntityType.TABLE_LIKE),
            eq(PolarisEntitySubType.ANY_SUBTYPE),
            eq(TABLE_NAME)))
        .thenReturn(new EntityResult(tableEntity));

    // Create a scan report
    ScanReport scanReport = createScanReport();

    // Call the reporter
    reporter.reportMetric(CATALOG_NAME, TABLE_IDENTIFIER, scanReport, Instant.now());

    // Verify persistence was called with correct record
    ArgumentCaptor<ScanMetricsRecord> captor = ArgumentCaptor.forClass(ScanMetricsRecord.class);
    verify(metricsPersistence).writeScanReport(captor.capture());

    ScanMetricsRecord record = captor.getValue();
    assertThat(record.catalogId()).isEqualTo(CATALOG_ID);
    assertThat(record.tableId()).isEqualTo(TABLE_ID);
    assertThat(record.reportId()).isNotNull();
  }

  @Test
  void testReportCommitMetrics() {
    // Setup catalog lookup
    PolarisBaseEntity catalogEntity = createCatalogEntity(CATALOG_ID, CATALOG_NAME);
    when(metaStoreManager.readEntityByName(
            eq(polarisCallContext),
            eq(null),
            eq(PolarisEntityType.CATALOG),
            eq(PolarisEntitySubType.ANY_SUBTYPE),
            eq(CATALOG_NAME)))
        .thenReturn(new EntityResult(catalogEntity));

    // Setup namespace lookups - "db" and "schema"
    PolarisBaseEntity dbNamespaceEntity = createNamespaceEntity(11111L, "db", CATALOG_ID);
    when(metaStoreManager.readEntityByName(
            eq(polarisCallContext),
            any(),
            eq(PolarisEntityType.NAMESPACE),
            eq(PolarisEntitySubType.ANY_SUBTYPE),
            eq("db")))
        .thenReturn(new EntityResult(dbNamespaceEntity));

    PolarisBaseEntity schemaNamespaceEntity = createNamespaceEntity(22222L, "schema", CATALOG_ID);
    when(metaStoreManager.readEntityByName(
            eq(polarisCallContext),
            any(),
            eq(PolarisEntityType.NAMESPACE),
            eq(PolarisEntitySubType.ANY_SUBTYPE),
            eq("schema")))
        .thenReturn(new EntityResult(schemaNamespaceEntity));

    // Setup table lookup
    PolarisBaseEntity tableEntity = createTableEntity(TABLE_ID, TABLE_NAME, CATALOG_ID);
    when(metaStoreManager.readEntityByName(
            eq(polarisCallContext),
            any(),
            eq(PolarisEntityType.TABLE_LIKE),
            eq(PolarisEntitySubType.ANY_SUBTYPE),
            eq(TABLE_NAME)))
        .thenReturn(new EntityResult(tableEntity));

    // Create a commit report
    CommitReport commitReport = createCommitReport();

    // Call the reporter
    reporter.reportMetric(CATALOG_NAME, TABLE_IDENTIFIER, commitReport, Instant.now());

    // Verify persistence was called with correct record
    ArgumentCaptor<CommitMetricsRecord> captor = ArgumentCaptor.forClass(CommitMetricsRecord.class);
    verify(metricsPersistence).writeCommitReport(captor.capture());

    CommitMetricsRecord record = captor.getValue();
    assertThat(record.catalogId()).isEqualTo(CATALOG_ID);
    assertThat(record.tableId()).isEqualTo(TABLE_ID);
    assertThat(record.reportId()).isNotNull();
  }

  @Test
  void testCatalogNotFound() {
    // Setup catalog lookup to return entity not found
    when(metaStoreManager.readEntityByName(
            eq(polarisCallContext),
            eq(null),
            eq(PolarisEntityType.CATALOG),
            eq(PolarisEntitySubType.ANY_SUBTYPE),
            eq(CATALOG_NAME)))
        .thenReturn(
            new EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, "Catalog not found"));

    ScanReport scanReport = createScanReport();

    // Call the reporter - should not throw
    reporter.reportMetric(CATALOG_NAME, TABLE_IDENTIFIER, scanReport, Instant.now());

    // Verify persistence was NOT called since catalog was not found
    verify(metricsPersistence, never()).writeScanReport(any());
    verify(metricsPersistence, never()).writeCommitReport(any());
  }

  @Test
  void testUnknownReportType() {
    // Setup catalog lookup
    PolarisBaseEntity catalogEntity = createCatalogEntity(CATALOG_ID, CATALOG_NAME);
    when(metaStoreManager.readEntityByName(
            eq(polarisCallContext),
            eq(null),
            eq(PolarisEntityType.CATALOG),
            eq(PolarisEntitySubType.ANY_SUBTYPE),
            eq(CATALOG_NAME)))
        .thenReturn(new EntityResult(catalogEntity));

    // Setup namespace lookups - "db" and "schema"
    PolarisBaseEntity dbNamespaceEntity = createNamespaceEntity(11111L, "db", CATALOG_ID);
    when(metaStoreManager.readEntityByName(
            eq(polarisCallContext),
            any(),
            eq(PolarisEntityType.NAMESPACE),
            eq(PolarisEntitySubType.ANY_SUBTYPE),
            eq("db")))
        .thenReturn(new EntityResult(dbNamespaceEntity));

    PolarisBaseEntity schemaNamespaceEntity = createNamespaceEntity(22222L, "schema", CATALOG_ID);
    when(metaStoreManager.readEntityByName(
            eq(polarisCallContext),
            any(),
            eq(PolarisEntityType.NAMESPACE),
            eq(PolarisEntitySubType.ANY_SUBTYPE),
            eq("schema")))
        .thenReturn(new EntityResult(schemaNamespaceEntity));

    // Setup table lookup
    PolarisBaseEntity tableEntity = createTableEntity(TABLE_ID, TABLE_NAME, CATALOG_ID);
    when(metaStoreManager.readEntityByName(
            eq(polarisCallContext),
            any(),
            eq(PolarisEntityType.TABLE_LIKE),
            eq(PolarisEntitySubType.ANY_SUBTYPE),
            eq(TABLE_NAME)))
        .thenReturn(new EntityResult(tableEntity));

    // Create an unknown report type (using a mock)
    MetricsReport unknownReport = mock(MetricsReport.class);

    // Call the reporter - should not throw
    reporter.reportMetric(CATALOG_NAME, TABLE_IDENTIFIER, unknownReport, Instant.now());

    // Verify persistence was NOT called since report type is unknown
    verify(metricsPersistence, never()).writeScanReport(any());
    verify(metricsPersistence, never()).writeCommitReport(any());
  }

  private PolarisBaseEntity createCatalogEntity(long id, String name) {
    return new PolarisBaseEntity.Builder()
        .catalogId(0L)
        .id(id)
        .parentId(0L)
        .typeCode(PolarisEntityType.CATALOG.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .name(name)
        .entityVersion(1)
        .build();
  }

  private PolarisBaseEntity createNamespaceEntity(long id, String name, long catalogId) {
    return new PolarisBaseEntity.Builder()
        .catalogId(catalogId)
        .id(id)
        .parentId(catalogId) // Parent is the catalog for simplicity
        .typeCode(PolarisEntityType.NAMESPACE.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .name(name)
        .entityVersion(1)
        .build();
  }

  private PolarisBaseEntity createTableEntity(long id, String name, long catalogId) {
    return new PolarisBaseEntity.Builder()
        .catalogId(catalogId)
        .id(id)
        .parentId(catalogId) // Parent is the catalog for simplicity
        .typeCode(PolarisEntityType.TABLE_LIKE.getCode())
        .subTypeCode(PolarisEntitySubType.ICEBERG_TABLE.getCode())
        .name(name)
        .entityVersion(1)
        .build();
  }

  private ScanReport createScanReport() {
    return ImmutableScanReport.builder()
        .tableName("db.schema.test_table")
        .snapshotId(123456789L)
        .schemaId(1)
        .filter(Expressions.alwaysTrue())
        .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()))
        .build();
  }

  private CommitReport createCommitReport() {
    CommitMetrics commitMetrics =
        CommitMetrics.of(new org.apache.iceberg.metrics.DefaultMetricsContext());
    CommitMetricsResult metricsResult = CommitMetricsResult.from(commitMetrics, Map.of());

    return ImmutableCommitReport.builder()
        .tableName("db.schema.test_table")
        .snapshotId(987654321L)
        .sequenceNumber(5L)
        .operation("append")
        .commitMetrics(metricsResult)
        .build();
  }
}
