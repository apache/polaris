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
package org.apache.polaris.extension.metrics.reports;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MetricsReportsService}.
 *
 * <p>All heavy dependencies (resolution manifest, authorizer, persistence) are mocked so these
 * tests run without any database or CDI container.
 */
class MetricsReportsServiceTest {

  private static final String CATALOG = "test-catalog";
  private static final String NAMESPACE = "db\u001Fschema"; // multi-level: db.schema
  private static final String TABLE = "events";
  private static final long CATALOG_ID = 100L;
  private static final long TABLE_ID = 200L;

  private MetricsPersistence persistence;
  private PolarisAuthorizer authorizer;
  private PolarisResolutionManifest manifest;
  private PolarisPrincipal principal;
  private ResolutionManifestFactory factory;
  private MetricsReportsService service;
  private RealmContext realmContext;
  private SecurityContext securityContext;

  @BeforeEach
  void setUp() {
    persistence = mock(MetricsPersistence.class);

    // BasePersistence extends MetricsPersistence, so we need a BasePersistence mock.
    org.apache.polaris.core.persistence.BasePersistence basePersistence =
        mock(org.apache.polaris.core.persistence.BasePersistence.class);
    // Delegate MetricsPersistence calls to the persistence mock.
    when(basePersistence.listScanReports(anyLong(), anyLong(), any(), any(), any(), any(), any()))
        .thenAnswer(
            inv ->
                persistence.listScanReports(
                    inv.getArgument(0),
                    inv.getArgument(1),
                    inv.getArgument(2),
                    inv.getArgument(3),
                    inv.getArgument(4),
                    inv.getArgument(5),
                    inv.getArgument(6)));
    when(basePersistence.listCommitReports(anyLong(), anyLong(), any(), any(), any(), any(), any()))
        .thenAnswer(
            inv ->
                persistence.listCommitReports(
                    inv.getArgument(0),
                    inv.getArgument(1),
                    inv.getArgument(2),
                    inv.getArgument(3),
                    inv.getArgument(4),
                    inv.getArgument(5),
                    inv.getArgument(6)));

    PolarisCallContext polarisCallContext = mock(PolarisCallContext.class);
    when(polarisCallContext.getMetaStore()).thenReturn(basePersistence);

    CallContext callContext = mock(CallContext.class);
    when(callContext.getPolarisCallContext()).thenReturn(polarisCallContext);

    authorizer = mock(PolarisAuthorizer.class);
    doNothing()
        .when(authorizer)
        .authorizeOrThrow(
            any(PolarisPrincipal.class),
            any(Set.class),
            any(PolarisAuthorizableOperation.class),
            any(),
            (org.apache.polaris.core.persistence.PolarisResolvedPathWrapper) isNull());

    principal = mock(PolarisPrincipal.class);

    // Wire a resolution manifest that always succeeds for CATALOG/NAMESPACE/TABLE.
    PolarisEntity tableEntity = mock(PolarisEntity.class);
    when(tableEntity.getCatalogId()).thenReturn(CATALOG_ID);
    when(tableEntity.getId()).thenReturn(TABLE_ID);

    ResolvedPolarisEntity resolvedLeaf = mock(ResolvedPolarisEntity.class);
    when(resolvedLeaf.getEntity()).thenReturn(tableEntity);

    PolarisResolvedPathWrapper tableWrapper = mock(PolarisResolvedPathWrapper.class);
    when(tableWrapper.getResolvedLeafEntity()).thenReturn(resolvedLeaf);

    manifest = mock(PolarisResolutionManifest.class);
    when(manifest.resolveAll()).thenReturn(new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS));
    when(manifest.getResolvedPath(
            any(ResolvedPathKey.class), eq(PolarisEntitySubType.ANY_SUBTYPE), eq(true)))
        .thenReturn(tableWrapper);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles()).thenReturn(Set.of());

    factory = mock(ResolutionManifestFactory.class);
    when(factory.createResolutionManifest(eq(principal), eq(CATALOG))).thenReturn(manifest);

    service = new MetricsReportsService(callContext, authorizer, principal, factory);
    realmContext = mock(RealmContext.class);
    securityContext = mock(SecurityContext.class);
  }

  // ── scan ─────────────────────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void listScanMetricsReturns200WithReports() {
    ScanMetricsRecord r = scanRecord("r-1");
    when(persistence.listScanReports(
            eq(CATALOG_ID),
            eq(TABLE_ID),
            isNull(),
            isNull(),
            isNull(),
            isNull(),
            any(PageToken.class)))
        .thenReturn(Page.fromItems(List.of(r)));

    Response response =
        service.listTableMetrics(
            CATALOG,
            NAMESPACE,
            TABLE,
            "scan",
            null,
            10,
            null,
            null,
            null,
            null,
            realmContext,
            securityContext);

    assertThat(response.getStatus()).isEqualTo(200);
    Map<String, Object> body = (Map<String, Object>) response.getEntity();
    assertThat(body.get("metricType")).isEqualTo("scan");
    assertThat((List<?>) body.get("reports")).hasSize(1);
  }

  @Test
  @SuppressWarnings("unchecked")
  void listScanMetricsPaginationTokenPropagated() {
    when(persistence.listScanReports(
            eq(CATALOG_ID),
            eq(TABLE_ID),
            isNull(),
            isNull(),
            isNull(),
            isNull(),
            any(PageToken.class)))
        .thenReturn(Page.fromItems(List.of()));

    Response response =
        service.listTableMetrics(
            CATALOG,
            NAMESPACE,
            TABLE,
            "scan",
            null,
            null,
            null,
            null,
            null,
            null,
            realmContext,
            securityContext);

    assertThat(response.getStatus()).isEqualTo(200);
    Map<String, Object> body = (Map<String, Object>) response.getEntity();
    assertThat(body).containsKey("nextPageToken");
    assertThat(body.get("nextPageToken")).isNull();
  }

  // ── commit ────────────────────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void listCommitMetricsReturns200WithReports() {
    CommitMetricsRecord r = commitRecord("c-1");
    when(persistence.listCommitReports(
            eq(CATALOG_ID),
            eq(TABLE_ID),
            isNull(),
            isNull(),
            isNull(),
            isNull(),
            any(PageToken.class)))
        .thenReturn(Page.fromItems(List.of(r)));

    Response response =
        service.listTableMetrics(
            CATALOG,
            NAMESPACE,
            TABLE,
            "commit",
            null,
            10,
            null,
            null,
            null,
            null,
            realmContext,
            securityContext);

    assertThat(response.getStatus()).isEqualTo(200);
    Map<String, Object> body = (Map<String, Object>) response.getEntity();
    assertThat(body.get("metricType")).isEqualTo("commit");
    assertThat((List<?>) body.get("reports")).hasSize(1);
  }

  // ── bad requests ──────────────────────────────────────────────────────────

  @Test
  void invalidMetricTypeThrowsIllegalArgument() {
    // No persistence stub needed — invalid metricType is rejected before querying persistence.
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                service.listTableMetrics(
                    CATALOG,
                    NAMESPACE,
                    TABLE,
                    "bogus",
                    null,
                    10,
                    null,
                    null,
                    null,
                    null,
                    realmContext,
                    securityContext))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("bogus");
  }

  @Test
  void emptyNamespaceThrowsIllegalArgument() {
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                service.listTableMetrics(
                    CATALOG,
                    "",
                    TABLE,
                    "scan",
                    null,
                    10,
                    null,
                    null,
                    null,
                    null,
                    realmContext,
                    securityContext))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("namespace");
  }

  @Test
  void nullNamespaceThrowsIllegalArgument() {
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                service.listTableMetrics(
                    CATALOG,
                    null,
                    TABLE,
                    "scan",
                    null,
                    10,
                    null,
                    null,
                    null,
                    null,
                    realmContext,
                    securityContext))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ── namespace decoding ─────────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void singleLevelNamespaceDecoded() {
    when(persistence.listScanReports(anyLong(), anyLong(), any(), any(), any(), any(), any()))
        .thenReturn(Page.fromItems(List.of()));

    // Single-level namespace — no unit separator.
    Response response =
        service.listTableMetrics(
            CATALOG,
            "mydb",
            TABLE,
            "scan",
            null,
            10,
            null,
            null,
            null,
            null,
            realmContext,
            securityContext);

    // Reaches persistence → namespace was decoded correctly.
    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  @SuppressWarnings("unchecked")
  void multiLevelNamespaceWithUnitSeparatorDecoded() {
    when(persistence.listScanReports(anyLong(), anyLong(), any(), any(), any(), any(), any()))
        .thenReturn(Page.fromItems(List.of()));

    // Two-level namespace separated by unit separator (0x1F), same as NAMESPACE constant.
    Response response =
        service.listTableMetrics(
            CATALOG,
            "level1\u001Flevel2",
            TABLE,
            "scan",
            null,
            10,
            null,
            null,
            null,
            null,
            realmContext,
            securityContext);

    assertThat(response.getStatus()).isEqualTo(200);
  }

  // ── filter propagation ────────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void scanFiltersPropagateToPersistence() {
    long snapshotId = 99L;
    long tsFrom = 1_000L;
    long tsTo = 9_000L;
    String principal = "alice";

    when(persistence.listScanReports(
            eq(CATALOG_ID),
            eq(TABLE_ID),
            eq(snapshotId),
            eq(principal),
            eq(tsFrom),
            eq(tsTo),
            any(PageToken.class)))
        .thenReturn(Page.fromItems(List.of()));

    Response response =
        service.listTableMetrics(
            CATALOG,
            NAMESPACE,
            TABLE,
            "scan",
            null,
            10,
            snapshotId,
            principal,
            tsFrom,
            tsTo,
            realmContext,
            securityContext);

    assertThat(response.getStatus()).isEqualTo(200);
    verify(persistence)
        .listScanReports(
            eq(CATALOG_ID),
            eq(TABLE_ID),
            eq(snapshotId),
            eq(principal),
            eq(tsFrom),
            eq(tsTo),
            any(PageToken.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void commitFiltersPropagateToPersistence() {
    long snapshotId = 77L;
    long tsFrom = 500L;
    long tsTo = 5_000L;
    String principal = "bob";

    when(persistence.listCommitReports(
            eq(CATALOG_ID),
            eq(TABLE_ID),
            eq(snapshotId),
            eq(principal),
            eq(tsFrom),
            eq(tsTo),
            any(PageToken.class)))
        .thenReturn(Page.fromItems(List.of()));

    Response response =
        service.listTableMetrics(
            CATALOG,
            NAMESPACE,
            TABLE,
            "commit",
            null,
            10,
            snapshotId,
            principal,
            tsFrom,
            tsTo,
            realmContext,
            securityContext);

    assertThat(response.getStatus()).isEqualTo(200);
    verify(persistence)
        .listCommitReports(
            eq(CATALOG_ID),
            eq(TABLE_ID),
            eq(snapshotId),
            eq(principal),
            eq(tsFrom),
            eq(tsTo),
            any(PageToken.class));
  }

  // ── wrong token type → 400 ────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void wrongPageTokenTypeScanPropagatesIllegalArgument() {
    // Persistence throws IllegalArgumentException when the page token carries a cursor of the
    // wrong type (e.g. EntityIdToken recycled from a different list operation).
    // IcebergExceptionMapper maps IllegalArgumentException to HTTP 400.
    when(persistence.listScanReports(anyLong(), anyLong(), any(), any(), any(), any(), any()))
        .thenThrow(
            new IllegalArgumentException(
                "pageToken contains a cursor of an unexpected type; expected MetricsReportToken"));

    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                service.listTableMetrics(
                    CATALOG,
                    NAMESPACE,
                    TABLE,
                    "scan",
                    null,
                    10,
                    null,
                    null,
                    null,
                    null,
                    realmContext,
                    securityContext))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unexpected type");
  }

  @Test
  void wrongPageTokenTypeCommitPropagatesIllegalArgument() {
    when(persistence.listCommitReports(anyLong(), anyLong(), any(), any(), any(), any(), any()))
        .thenThrow(
            new IllegalArgumentException(
                "pageToken contains a cursor of an unexpected type; expected MetricsReportToken"));

    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                service.listTableMetrics(
                    CATALOG,
                    NAMESPACE,
                    TABLE,
                    "commit",
                    null,
                    10,
                    null,
                    null,
                    null,
                    null,
                    realmContext,
                    securityContext))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unexpected type");
  }

  // ── envelope shape ───────────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void scanReportHasEnvelopeStructure() {
    ScanMetricsRecord r = scanRecord("r-envelope");
    when(persistence.listScanReports(anyLong(), anyLong(), any(), any(), any(), any(), any()))
        .thenReturn(Page.fromItems(List.of(r)));

    Response response =
        service.listTableMetrics(
            CATALOG,
            NAMESPACE,
            TABLE,
            "scan",
            null,
            10,
            null,
            null,
            null,
            null,
            realmContext,
            securityContext);

    assertThat(response.getStatus()).isEqualTo(200);
    Map<String, Object> body = (Map<String, Object>) response.getEntity();
    Map<String, Object> report = (Map<String, Object>) ((List<?>) body.get("reports")).get(0);

    assertThat(report.get("id")).isEqualTo("r-envelope");
    assertThat(report).containsKey("timestampMs");
    assertThat(report.get("actor")).isInstanceOf(Map.class);
    assertThat(((Map<String, Object>) report.get("actor")).get("principalName")).isEqualTo("alice");
    assertThat(report.get("request")).isInstanceOf(Map.class);
    assertThat(((Map<String, Object>) report.get("request")).get("requestId")).isEqualTo("req-1");
    assertThat(report.get("object")).isInstanceOf(Map.class);
    assertThat(report.get("payload")).isInstanceOf(Map.class);
    Map<String, Object> payload = (Map<String, Object>) report.get("payload");
    assertThat(payload.get("type")).isEqualTo("iceberg.metrics.scan");
    assertThat(payload.get("version")).isEqualTo(1);
    assertThat(payload.get("data")).isInstanceOf(Map.class);
    // Flat fields must NOT appear at the top level
    assertThat(report).doesNotContainKey("reportId");
    assertThat(report).doesNotContainKey("principalName");
    assertThat(report).doesNotContainKey("resultDataFiles");
  }

  @Test
  @SuppressWarnings("unchecked")
  void commitReportHasEnvelopeStructure() {
    CommitMetricsRecord r = commitRecord("c-envelope");
    when(persistence.listCommitReports(anyLong(), anyLong(), any(), any(), any(), any(), any()))
        .thenReturn(Page.fromItems(List.of(r)));

    Response response =
        service.listTableMetrics(
            CATALOG,
            NAMESPACE,
            TABLE,
            "commit",
            null,
            10,
            null,
            null,
            null,
            null,
            realmContext,
            securityContext);

    assertThat(response.getStatus()).isEqualTo(200);
    Map<String, Object> body = (Map<String, Object>) response.getEntity();
    Map<String, Object> report = (Map<String, Object>) ((List<?>) body.get("reports")).get(0);

    assertThat(report.get("id")).isEqualTo("c-envelope");
    assertThat(report.get("actor")).isInstanceOf(Map.class);
    assertThat(report.get("request")).isInstanceOf(Map.class);
    Map<String, Object> object = (Map<String, Object>) report.get("object");
    assertThat(object.get("snapshotId")).isEqualTo(42L);
    Map<String, Object> payload = (Map<String, Object>) report.get("payload");
    assertThat(payload.get("type")).isEqualTo("iceberg.metrics.commit");
    assertThat(payload.get("version")).isEqualTo(1);
    Map<String, Object> data = (Map<String, Object>) payload.get("data");
    assertThat(data.get("operation")).isEqualTo("append");
    // Flat fields must NOT appear at the top level
    assertThat(report).doesNotContainKey("reportId");
    assertThat(report).doesNotContainKey("snapshotId");
    assertThat(report).doesNotContainKey("operation");
  }

  // ── authorization (403) ───────────────────────────────────────────────────

  @Test
  void unauthorizedPrincipalPropagatesForbidden() {
    // Configure persistence so resolution succeeds, but the authorizer denies.
    when(persistence.listScanReports(anyLong(), anyLong(), any(), any(), any(), any(), any()))
        .thenReturn(Page.fromItems(List.of()));
    doThrow(new ForbiddenException("insufficient privileges"))
        .when(authorizer)
        .authorizeOrThrow(
            any(PolarisPrincipal.class),
            any(Set.class),
            any(PolarisAuthorizableOperation.class),
            any(),
            (org.apache.polaris.core.persistence.PolarisResolvedPathWrapper) isNull());

    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                service.listTableMetrics(
                    CATALOG,
                    NAMESPACE,
                    TABLE,
                    "scan",
                    null,
                    10,
                    null,
                    null,
                    null,
                    null,
                    realmContext,
                    securityContext))
        .isInstanceOf(ForbiddenException.class);
  }

  // ── not found (404) ───────────────────────────────────────────────────────

  @Test
  void tableNotFoundPropagatesNotFoundException() {
    // Manifest resolves but getResolvedPath returns null (table not found).
    when(manifest.getResolvedPath(
            any(ResolvedPathKey.class), eq(PolarisEntitySubType.ANY_SUBTYPE), eq(true)))
        .thenReturn(null);

    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                service.listTableMetrics(
                    CATALOG,
                    NAMESPACE,
                    TABLE,
                    "scan",
                    null,
                    10,
                    null,
                    null,
                    null,
                    null,
                    realmContext,
                    securityContext))
        .isInstanceOf(org.apache.iceberg.exceptions.NotFoundException.class)
        .hasMessageContaining(TABLE);
  }

  @Test
  void catalogNotFoundPropagatesNotFoundException() {
    // Top-level entity (catalog) could not be resolved.
    when(manifest.resolveAll()).thenReturn(new ResolverStatus(PolarisEntityType.CATALOG, CATALOG));

    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                service.listTableMetrics(
                    CATALOG,
                    NAMESPACE,
                    TABLE,
                    "scan",
                    null,
                    10,
                    null,
                    null,
                    null,
                    null,
                    realmContext,
                    securityContext))
        .isInstanceOf(org.apache.iceberg.exceptions.NotFoundException.class)
        .hasMessageContaining(CATALOG);
  }

  @Test
  void namespaceOrTablePathNotFoundPropagatesNotFoundException() {
    // PATH_COULD_NOT_BE_FULLY_RESOLVED — namespace or table segment not found.
    ResolverPath failedPath = new ResolverPath(List.of(NAMESPACE), PolarisEntityType.NAMESPACE);
    when(manifest.resolveAll()).thenReturn(new ResolverStatus(failedPath, 0));

    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                service.listTableMetrics(
                    CATALOG,
                    NAMESPACE,
                    TABLE,
                    "scan",
                    null,
                    10,
                    null,
                    null,
                    null,
                    null,
                    realmContext,
                    securityContext))
        .isInstanceOf(org.apache.iceberg.exceptions.NotFoundException.class);
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private static ScanMetricsRecord scanRecord(String reportId) {
    return ScanMetricsRecord.builder()
        .reportId(reportId)
        .catalogId(CATALOG_ID)
        .tableId(TABLE_ID)
        .timestamp(Instant.ofEpochMilli(1_000_000L))
        .metadata(Map.of())
        .principalName("alice")
        .requestId("req-1")
        .otelTraceId(null)
        .otelSpanId(null)
        .snapshotId(Optional.empty())
        .schemaId(Optional.empty())
        .filterExpression(Optional.empty())
        .projectedFieldIds(List.of())
        .projectedFieldNames(List.of())
        .resultDataFiles(1L)
        .resultDeleteFiles(0L)
        .totalFileSizeBytes(1024L)
        .totalDataManifests(1L)
        .totalDeleteManifests(0L)
        .scannedDataManifests(1L)
        .scannedDeleteManifests(0L)
        .skippedDataManifests(0L)
        .skippedDeleteManifests(0L)
        .skippedDataFiles(0L)
        .skippedDeleteFiles(0L)
        .totalPlanningDurationMs(50L)
        .equalityDeleteFiles(0L)
        .positionalDeleteFiles(0L)
        .indexedDeleteFiles(0L)
        .totalDeleteFileSizeBytes(0L)
        .build();
  }

  private static CommitMetricsRecord commitRecord(String reportId) {
    return CommitMetricsRecord.builder()
        .reportId(reportId)
        .catalogId(CATALOG_ID)
        .tableId(TABLE_ID)
        .timestamp(Instant.ofEpochMilli(2_000_000L))
        .metadata(Map.of())
        .principalName("bob")
        .requestId("req-2")
        .otelTraceId(null)
        .otelSpanId(null)
        .snapshotId(42L)
        .sequenceNumber(Optional.empty())
        .operation("append")
        .addedDataFiles(1L)
        .removedDataFiles(0L)
        .totalDataFiles(10L)
        .addedDeleteFiles(0L)
        .removedDeleteFiles(0L)
        .totalDeleteFiles(0L)
        .addedEqualityDeleteFiles(0L)
        .removedEqualityDeleteFiles(0L)
        .addedPositionalDeleteFiles(0L)
        .removedPositionalDeleteFiles(0L)
        .addedRecords(100L)
        .removedRecords(0L)
        .totalRecords(1000L)
        .addedFileSizeBytes(2048L)
        .removedFileSizeBytes(0L)
        .totalFileSizeBytes(20480L)
        .totalDurationMs(Optional.empty())
        .attempts(1)
        .build();
  }
}
