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
package org.apache.polaris.service.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.metrics.api.model.ListMetricsResponse;
import org.apache.polaris.core.metrics.api.model.MetricsReport;
import org.apache.polaris.core.metrics.api.model.QueryMetricsRequest;
import org.apache.polaris.core.metrics.api.model.TableRef;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.extension.metrics.spi.MetricsQuerySpi;
import org.apache.polaris.service.catalog.DefaultCatalogPrefixParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MetricsReportsService}.
 *
 * <p>Without a durable query backend the no-op default provider returns empty pages; the durable
 * JDBC implementation (#4756) returns real data. These tests cover authorization, resolution error
 * paths, input validation, and mapping of persisted records to response payloads.
 */
class MetricsReportsServiceTest {

  private static final String CATALOG = "test-catalog";
  private static final List<String> NAMESPACE = List.of("db", "schema");
  private static final String TABLE = "events";

  private PolarisAuthorizer authorizer;
  private PolarisResolutionManifest manifest;
  private PolarisPrincipal principal;
  private ResolutionManifestFactory factory;
  private MetricsReportsService service;
  private RealmContext realmContext;
  private SecurityContext securityContext;
  private Instance<MetricsQuerySpi> queryProvider;

  @BeforeEach
  void setUp() {
    authorizer = mock(PolarisAuthorizer.class);
    principal = mock(PolarisPrincipal.class);

    PolarisResolvedPathWrapper tableWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisEntity leafEntity = mock(PolarisEntity.class);
    when(leafEntity.getId()).thenReturn(42L);
    when(tableWrapper.getRawLeafEntity()).thenReturn(leafEntity);
    manifest = mock(PolarisResolutionManifest.class);
    factory = mock(ResolutionManifestFactory.class);
    realmContext = mock(RealmContext.class);
    securityContext = mock(SecurityContext.class);

    CatalogEntity catalogEntity = mock(CatalogEntity.class);
    when(catalogEntity.getId()).thenReturn(7L);

    when(manifest.resolveAll()).thenReturn(new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS));
    when(manifest.getResolvedCatalogEntity()).thenReturn(catalogEntity);
    when(manifest.getResolvedPath(
            any(ResolvedPathKey.class), eq(PolarisEntitySubType.ICEBERG_TABLE), eq(true)))
        .thenReturn(tableWrapper);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles()).thenReturn(Set.of());
    when(factory.createResolutionManifest(eq(principal), eq(CATALOG))).thenReturn(manifest);
    when(authorizer.authorize(any(), any())).thenReturn(AuthorizationDecision.allow());

    // By default the no-op query provider is active (durable backend absent) and returns
    // empty pages, mirroring the @DefaultBean NoOpMetricsQuery in
    // polaris-extensions-metrics-reports.
    MetricsQuerySpi noOp = mock(MetricsQuerySpi.class);
    when(noOp.listReports(
            eq(MetricsQuerySpi.MetricType.SCAN),
            anyLong(),
            any(),
            any(),
            any(),
            any(),
            any(PageToken.class)))
        .thenReturn(new MetricsQuerySpi.ScanResult(Page.fromItems(List.of())));
    when(noOp.listReports(
            eq(MetricsQuerySpi.MetricType.COMMIT),
            anyLong(),
            any(),
            any(),
            any(),
            any(),
            any(PageToken.class)))
        .thenReturn(new MetricsQuerySpi.CommitResult(Page.fromItems(List.of())));
    @SuppressWarnings("unchecked")
    Instance<MetricsQuerySpi> noOpProvider = mock(Instance.class);
    when(noOpProvider.get()).thenReturn(noOp);
    queryProvider = noOpProvider;

    service =
        new MetricsReportsService(
            authorizer, principal, factory, queryProvider, new DefaultCatalogPrefixParser());
    realmContext = mock(RealmContext.class);
    securityContext = mock(SecurityContext.class);
  }

  private static QueryMetricsRequest requestFor(
      String metricType, List<String> namespace, String table) {
    return new QueryMetricsRequest(
        "scan".equals(metricType)
            ? QueryMetricsRequest.MetricTypeEnum.SCAN
            : QueryMetricsRequest.MetricTypeEnum.COMMIT,
        List.of(new TableRef(namespace, table)));
  }

  @Test
  void authorizedRequestWithNoBackendReturnsEmptyPage() {
    Response response =
        service.queryTableMetrics(
            CATALOG, requestFor("scan", NAMESPACE, TABLE), realmContext, securityContext);

    // With the no-op default query provider, the read path succeeds with an empty result set.
    assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
  }

  @Test
  void unauthorizedRequestThrowsForbiddenException() {
    when(authorizer.authorize(any(), any())).thenReturn(AuthorizationDecision.deny("denied"));

    assertThatThrownBy(
            () ->
                service.queryTableMetrics(
                    CATALOG, requestFor("scan", NAMESPACE, TABLE), realmContext, securityContext))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  void tableNotFoundThrowsNotFoundException() {
    when(manifest.getResolvedPath(
            any(ResolvedPathKey.class), eq(PolarisEntitySubType.ICEBERG_TABLE), eq(true)))
        .thenReturn(null);

    assertThatThrownBy(
            () ->
                service.queryTableMetrics(
                    CATALOG, requestFor("scan", NAMESPACE, TABLE), realmContext, securityContext))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining(TABLE);
  }

  @Test
  void catalogNotFoundPropagatesNotFoundException() {
    when(manifest.resolveAll()).thenReturn(new ResolverStatus(PolarisEntityType.CATALOG, CATALOG));

    assertThatThrownBy(
            () ->
                service.queryTableMetrics(
                    CATALOG, requestFor("scan", NAMESPACE, TABLE), realmContext, securityContext))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining(CATALOG);
  }

  @Test
  void pathNotFoundPropagatesNotFoundException() {
    ResolverPath failedPath = new ResolverPath(NAMESPACE, PolarisEntityType.NAMESPACE);
    when(manifest.resolveAll()).thenReturn(new ResolverStatus(failedPath, 0));

    assertThatThrownBy(
            () ->
                service.queryTableMetrics(
                    CATALOG, requestFor("scan", NAMESPACE, TABLE), realmContext, securityContext))
        .isInstanceOf(NotFoundException.class);
  }

  @Test
  void emptyTablesThrowsIllegalArgumentException() {
    QueryMetricsRequest request =
        new QueryMetricsRequest(QueryMetricsRequest.MetricTypeEnum.SCAN, List.of());

    assertThatThrownBy(
            () -> service.queryTableMetrics(CATALOG, request, realmContext, securityContext))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("tables");
  }

  @Test
  void snapshotIdWithMultipleTablesThrowsIllegalArgumentException() {
    QueryMetricsRequest request =
        new QueryMetricsRequest(
            QueryMetricsRequest.MetricTypeEnum.SCAN,
            List.of(new TableRef(NAMESPACE, TABLE), new TableRef(NAMESPACE, "other-table")),
            null,
            null,
            123L,
            null,
            null);

    assertThatThrownBy(
            () -> service.queryTableMetrics(CATALOG, request, realmContext, securityContext))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("snapshotId");
  }

  @Test
  void multiTableRequestQueriesAllResolvedTableIds() {
    Response response =
        service.queryTableMetrics(
            CATALOG,
            new QueryMetricsRequest(
                QueryMetricsRequest.MetricTypeEnum.SCAN,
                List.of(new TableRef(NAMESPACE, TABLE), new TableRef(NAMESPACE, "other-table"))),
            realmContext,
            securityContext);

    assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
  }

  @Test
  void populatedScanResultIsReturnedAsReconstructedPayload() {
    ScanMetricsRecord record =
        ScanMetricsRecord.builder()
            .reportId("scan-1")
            .catalogId(7L)
            .tableId(42L)
            .timestamp(Instant.ofEpochMilli(1_000L))
            .putMetadata("custom-key", "custom-value")
            .principalName("alice")
            .requestId("req-1")
            .snapshotId(99L)
            .schemaId(3)
            .filterExpression("id > 5")
            .projectedFieldIds(List.of(1, 2))
            .projectedFieldNames(List.of("id", "name"))
            .resultDataFiles(10L)
            .resultDeleteFiles(0L)
            .totalFileSizeBytes(2048L)
            .totalDataManifests(2L)
            .totalDeleteManifests(0L)
            .scannedDataManifests(2L)
            .scannedDeleteManifests(0L)
            .skippedDataManifests(0L)
            .skippedDeleteManifests(0L)
            .skippedDataFiles(1L)
            .skippedDeleteFiles(0L)
            .totalPlanningDurationMs(15L)
            .equalityDeleteFiles(0L)
            .positionalDeleteFiles(0L)
            .indexedDeleteFiles(0L)
            .totalDeleteFileSizeBytes(0L)
            .build();
    MetricsQuerySpi backend = mock(MetricsQuerySpi.class);
    when(backend.listReports(
            eq(MetricsQuerySpi.MetricType.SCAN),
            eq(7L),
            eq(List.of(42L)),
            any(),
            any(),
            any(),
            any(PageToken.class)))
        .thenReturn(new MetricsQuerySpi.ScanResult(Page.fromItems(List.of(record))));
    when(queryProvider.get()).thenReturn(backend);

    Response response =
        service.queryTableMetrics(
            CATALOG, requestFor("scan", NAMESPACE, TABLE), realmContext, securityContext);

    assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    List<MetricsReport> reports = ((ListMetricsResponse) response.getEntity()).getReports();
    assertThat(reports).hasSize(1);
    MetricsReport report = reports.getFirst();
    assertThat(report.getMetricType()).isEqualTo(MetricsReport.MetricTypeEnum.SCAN);
    assertThat(report.getTable()).isEqualTo(new TableRef(NAMESPACE, TABLE));
    assertThat(report.getTimestampMs()).isEqualTo(1_000L);
    assertThat(report.getSnapshotId()).isEqualTo(99L);
    assertThat(report.getActor().getPrincipalName()).isEqualTo("alice");
    assertThat(report.getRequest().getRequestId()).isEqualTo("req-1");
    assertThat(report.getPayload())
        .containsEntry("type", "iceberg.metrics.scan")
        .containsEntry("version", 1);
    @SuppressWarnings("unchecked")
    Map<String, Object> data = (Map<String, Object>) report.getPayload().get("data");
    assertThat(data)
        .containsEntry("schema-id", 3)
        .containsEntry("filter", "id > 5")
        .containsEntry("projected-field-names", List.of("id", "name"))
        .containsEntry("result-data-files", 10L)
        .containsEntry("total-planning-duration-ms", 15L);
    // The payload is a projection of the persisted record: record metadata is not included.
    assertThat(data).doesNotContainKey("custom-key");
    assertThat(report.getPayload()).containsOnlyKeys("type", "version", "data");
  }

  @Test
  void populatedCommitResultIsReturnedAsReconstructedPayload() {
    CommitMetricsRecord record =
        CommitMetricsRecord.builder()
            .reportId("commit-1")
            .catalogId(7L)
            .tableId(42L)
            .timestamp(Instant.ofEpochMilli(2_000L))
            .snapshotId(100L)
            .sequenceNumber(5L)
            .operation("append")
            .addedDataFiles(3L)
            .removedDataFiles(0L)
            .totalDataFiles(13L)
            .addedDeleteFiles(0L)
            .removedDeleteFiles(0L)
            .totalDeleteFiles(0L)
            .addedEqualityDeleteFiles(0L)
            .removedEqualityDeleteFiles(0L)
            .addedPositionalDeleteFiles(0L)
            .removedPositionalDeleteFiles(0L)
            .addedRecords(300L)
            .removedRecords(0L)
            .totalRecords(1300L)
            .addedFileSizeBytes(4096L)
            .removedFileSizeBytes(0L)
            .totalFileSizeBytes(40960L)
            .totalDurationMs(25L)
            .attempts(1)
            .build();
    MetricsQuerySpi backend = mock(MetricsQuerySpi.class);
    when(backend.listReports(
            eq(MetricsQuerySpi.MetricType.COMMIT),
            eq(7L),
            eq(List.of(42L)),
            any(),
            any(),
            any(),
            any(PageToken.class)))
        .thenReturn(new MetricsQuerySpi.CommitResult(Page.fromItems(List.of(record))));
    when(queryProvider.get()).thenReturn(backend);

    Response response =
        service.queryTableMetrics(
            CATALOG, requestFor("commit", NAMESPACE, TABLE), realmContext, securityContext);

    assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    List<MetricsReport> reports = ((ListMetricsResponse) response.getEntity()).getReports();
    assertThat(reports).hasSize(1);
    MetricsReport report = reports.getFirst();
    assertThat(report.getMetricType()).isEqualTo(MetricsReport.MetricTypeEnum.COMMIT);
    assertThat(report.getTimestampMs()).isEqualTo(2_000L);
    assertThat(report.getSnapshotId()).isEqualTo(100L);
    assertThat(report.getActor()).isNull();
    assertThat(report.getRequest()).isNull();
    assertThat(report.getPayload()).containsEntry("type", "iceberg.metrics.commit");
    @SuppressWarnings("unchecked")
    Map<String, Object> data = (Map<String, Object>) report.getPayload().get("data");
    assertThat(data)
        .containsEntry("sequence-number", 5L)
        .containsEntry("operation", "append")
        .containsEntry("added-records", 300L)
        .containsEntry("total-duration-ms", 25L)
        .containsEntry("attempts", 1);
  }

  @Test
  void multiLevelNamespaceIsPassedThrough() {
    Response response =
        service.queryTableMetrics(
            CATALOG,
            requestFor("scan", List.of("db", "schema"), TABLE),
            realmContext,
            securityContext);

    assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
  }
}
