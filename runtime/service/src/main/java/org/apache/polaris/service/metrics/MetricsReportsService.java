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

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.metrics.api.model.CommitMetricsObject;
import org.apache.polaris.core.metrics.api.model.CommitMetricsReport;
import org.apache.polaris.core.metrics.api.model.CommitPayload;
import org.apache.polaris.core.metrics.api.model.CommitPayloadData;
import org.apache.polaris.core.metrics.api.model.ListCommitMetricsResponse;
import org.apache.polaris.core.metrics.api.model.ListScanMetricsResponse;
import org.apache.polaris.core.metrics.api.model.MetricsActor;
import org.apache.polaris.core.metrics.api.model.MetricsRequest;
import org.apache.polaris.core.metrics.api.model.QueryMetricsRequest;
import org.apache.polaris.core.metrics.api.model.ScanMetricsObject;
import org.apache.polaris.core.metrics.api.model.ScanMetricsReport;
import org.apache.polaris.core.metrics.api.model.ScanPayload;
import org.apache.polaris.core.metrics.api.model.ScanPayloadData;
import org.apache.polaris.core.metrics.api.model.TableRef;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.MetricsRecordIdentity;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.extension.metrics.spi.MetricsQuerySpi;
import org.apache.polaris.service.metrics.api.PolarisCatalogsApiService;
import org.jspecify.annotations.NonNull;

/**
 * Service implementation for the Metrics Reports API.
 *
 * <p>Resolves catalog/namespace/table names to internal IDs, performs authorization, and delegates
 * durable reads to {@link MetricsQuerySpi} when an implementation is available.
 */
@Beta
@RequestScoped
public class MetricsReportsService implements PolarisCatalogsApiService {

  private final PolarisAuthorizer authorizer;
  private final PolarisPrincipal polarisPrincipal;
  private final ResolutionManifestFactory resolutionManifestFactory;
  private final Instance<MetricsQuerySpi> queryProvider;

  @Inject
  public MetricsReportsService(
      @NonNull PolarisAuthorizer authorizer,
      @NonNull PolarisPrincipal polarisPrincipal,
      @NonNull ResolutionManifestFactory resolutionManifestFactory,
      @Any Instance<MetricsQuerySpi> queryProvider) {
    this.authorizer = authorizer;
    this.polarisPrincipal = polarisPrincipal;
    this.resolutionManifestFactory = resolutionManifestFactory;
    this.queryProvider = queryProvider;
  }

  @Override
  public Response queryTableMetrics(
      String catalogName,
      QueryMetricsRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {

    List<TableRef> tableRefs = request.getTables();
    if (tableRefs == null || tableRefs.isEmpty()) {
      throw new IllegalArgumentException("tables must not be empty");
    }

    List<TableIdentifier> identifiers =
        tableRefs.stream()
            .map(
                ref ->
                    TableIdentifier.of(
                        Namespace.of(ref.getNamespace().toArray(new String[0])), ref.getName()))
            .toList();

    PolarisResolutionManifest manifest = resolveAndAuthorizeTableMetrics(catalogName, identifiers);

    CatalogEntity catalogEntity = manifest.getResolvedCatalogEntity();
    Preconditions.checkNotNull(catalogEntity, "No catalog available");
    long catalogId = catalogEntity.getId();

    List<Long> tableIds = new ArrayList<>(identifiers.size());
    Map<Long, TableIdentifier> tableIdToIdentifier = new LinkedHashMap<>();
    for (TableIdentifier identifier : identifiers) {
      PolarisResolvedPathWrapper tableWrapper =
          manifest.getResolvedPath(
              ResolvedPathKey.ofTableLike(identifier), PolarisEntitySubType.ANY_SUBTYPE, true);
      long tableId = tableWrapper.getRawLeafEntity().getId();
      tableIds.add(tableId);
      tableIdToIdentifier.put(tableId, identifier);
    }

    MetricsQuerySpi.MetricType type = parseMetricType(request.getMetricType().toString());
    PageToken pt = PageToken.build(request.getPageToken(), request.getPageSize(), () -> true);
    MetricsQuerySpi provider = queryProvider.get();

    Page<? extends MetricsRecordIdentity> page =
        provider.listReports(
            type,
            catalogId,
            tableIds,
            request.getSnapshotId(),
            request.getTimestampFrom(),
            request.getTimestampTo(),
            pt);

    if (type == MetricsQuerySpi.MetricType.COMMIT) {
      List<CommitMetricsReport> reports =
          page.items().stream()
              .map(
                  r ->
                      toCommitReport((CommitMetricsRecord) r, tableIdToIdentifier.get(r.tableId())))
              .toList();
      return Response.ok(
              new ListCommitMetricsResponse(
                  page.encodedResponseToken(),
                  ListCommitMetricsResponse.MetricTypeEnum.COMMIT,
                  reports))
          .build();
    }

    List<ScanMetricsReport> reports =
        page.items().stream()
            .map(r -> toScanReport((ScanMetricsRecord) r, tableIdToIdentifier.get(r.tableId())))
            .toList();
    return Response.ok(
            new ListScanMetricsResponse(
                page.encodedResponseToken(), ListScanMetricsResponse.MetricTypeEnum.SCAN, reports))
        .build();
  }

  private static MetricsQuerySpi.MetricType parseMetricType(String metricType) {
    if ("scan".equalsIgnoreCase(metricType)) {
      return MetricsQuerySpi.MetricType.SCAN;
    }
    if ("commit".equalsIgnoreCase(metricType)) {
      return MetricsQuerySpi.MetricType.COMMIT;
    }
    throw new IllegalArgumentException(
        "metricType must be one of [scan, commit], got: " + metricType);
  }

  private PolarisResolutionManifest resolveAndAuthorizeTableMetrics(
      String catalogName, List<TableIdentifier> identifiers) {
    PolarisResolutionManifest manifest =
        resolutionManifestFactory.createResolutionManifest(polarisPrincipal, catalogName);
    for (TableIdentifier identifier : identifiers) {
      manifest.addPassthroughPath(
          new ResolverPath(
              Arrays.asList(identifier.namespace().levels()), PolarisEntityType.NAMESPACE));
      manifest.addPassthroughPath(
          new ResolverPath(
              PolarisCatalogHelpers.tableIdentifierToList(identifier),
              PolarisEntityType.TABLE_LIKE));
    }
    ResolverStatus status = manifest.resolveAll();

    if (status.getStatus() == ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED) {
      throw new NotFoundException(
          "TopLevelEntity of type %s does not exist: %s",
          status.getFailedToResolvedEntityType(), status.getFailedToResolvedEntityName());
    }
    if (status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED) {
      throw new NotFoundException("Table not found");
    }

    for (TableIdentifier identifier : identifiers) {
      PolarisResolvedPathWrapper tableWrapper =
          manifest.getResolvedPath(
              ResolvedPathKey.ofTableLike(identifier), PolarisEntitySubType.ANY_SUBTYPE, true);

      if (tableWrapper == null) {
        throw new NotFoundException("Table not found: %s", identifier);
      }

      authorizer.authorizeOrThrow(
          polarisPrincipal,
          manifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
          PolarisAuthorizableOperation.LIST_TABLE_METRICS,
          tableWrapper,
          null);
    }

    return manifest;
  }

  private static ScanMetricsReport toScanReport(ScanMetricsRecord r, TableIdentifier identifier) {
    MetricsActor actor = r.principalName() != null ? new MetricsActor(r.principalName()) : null;
    MetricsRequest request =
        (r.requestId() != null || r.otelTraceId() != null || r.otelSpanId() != null)
            ? new MetricsRequest(r.requestId(), r.otelTraceId(), r.otelSpanId())
            : null;
    ScanMetricsObject object =
        new ScanMetricsObject(toTableRef(identifier), r.snapshotId().orElse(null));
    ScanPayloadData data =
        ScanPayloadData.builder()
            .setSchemaId(r.schemaId().orElse(null))
            .setFilterExpression(r.filterExpression().orElse(null))
            .setProjectedFieldIds(r.projectedFieldIds())
            .setProjectedFieldNames(r.projectedFieldNames())
            .setResultDataFiles(r.resultDataFiles())
            .setResultDeleteFiles(r.resultDeleteFiles())
            .setTotalFileSizeBytes(r.totalFileSizeBytes())
            .setTotalDataManifests(r.totalDataManifests())
            .setTotalDeleteManifests(r.totalDeleteManifests())
            .setScannedDataManifests(r.scannedDataManifests())
            .setScannedDeleteManifests(r.scannedDeleteManifests())
            .setSkippedDataManifests(r.skippedDataManifests())
            .setSkippedDeleteManifests(r.skippedDeleteManifests())
            .setSkippedDataFiles(r.skippedDataFiles())
            .setSkippedDeleteFiles(r.skippedDeleteFiles())
            .setTotalPlanningDurationMs(r.totalPlanningDurationMs())
            .setEqualityDeleteFiles(r.equalityDeleteFiles())
            .setPositionalDeleteFiles(r.positionalDeleteFiles())
            .setIndexedDeleteFiles(r.indexedDeleteFiles())
            .setTotalDeleteFileSizeBytes(r.totalDeleteFileSizeBytes())
            .build();
    ScanPayload payload =
        new ScanPayload(
            ScanPayload.TypeEnum.ICEBERG_METRICS_SCAN, ScanPayload.VersionEnum.NUMBER_1, data);
    return new ScanMetricsReport(
        r.reportId(), r.timestamp().toEpochMilli(), actor, request, object, payload);
  }

  private static CommitMetricsReport toCommitReport(
      CommitMetricsRecord r, TableIdentifier identifier) {
    MetricsActor actor = r.principalName() != null ? new MetricsActor(r.principalName()) : null;
    MetricsRequest request =
        (r.requestId() != null || r.otelTraceId() != null || r.otelSpanId() != null)
            ? new MetricsRequest(r.requestId(), r.otelTraceId(), r.otelSpanId())
            : null;
    CommitMetricsObject object = new CommitMetricsObject(toTableRef(identifier), r.snapshotId());
    CommitPayloadData data =
        CommitPayloadData.builder()
            .setSequenceNumber(r.sequenceNumber().orElse(null))
            .setOperation(r.operation())
            .setAddedDataFiles(r.addedDataFiles())
            .setRemovedDataFiles(r.removedDataFiles())
            .setTotalDataFiles(r.totalDataFiles())
            .setAddedDeleteFiles(r.addedDeleteFiles())
            .setRemovedDeleteFiles(r.removedDeleteFiles())
            .setTotalDeleteFiles(r.totalDeleteFiles())
            .setAddedEqualityDeleteFiles(r.addedEqualityDeleteFiles())
            .setRemovedEqualityDeleteFiles(r.removedEqualityDeleteFiles())
            .setAddedPositionalDeleteFiles(r.addedPositionalDeleteFiles())
            .setRemovedPositionalDeleteFiles(r.removedPositionalDeleteFiles())
            .setAddedRecords(r.addedRecords())
            .setRemovedRecords(r.removedRecords())
            .setTotalRecords(r.totalRecords())
            .setAddedFileSizeBytes(r.addedFileSizeBytes())
            .setRemovedFileSizeBytes(r.removedFileSizeBytes())
            .setTotalFileSizeBytes(r.totalFileSizeBytes())
            .setTotalDurationMs(r.totalDurationMs().orElse(null))
            .setAttempts(r.attempts())
            .build();
    CommitPayload payload =
        new CommitPayload(
            CommitPayload.TypeEnum.ICEBERG_METRICS_COMMIT,
            CommitPayload.VersionEnum.NUMBER_1,
            data);
    return new CommitMetricsReport(
        r.reportId(), r.timestamp().toEpochMilli(), actor, request, object, payload);
  }

  private static TableRef toTableRef(TableIdentifier identifier) {
    return new TableRef(Arrays.asList(identifier.namespace().levels()), identifier.name());
  }
}
