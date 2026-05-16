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

import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
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
import org.apache.polaris.service.metrics.api.PolarisCatalogsApiService;

/**
 * Service implementation for the Metrics Reports API.
 *
 * <p>Resolves catalog/namespace/table names to internal IDs, performs authorization, and delegates
 * to the {@link org.apache.polaris.core.persistence.metrics.MetricsPersistence} layer to retrieve
 * persisted metrics reports.
 */
@RequestScoped
public class MetricsReportsService implements PolarisCatalogsApiService {

  private final CallContext callContext;
  private final PolarisMetaStoreManager metaStoreManager;
  private final PolarisAuthorizer authorizer;
  private final PolarisPrincipal polarisPrincipal;
  private final ResolutionManifestFactory resolutionManifestFactory;

  @Inject
  public MetricsReportsService(
      @Nonnull CallContext callContext,
      @Nonnull PolarisMetaStoreManager metaStoreManager,
      @Nonnull PolarisAuthorizer authorizer,
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull ResolutionManifestFactory resolutionManifestFactory) {
    this.callContext = callContext;
    this.metaStoreManager = metaStoreManager;
    this.authorizer = authorizer;
    this.polarisPrincipal = polarisPrincipal;
    this.resolutionManifestFactory = resolutionManifestFactory;
  }

  @Override
  public Response listTableMetrics(
      String catalogName,
      String namespace,
      String table,
      String metricType,
      String pageToken,
      Integer pageSize,
      Long snapshotId,
      String principalName,
      Long timestampFrom,
      Long timestampTo,
      RealmContext realmContext,
      SecurityContext securityContext) {

    Namespace ns = decodeNamespace(namespace);
    TableIdentifier identifier = TableIdentifier.of(ns, table);

    // Resolve and authorize
    PolarisResolvedPathWrapper tableWrapper =
        resolveAndAuthorizeTableMetrics(catalogName, identifier);

    PolarisEntity tableEntity = tableWrapper.getResolvedLeafEntity().getEntity();
    long catalogId = tableEntity.getCatalogId();
    long tableId = tableEntity.getId();

    PageToken token = PageToken.build(pageToken, pageSize, () -> true);

    return switch (metricType) {
      case "scan" -> {
        Page<ScanMetricsRecord> page =
            metaStoreManager.listScanMetrics(
                callContext.getPolarisCallContext(),
                catalogId,
                tableId,
                snapshotId,
                principalName,
                timestampFrom,
                timestampTo,
                token);
        yield Response.ok(buildScanResponse(page)).build();
      }
      case "commit" -> {
        Page<CommitMetricsRecord> page =
            metaStoreManager.listCommitMetrics(
                callContext.getPolarisCallContext(),
                catalogId,
                tableId,
                snapshotId,
                principalName,
                timestampFrom,
                timestampTo,
                token);
        yield Response.ok(buildCommitResponse(page)).build();
      }
      default ->
          throw new IllegalArgumentException(
              "Invalid metricType: " + metricType + "; must be 'scan' or 'commit'");
    };
  }

  private PolarisResolvedPathWrapper resolveAndAuthorizeTableMetrics(
      String catalogName, TableIdentifier identifier) {
    PolarisResolutionManifest manifest =
        resolutionManifestFactory.createResolutionManifest(polarisPrincipal, catalogName);
    manifest.addPassthroughPath(
        new ResolverPath(
            Arrays.asList(identifier.namespace().levels()), PolarisEntityType.NAMESPACE));
    manifest.addPassthroughPath(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(identifier), PolarisEntityType.TABLE_LIKE));
    ResolverStatus status = manifest.resolveAll();

    if (status.getStatus() == ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED) {
      throw new NotFoundException(
          "TopLevelEntity of type %s does not exist: %s",
          status.getFailedToResolvedEntityType(), status.getFailedToResolvedEntityName());
    }
    if (status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED) {
      throw new NotFoundException("Table not found: %s", identifier);
    }

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

    return tableWrapper;
  }

  private static Namespace decodeNamespace(String encodedNamespace) {
    if (encodedNamespace == null || encodedNamespace.isEmpty()) {
      throw new IllegalArgumentException("namespace must not be empty");
    }
    // Multi-level namespaces use unit separator (0x1F / %1F) between levels
    String[] levels = encodedNamespace.split("\u001F", -1);
    return Namespace.of(levels);
  }

  private static MetricsListResponse<ScanMetricsReport> buildScanResponse(
      Page<ScanMetricsRecord> page) {
    List<ScanMetricsReport> reports =
        page.items().stream().map(MetricsReportsService::toScanReport).toList();
    return new MetricsListResponse<>("scan", page.encodedResponseToken(), reports);
  }

  private static MetricsListResponse<CommitMetricsReport> buildCommitResponse(
      Page<CommitMetricsRecord> page) {
    List<CommitMetricsReport> reports =
        page.items().stream().map(MetricsReportsService::toCommitReport).toList();
    return new MetricsListResponse<>("commit", page.encodedResponseToken(), reports);
  }

  private static ScanMetricsReport toScanReport(ScanMetricsRecord r) {
    MetricsReportActor actor = new MetricsReportActor(r.principalName());
    MetricsReportRequest request =
        new MetricsReportRequest(r.requestId(), r.otelTraceId(), r.otelSpanId());
    ScanMetricsReport.TableObject object =
        new ScanMetricsReport.TableObject(r.snapshotId().orElse(null));
    ScanMetricsReport.Payload.Data data =
        new ScanMetricsReport.Payload.Data(
            r.schemaId().orElse(null),
            r.filterExpression().orElse(null),
            r.projectedFieldIds().isEmpty()
                ? null
                : r.projectedFieldIds().stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(",")),
            r.projectedFieldNames().isEmpty() ? null : String.join(",", r.projectedFieldNames()),
            r.resultDataFiles(),
            r.resultDeleteFiles(),
            r.totalFileSizeBytes(),
            r.totalDataManifests(),
            r.totalDeleteManifests(),
            r.scannedDataManifests(),
            r.scannedDeleteManifests(),
            r.skippedDataManifests(),
            r.skippedDeleteManifests(),
            r.skippedDataFiles(),
            r.skippedDeleteFiles(),
            r.totalPlanningDurationMs(),
            r.equalityDeleteFiles(),
            r.positionalDeleteFiles(),
            r.indexedDeleteFiles(),
            r.totalDeleteFileSizeBytes());
    ScanMetricsReport.Payload payload =
        new ScanMetricsReport.Payload("iceberg.metrics.scan", 1, data);
    return new ScanMetricsReport(
        r.reportId(), r.timestamp().toEpochMilli(), actor, request, object, payload);
  }

  private static CommitMetricsReport toCommitReport(CommitMetricsRecord r) {
    MetricsReportActor actor = new MetricsReportActor(r.principalName());
    MetricsReportRequest request =
        new MetricsReportRequest(r.requestId(), r.otelTraceId(), r.otelSpanId());
    CommitMetricsReport.TableObject object = new CommitMetricsReport.TableObject(r.snapshotId());
    CommitMetricsReport.Payload.Data data =
        new CommitMetricsReport.Payload.Data(
            r.sequenceNumber().orElse(null),
            r.operation(),
            r.addedDataFiles(),
            r.removedDataFiles(),
            r.totalDataFiles(),
            r.addedDeleteFiles(),
            r.removedDeleteFiles(),
            r.totalDeleteFiles(),
            r.addedEqualityDeleteFiles(),
            r.removedEqualityDeleteFiles(),
            r.addedPositionalDeleteFiles(),
            r.removedPositionalDeleteFiles(),
            r.addedRecords(),
            r.removedRecords(),
            r.totalRecords(),
            r.addedFileSizeBytes(),
            r.removedFileSizeBytes(),
            r.totalFileSizeBytes(),
            r.totalDurationMs().orElse(null),
            r.attempts());
    CommitMetricsReport.Payload payload =
        new CommitMetricsReport.Payload("iceberg.metrics.commit", 1, data);
    return new CommitMetricsReport(
        r.reportId(), r.timestamp().toEpochMilli(), actor, request, object, payload);
  }
}
