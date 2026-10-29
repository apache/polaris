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
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.polaris.core.auth.AuthorizationIntent;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.SingleTargetAuthorizationIntent;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.metrics.api.model.CatalogObject;
import org.apache.polaris.core.metrics.api.model.ListMetricsResponse;
import org.apache.polaris.core.metrics.api.model.MetricsReport;
import org.apache.polaris.core.metrics.api.model.QueryMetricsRequest;
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
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.common.PolarisSecurableMapper;
import org.apache.polaris.service.metrics.api.PolarisCatalogMetricsReportsApiService;
import org.jspecify.annotations.NonNull;

/**
 * Service implementation for the Metrics Reports API.
 *
 * <p>Resolves catalog/namespace/table names to internal IDs, performs authorization, and delegates
 * durable reads to {@link MetricsQuerySpi} when an implementation is available.
 *
 * <p>Report payloads are projections reconstructed from the persisted {@link ScanMetricsRecord} and
 * {@link CommitMetricsRecord} fields; they do not preserve the original Iceberg report as received.
 */
@Beta
@RequestScoped
public class MetricsReportsService implements PolarisCatalogMetricsReportsApiService {

  private static final String METRIC_TYPE_SCAN = "iceberg-scan";
  private static final String METRIC_TYPE_COMMIT = "iceberg-commit";
  private static final String OBJECT_TYPE_ICEBERG_TABLE = "iceberg-table";

  private final PolarisAuthorizer authorizer;
  private final PolarisPrincipal polarisPrincipal;
  private final ResolutionManifestFactory resolutionManifestFactory;
  private final Instance<MetricsQuerySpi> queryProvider;
  private final CatalogPrefixParser prefixParser;
  private final RealmConfig realmConfig;

  @Inject
  public MetricsReportsService(
      @NonNull PolarisAuthorizer authorizer,
      @NonNull PolarisPrincipal polarisPrincipal,
      @NonNull ResolutionManifestFactory resolutionManifestFactory,
      @Any Instance<MetricsQuerySpi> queryProvider,
      @NonNull CatalogPrefixParser prefixParser,
      @NonNull RealmConfig realmConfig) {
    this.authorizer = authorizer;
    this.polarisPrincipal = polarisPrincipal;
    this.resolutionManifestFactory = resolutionManifestFactory;
    this.prefixParser = prefixParser;
    this.queryProvider = queryProvider;
    this.realmConfig = realmConfig;
  }

  @Override
  public Response queryTableMetrics(
      String prefix,
      QueryMetricsRequest request,
      RealmContext realmContext,
      SecurityContext securityContext) {
    String catalogName = prefixParser.prefixToCatalogName(prefix);

    if (request.getObjects() == null || request.getObjects().isEmpty()) {
      throw new IllegalArgumentException("objects must not be empty");
    }
    if (request.getMetricTypes() == null || request.getMetricTypes().isEmpty()) {
      throw new IllegalArgumentException("metricTypes must not be empty");
    }

    // Unsupported metric types and object types yield no reports.
    Set<MetricsQuerySpi.MetricType> types = EnumSet.noneOf(MetricsQuerySpi.MetricType.class);
    for (String metricType : request.getMetricTypes()) {
      switch (metricType) {
        case METRIC_TYPE_SCAN -> types.add(MetricsQuerySpi.MetricType.SCAN);
        case METRIC_TYPE_COMMIT -> types.add(MetricsQuerySpi.MetricType.COMMIT);
        default -> {}
      }
    }

    // Snapshot IDs requested per table; an empty set means every snapshot, so an entry without a
    // snapshot ID takes precedence over entries of the same table with one.
    Map<TableIdentifier, Set<Long>> snapshotsByTable = new LinkedHashMap<>();
    for (CatalogObject object : request.getObjects()) {
      if (!OBJECT_TYPE_ICEBERG_TABLE.equals(object.getType())) {
        continue;
      }
      TableIdentifier identifier = toTableIdentifier(object.getIdentifier());
      Set<Long> snapshots = snapshotsByTable.get(identifier);
      if (object.getSnapshotId() == null) {
        snapshotsByTable.put(identifier, Set.of());
      } else if (snapshots == null) {
        snapshotsByTable.put(identifier, new LinkedHashSet<>(List.of(object.getSnapshotId())));
      } else if (!snapshots.isEmpty()) {
        snapshots.add(object.getSnapshotId());
      }
    }
    if (snapshotsByTable.isEmpty()) {
      return toResponse(null, List.of());
    }

    ResolvedTables resolved =
        resolveAndAuthorizeTableMetrics(catalogName, List.copyOf(snapshotsByTable.keySet()));
    if (types.isEmpty()) {
      return toResponse(null, List.of());
    }
    CatalogEntity catalogEntity = resolved.catalogEntity();
    Map<Long, TableIdentifier> tableIdToIdentifier = resolved.tableIdToIdentifier();

    List<MetricsQuerySpi.TableSelector> selectors = new ArrayList<>();
    tableIdToIdentifier.forEach(
        (tableId, identifier) -> {
          Set<Long> snapshots = snapshotsByTable.get(identifier);
          if (snapshots.isEmpty()) {
            selectors.add(new MetricsQuerySpi.TableSelector(tableId, null));
          } else {
            snapshots.forEach(
                snapshotId ->
                    selectors.add(new MetricsQuerySpi.TableSelector(tableId, snapshotId)));
          }
        });

    int maxPageSize =
        realmConfig.getConfig(FeatureConfiguration.LIST_PAGINATION_MAX_PAGE_SIZE, catalogEntity);
    PageToken pt =
        PageToken.build(
            request.getPageToken(),
            request.getPageSize(),
            maxPageSize,
            () ->
                realmConfig.getConfig(FeatureConfiguration.LIST_PAGINATION_ENABLED, catalogEntity));
    MetricsQuerySpi provider = queryProvider.get();

    Page<MetricsRecordIdentity> page =
        provider.listReports(
            types,
            catalogEntity.getId(),
            selectors,
            request.getTimestampFrom(),
            request.getTimestampTo(),
            pt);

    String nextPageToken = page.encodedResponseToken();
    if (request.getPageToken() == null && request.getPageSize() == null && nextPageToken != null) {
      throw new BadRequestException(
          "Metrics do not fit the maximum page size of %s; supply a pageToken to paginate",
          maxPageSize);
    }

    return toResponse(
        nextPageToken,
        page.items().stream()
            .map(
                r ->
                    switch (r) {
                      case ScanMetricsRecord scan ->
                          toScanReport(scan, tableIdToIdentifier.get(scan.tableId()));
                      case CommitMetricsRecord commit ->
                          toCommitReport(commit, tableIdToIdentifier.get(commit.tableId()));
                      default ->
                          throw new IllegalStateException(
                              "Provider returned unsupported record type " + r.getClass());
                    })
            .toList());
  }

  private static TableIdentifier toTableIdentifier(List<String> identifier) {
    if (identifier.isEmpty()) {
      throw new IllegalArgumentException("object identifier must not be empty");
    }
    return TableIdentifier.of(
        Namespace.of(identifier.subList(0, identifier.size() - 1).toArray(new String[0])),
        identifier.getLast());
  }

  private static Response toResponse(String nextPageToken, List<MetricsReport> reports) {
    return Response.ok(new ListMetricsResponse(nextPageToken, reports)).build();
  }

  private record ResolvedTables(
      CatalogEntity catalogEntity, Map<Long, TableIdentifier> tableIdToIdentifier) {}

  private ResolvedTables resolveAndAuthorizeTableMetrics(
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

    AuthorizationRequest authorizationRequest =
        new AuthorizationRequest(
            polarisPrincipal,
            identifiers.stream()
                .<AuthorizationIntent>map(
                    identifier ->
                        new SingleTargetAuthorizationIntent(
                            PolarisAuthorizableOperation.LIST_TABLE_METRICS,
                            PolarisSecurableMapper.tableLike(catalogName, identifier)))
                .toList());
    AuthorizationState authorizationState = new AuthorizationState(manifest);
    authorizer.resolveAuthorizationInputs(authorizationState, authorizationRequest);

    Map<Long, TableIdentifier> tableIdToIdentifier = new LinkedHashMap<>();
    for (TableIdentifier identifier : identifiers) {
      PolarisResolvedPathWrapper tableWrapper =
          manifest.getResolvedPath(
              ResolvedPathKey.ofTableLike(identifier), PolarisEntitySubType.ICEBERG_TABLE, true);

      if (tableWrapper == null) {
        throw new NotFoundException("Table not found: %s", identifier);
      }
      tableIdToIdentifier.put(tableWrapper.getRawLeafEntity().getId(), identifier);
    }

    authorizer.authorize(authorizationState, authorizationRequest).throwIfDenied();

    CatalogEntity catalogEntity = manifest.getResolvedCatalogEntity();
    Preconditions.checkNotNull(catalogEntity, "No catalog available");
    return new ResolvedTables(catalogEntity, tableIdToIdentifier);
  }

  private static MetricsReport toScanReport(ScanMetricsRecord r, TableIdentifier identifier) {
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("schema-id", r.schemaId().orElse(null));
    data.put("filter", r.filterExpression().orElse(null));
    data.put("projected-field-ids", r.projectedFieldIds());
    data.put("projected-field-names", r.projectedFieldNames());
    data.put("result-data-files", r.resultDataFiles());
    data.put("result-delete-files", r.resultDeleteFiles());
    data.put("total-file-size-bytes", r.totalFileSizeBytes());
    data.put("total-data-manifests", r.totalDataManifests());
    data.put("total-delete-manifests", r.totalDeleteManifests());
    data.put("scanned-data-manifests", r.scannedDataManifests());
    data.put("scanned-delete-manifests", r.scannedDeleteManifests());
    data.put("skipped-data-manifests", r.skippedDataManifests());
    data.put("skipped-delete-manifests", r.skippedDeleteManifests());
    data.put("skipped-data-files", r.skippedDataFiles());
    data.put("skipped-delete-files", r.skippedDeleteFiles());
    data.put("total-planning-duration-ms", r.totalPlanningDurationMs());
    data.put("equality-delete-files", r.equalityDeleteFiles());
    data.put("positional-delete-files", r.positionalDeleteFiles());
    data.put("indexed-delete-files", r.indexedDeleteFiles());
    data.put("total-delete-file-size-bytes", r.totalDeleteFileSizeBytes());

    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("type", "iceberg.metrics.scan");
    payload.put("version", 1);
    payload.put("data", data);

    return toReport(
        METRIC_TYPE_SCAN,
        identifier,
        r.timestamp().toEpochMilli(),
        r.snapshotId().orElse(null),
        r.requestId(),
        r.otelTraceId(),
        r.otelSpanId(),
        payload);
  }

  private static MetricsReport toCommitReport(CommitMetricsRecord r, TableIdentifier identifier) {
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("sequence-number", r.sequenceNumber().orElse(null));
    data.put("operation", r.operation());
    data.put("added-data-files", r.addedDataFiles());
    data.put("removed-data-files", r.removedDataFiles());
    data.put("total-data-files", r.totalDataFiles());
    data.put("added-delete-files", r.addedDeleteFiles());
    data.put("removed-delete-files", r.removedDeleteFiles());
    data.put("total-delete-files", r.totalDeleteFiles());
    data.put("added-equality-delete-files", r.addedEqualityDeleteFiles());
    data.put("removed-equality-delete-files", r.removedEqualityDeleteFiles());
    data.put("added-positional-delete-files", r.addedPositionalDeleteFiles());
    data.put("removed-positional-delete-files", r.removedPositionalDeleteFiles());
    data.put("added-records", r.addedRecords());
    data.put("removed-records", r.removedRecords());
    data.put("total-records", r.totalRecords());
    data.put("added-file-size-bytes", r.addedFileSizeBytes());
    data.put("removed-file-size-bytes", r.removedFileSizeBytes());
    data.put("total-file-size-bytes", r.totalFileSizeBytes());
    data.put("total-duration-ms", r.totalDurationMs().orElse(null));
    data.put("attempts", r.attempts());

    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("type", "iceberg.metrics.commit");
    payload.put("version", 1);
    payload.put("data", data);

    return toReport(
        METRIC_TYPE_COMMIT,
        identifier,
        r.timestamp().toEpochMilli(),
        r.snapshotId(),
        r.requestId(),
        r.otelTraceId(),
        r.otelSpanId(),
        payload);
  }

  private static MetricsReport toReport(
      String metricType,
      TableIdentifier identifier,
      long timestampMs,
      Long snapshotId,
      String requestId,
      String otelTraceId,
      String otelSpanId,
      Map<String, Object> payload) {
    if (requestId != null || otelTraceId != null || otelSpanId != null) {
      Map<String, Object> request = new LinkedHashMap<>();
      request.put("request-id", requestId);
      request.put("otel-trace-id", otelTraceId);
      request.put("otel-span-id", otelSpanId);
      payload.put("request", request);
    }
    return new MetricsReport(
        metricType, toCatalogObject(identifier, snapshotId), timestampMs, payload);
  }

  private static CatalogObject toCatalogObject(TableIdentifier identifier, Long snapshotId) {
    List<String> levels = new ArrayList<>(Arrays.asList(identifier.namespace().levels()));
    levels.add(identifier.name());
    return new CatalogObject(levels, OBJECT_TYPE_ICEBERG_TABLE, snapshotId);
  }
}
