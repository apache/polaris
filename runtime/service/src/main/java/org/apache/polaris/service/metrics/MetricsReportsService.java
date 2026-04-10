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

import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
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
import org.apache.polaris.service.metrics.api.PolarisCatalogsApiService;

/**
 * Service implementation for the Metrics Reports API.
 *
 * <p>Resolves catalog/namespace/table names to internal IDs, performs authorization, and delegates
 * to the {@link MetricsPersistence} layer to retrieve persisted metrics reports.
 */
@RequestScoped
public class MetricsReportsService implements PolarisCatalogsApiService {

  private final CallContext callContext;
  private final PolarisAuthorizer authorizer;
  private final PolarisPrincipal polarisPrincipal;
  private final ResolutionManifestFactory resolutionManifestFactory;

  @Inject
  public MetricsReportsService(
      @Nonnull CallContext callContext,
      @Nonnull PolarisAuthorizer authorizer,
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull ResolutionManifestFactory resolutionManifestFactory) {
    this.callContext = callContext;
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

    MetricsPersistence persistence = callContext.getPolarisCallContext().getMetaStore();

    PageToken token = PageToken.build(pageToken, pageSize, () -> true);

    return switch (metricType) {
      case "scan" -> {
        Page<ScanMetricsRecord> page =
            persistence.listScanReports(
                catalogId, tableId, snapshotId, principalName, timestampFrom, timestampTo, token);
        yield Response.ok(buildResponse("scan", page, MetricsReportsService::scanRecordToMap))
            .build();
      }
      case "commit" -> {
        Page<CommitMetricsRecord> page =
            persistence.listCommitReports(
                catalogId, tableId, snapshotId, principalName, timestampFrom, timestampTo, token);
        yield Response.ok(buildResponse("commit", page, MetricsReportsService::commitRecordToMap))
            .build();
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

  private static <T> Map<String, Object> buildResponse(
      String metricType, Page<T> page, Function<T, Map<String, Object>> toMap) {
    List<Map<String, Object>> reports = page.items().stream().map(toMap).toList();
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("metricType", metricType);
    response.put("nextPageToken", page.encodedResponseToken());
    response.put("reports", reports);
    return response;
  }

  private static Map<String, Object> scanRecordToMap(ScanMetricsRecord r) {
    Map<String, Object> actor = new LinkedHashMap<>();
    actor.put("principalName", r.principalName());

    Map<String, Object> request = new LinkedHashMap<>();
    request.put("requestId", r.requestId());
    request.put("otelTraceId", r.otelTraceId());
    request.put("otelSpanId", r.otelSpanId());

    Map<String, Object> object = new LinkedHashMap<>();
    r.snapshotId().ifPresent(v -> object.put("snapshotId", v));

    Map<String, Object> data = new LinkedHashMap<>();
    r.schemaId().ifPresent(v -> data.put("schemaId", v));
    r.filterExpression().ifPresent(v -> data.put("filterExpression", v));
    if (!r.projectedFieldIds().isEmpty()) {
      data.put(
          "projectedFieldIds",
          r.projectedFieldIds().stream().map(Object::toString).collect(Collectors.joining(",")));
    }
    if (!r.projectedFieldNames().isEmpty()) {
      data.put("projectedFieldNames", String.join(",", r.projectedFieldNames()));
    }
    data.put("resultDataFiles", r.resultDataFiles());
    data.put("resultDeleteFiles", r.resultDeleteFiles());
    data.put("totalFileSizeBytes", r.totalFileSizeBytes());
    data.put("totalDataManifests", r.totalDataManifests());
    data.put("totalDeleteManifests", r.totalDeleteManifests());
    data.put("scannedDataManifests", r.scannedDataManifests());
    data.put("scannedDeleteManifests", r.scannedDeleteManifests());
    data.put("skippedDataManifests", r.skippedDataManifests());
    data.put("skippedDeleteManifests", r.skippedDeleteManifests());
    data.put("skippedDataFiles", r.skippedDataFiles());
    data.put("skippedDeleteFiles", r.skippedDeleteFiles());
    data.put("totalPlanningDurationMs", r.totalPlanningDurationMs());
    data.put("equalityDeleteFiles", r.equalityDeleteFiles());
    data.put("positionalDeleteFiles", r.positionalDeleteFiles());
    data.put("indexedDeleteFiles", r.indexedDeleteFiles());
    data.put("totalDeleteFileSizeBytes", r.totalDeleteFileSizeBytes());

    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("type", "iceberg.metrics.scan");
    payload.put("version", 1);
    payload.put("data", data);

    Map<String, Object> m = new LinkedHashMap<>();
    m.put("id", r.reportId());
    m.put("timestampMs", r.timestamp().toEpochMilli());
    m.put("actor", actor);
    m.put("request", request);
    m.put("object", object);
    m.put("payload", payload);
    return m;
  }

  private static Map<String, Object> commitRecordToMap(CommitMetricsRecord r) {
    Map<String, Object> actor = new LinkedHashMap<>();
    actor.put("principalName", r.principalName());

    Map<String, Object> request = new LinkedHashMap<>();
    request.put("requestId", r.requestId());
    request.put("otelTraceId", r.otelTraceId());
    request.put("otelSpanId", r.otelSpanId());

    Map<String, Object> object = new LinkedHashMap<>();
    object.put("snapshotId", r.snapshotId());

    Map<String, Object> data = new LinkedHashMap<>();
    r.sequenceNumber().ifPresent(v -> data.put("sequenceNumber", v));
    data.put("operation", r.operation());
    data.put("addedDataFiles", r.addedDataFiles());
    data.put("removedDataFiles", r.removedDataFiles());
    data.put("totalDataFiles", r.totalDataFiles());
    data.put("addedDeleteFiles", r.addedDeleteFiles());
    data.put("removedDeleteFiles", r.removedDeleteFiles());
    data.put("totalDeleteFiles", r.totalDeleteFiles());
    data.put("addedEqualityDeleteFiles", r.addedEqualityDeleteFiles());
    data.put("removedEqualityDeleteFiles", r.removedEqualityDeleteFiles());
    data.put("addedPositionalDeleteFiles", r.addedPositionalDeleteFiles());
    data.put("removedPositionalDeleteFiles", r.removedPositionalDeleteFiles());
    data.put("addedRecords", r.addedRecords());
    data.put("removedRecords", r.removedRecords());
    data.put("totalRecords", r.totalRecords());
    data.put("addedFileSizeBytes", r.addedFileSizeBytes());
    data.put("removedFileSizeBytes", r.removedFileSizeBytes());
    data.put("totalFileSizeBytes", r.totalFileSizeBytes());
    r.totalDurationMs().ifPresent(v -> data.put("totalDurationMs", v));
    data.put("attempts", r.attempts());

    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("type", "iceberg.metrics.commit");
    payload.put("version", 1);
    payload.put("data", data);

    Map<String, Object> m = new LinkedHashMap<>();
    m.put("id", r.reportId());
    m.put("timestampMs", r.timestamp().toEpochMilli());
    m.put("actor", actor);
    m.put("request", request);
    m.put("object", object);
    m.put("payload", payload);
    return m;
  }
}
