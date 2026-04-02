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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
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
import org.apache.polaris.core.persistence.metrics.MetricsRecordIdentity;
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

    PageToken token;
    try {
      token = PageToken.build(pageToken, pageSize, () -> true);
    } catch (IllegalArgumentException e) {
      return errorResponse(400, e.getMessage());
    }

    return switch (metricType) {
      case "scan" -> {
        Page<ScanMetricsRecord> page;
        try {
          page =
              persistence.listScanReports(
                  catalogId, tableId, snapshotId, principalName, timestampFrom, timestampTo, token);
        } catch (IllegalArgumentException e) {
          yield errorResponse(400, e.getMessage());
        }
        yield Response.ok(buildResponse("scan", page, MetricsReportsService::scanRecordToMap)).build();
      }
      case "commit" -> {
        Page<CommitMetricsRecord> page;
        try {
          page =
              persistence.listCommitReports(
                  catalogId, tableId, snapshotId, principalName, timestampFrom, timestampTo, token);
        } catch (IllegalArgumentException e) {
          yield errorResponse(400, e.getMessage());
        }
        yield Response.ok(buildResponse("commit", page, MetricsReportsService::commitRecordToMap)).build();
      }
      default -> errorResponse(400, "Invalid metricType: " + metricType + "; must be 'scan' or 'commit'");
    };
  }

  private static Response errorResponse(int code, String message) {
    Map<String, Object> err = new LinkedHashMap<>();
    err.put("message", message);
    err.put("type", "BadRequestException");
    err.put("code", code);
    return Response.status(code).entity(err).build();
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
      throw new BadRequestException("namespace must not be empty");
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

  private static Map<String, Object> baseRecordFields(MetricsRecordIdentity r) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("reportId", r.reportId());
    m.put("catalogId", r.catalogId());
    m.put("tableId", r.tableId());
    m.put("timestampMs", r.timestamp().toEpochMilli());
    m.put("principalName", r.principalName());
    m.put("requestId", r.requestId());
    m.put("otelTraceId", r.otelTraceId());
    m.put("otelSpanId", r.otelSpanId());
    return m;
  }

  private static Map<String, Object> scanRecordToMap(ScanMetricsRecord r) {
    Map<String, Object> m = baseRecordFields(r);
    r.snapshotId().ifPresent(v -> m.put("snapshotId", v));
    r.schemaId().ifPresent(v -> m.put("schemaId", v));
    r.filterExpression().ifPresent(v -> m.put("filterExpression", v));
    if (!r.projectedFieldIds().isEmpty()) {
      m.put(
          "projectedFieldIds",
          r.projectedFieldIds().stream().map(Object::toString).collect(Collectors.joining(",")));
    }
    if (!r.projectedFieldNames().isEmpty()) {
      m.put("projectedFieldNames", String.join(",", r.projectedFieldNames()));
    }
    m.put("resultDataFiles", r.resultDataFiles());
    m.put("resultDeleteFiles", r.resultDeleteFiles());
    m.put("totalFileSizeBytes", r.totalFileSizeBytes());
    m.put("totalDataManifests", r.totalDataManifests());
    m.put("totalDeleteManifests", r.totalDeleteManifests());
    m.put("scannedDataManifests", r.scannedDataManifests());
    m.put("scannedDeleteManifests", r.scannedDeleteManifests());
    m.put("skippedDataManifests", r.skippedDataManifests());
    m.put("skippedDeleteManifests", r.skippedDeleteManifests());
    m.put("skippedDataFiles", r.skippedDataFiles());
    m.put("skippedDeleteFiles", r.skippedDeleteFiles());
    m.put("totalPlanningDurationMs", r.totalPlanningDurationMs());
    m.put("equalityDeleteFiles", r.equalityDeleteFiles());
    m.put("positionalDeleteFiles", r.positionalDeleteFiles());
    m.put("indexedDeleteFiles", r.indexedDeleteFiles());
    m.put("totalDeleteFileSizeBytes", r.totalDeleteFileSizeBytes());
    return m;
  }

  private static Map<String, Object> commitRecordToMap(CommitMetricsRecord r) {
    Map<String, Object> m = baseRecordFields(r);
    m.put("snapshotId", r.snapshotId());
    r.sequenceNumber().ifPresent(v -> m.put("sequenceNumber", v));
    m.put("operation", r.operation());
    m.put("addedDataFiles", r.addedDataFiles());
    m.put("removedDataFiles", r.removedDataFiles());
    m.put("totalDataFiles", r.totalDataFiles());
    m.put("addedDeleteFiles", r.addedDeleteFiles());
    m.put("removedDeleteFiles", r.removedDeleteFiles());
    m.put("totalDeleteFiles", r.totalDeleteFiles());
    m.put("addedEqualityDeleteFiles", r.addedEqualityDeleteFiles());
    m.put("removedEqualityDeleteFiles", r.removedEqualityDeleteFiles());
    m.put("addedPositionalDeleteFiles", r.addedPositionalDeleteFiles());
    m.put("removedPositionalDeleteFiles", r.removedPositionalDeleteFiles());
    m.put("addedRecords", r.addedRecords());
    m.put("removedRecords", r.removedRecords());
    m.put("totalRecords", r.totalRecords());
    m.put("addedFileSizeBytes", r.addedFileSizeBytes());
    m.put("removedFileSizeBytes", r.removedFileSizeBytes());
    m.put("totalFileSizeBytes", r.totalFileSizeBytes());
    r.totalDurationMs().ifPresent(v -> m.put("totalDurationMs", v));
    m.put("attempts", r.attempts());
    return m;
  }
}
