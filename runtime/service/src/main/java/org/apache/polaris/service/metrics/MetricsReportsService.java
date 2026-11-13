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
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
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
  private static final char TOKEN_SEPARATOR = '.';
  private static final int FINGERPRINT_BYTES = 12;

  private final PolarisAuthorizer authorizer;
  private final PolarisPrincipal polarisPrincipal;
  private final ResolutionManifestFactory resolutionManifestFactory;
  private final Instance<MetricsQuerySpi> queryProvider;
  private final CatalogPrefixParser prefixParser;
  private final RealmConfig realmConfig;
  private final Clock clock;

  @Inject
  public MetricsReportsService(
      @NonNull PolarisAuthorizer authorizer,
      @NonNull PolarisPrincipal polarisPrincipal,
      @NonNull ResolutionManifestFactory resolutionManifestFactory,
      @Any Instance<MetricsQuerySpi> queryProvider,
      @NonNull CatalogPrefixParser prefixParser,
      @NonNull RealmConfig realmConfig,
      @NonNull Clock clock) {
    this.authorizer = authorizer;
    this.polarisPrincipal = polarisPrincipal;
    this.resolutionManifestFactory = resolutionManifestFactory;
    this.prefixParser = prefixParser;
    this.queryProvider = queryProvider;
    this.realmConfig = realmConfig;
    this.clock = clock;
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
    int maxObjects = realmConfig.getConfig(FeatureConfiguration.METRICS_QUERY_MAX_OBJECTS);
    if (request.getObjects().size() > maxObjects) {
      throw new IllegalArgumentException(
          "objects must not contain more than %d entries".formatted(maxObjects));
    }
    if (request.getPageSize() != null && request.getPageSize() < 1) {
      throw new IllegalArgumentException("pageSize must be at least 1");
    }
    if (request.getTimestampFrom() != null
        && request.getTimestampTo() != null
        && request.getTimestampFrom() >= request.getTimestampTo()) {
      throw new IllegalArgumentException("timestampFrom must be before timestampTo");
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
      } else {
        long snapshotId = parseIcebergSnapshotId(object.getSnapshotId());
        if (snapshots == null) {
          snapshotsByTable.put(identifier, new LinkedHashSet<>(List.of(snapshotId)));
        } else if (!snapshots.isEmpty()) {
          snapshots.add(snapshotId);
        }
      }
    }
    if (types.isEmpty() || snapshotsByTable.isEmpty()) {
      return toResponse(null, List.of());
    }

    // A page token is only valid for the query that produced it. Check that before doing any work.
    String queryFingerprint =
        fingerprint(
            catalogName,
            types,
            snapshotsByTable,
            request.getTimestampFrom(),
            request.getTimestampTo());
    ResumedPage resumed = unwrapPageToken(request.getPageToken(), queryFingerprint);
    String backendPageToken = resumed == null ? null : resumed.backendToken();

    ResolvedTables resolved =
        resolveAndAuthorizeTableMetrics(catalogName, List.copyOf(snapshotsByTable.keySet()));
    Map<Long, TableIdentifier> tableIdToIdentifier = resolved.tableIdToIdentifier();
    if (tableIdToIdentifier.isEmpty()) {
      // On a later page, the tables readable earlier were dropped or lost the privilege. End the
      // listing instead of failing it partway through.
      if (resumed != null) {
        return toResponse(null, List.of());
      }
      // Missing and denied tables are indistinguishable, so this does not reveal which exist.
      throw new ForbiddenException(
          "Principal '%s' is not authorized to read metrics of any of the requested tables",
          polarisPrincipal.getName());
    }
    CatalogEntity catalogEntity = resolved.catalogEntity();

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

    // The query API always paginates, independently of LIST_PAGINATION_ENABLED, so that no request
    // can ask the backend for an unbounded result.
    int maxPageSize =
        Math.max(1, realmConfig.getConfig(FeatureConfiguration.METRICS_QUERY_MAX_PAGE_SIZE));
    int defaultPageSize =
        Math.max(1, realmConfig.getConfig(FeatureConfiguration.METRICS_QUERY_DEFAULT_PAGE_SIZE));
    Integer requestedPageSize = request.getPageSize();
    if (requestedPageSize == null && backendPageToken == null) {
      requestedPageSize = defaultPageSize;
    }
    PageToken pt = PageToken.build(backendPageToken, requestedPageSize, maxPageSize, () -> true);

    // Without a lower bound the query would cover the full history of every requested table. The
    // window is fixed when the first page is served and carried in the page token, so that later
    // pages query the same range instead of one that slides with the clock.
    Long timestampFrom = request.getTimestampFrom();
    if (resumed != null) {
      timestampFrom = resumed.timestampFrom();
    }
    if (timestampFrom == null) {
      long windowDays =
          Math.max(
              1,
              realmConfig.getConfig(FeatureConfiguration.METRICS_QUERY_DEFAULT_TIME_WINDOW_DAYS));
      long upperBound =
          request.getTimestampTo() != null ? request.getTimestampTo() : clock.millis();
      timestampFrom = upperBound - Duration.ofDays(windowDays).toMillis();
    }
    MetricsQuerySpi provider = queryProvider.get();

    Page<MetricsRecordIdentity> page =
        provider.listReports(
            types, catalogEntity.getId(), selectors, timestampFrom, request.getTimestampTo(), pt);

    String backendNextToken = page.encodedResponseToken();
    String nextPageToken =
        backendNextToken == null
            ? null
            : String.join(
                String.valueOf(TOKEN_SEPARATOR),
                queryFingerprint,
                Long.toString(timestampFrom),
                backendNextToken);

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

  /**
   * Returns a digest of everything that selects the result set of a query, excluding paging
   * parameters. It binds a page token to its query; it is not a signature.
   */
  private static String fingerprint(
      String catalogName,
      Set<MetricsQuerySpi.MetricType> types,
      Map<TableIdentifier, Set<Long>> snapshotsByTable,
      Long timestampFrom,
      Long timestampTo) {
    Set<String> tables = new TreeSet<>();
    snapshotsByTable.forEach(
        (identifier, snapshots) -> {
          StringBuilder entry = new StringBuilder();
          for (String level : identifier.namespace().levels()) {
            entry.append(level.length()).append(':').append(level);
          }
          entry
              .append('/')
              .append(identifier.name().length())
              .append(':')
              .append(identifier.name());
          entry.append('#').append(new TreeSet<>(snapshots));
          tables.add(entry.toString());
        });

    Hasher hasher = Hashing.sha256().newHasher();
    putField(hasher, catalogName);
    hasher.putInt(types.size());
    types.stream().map(Enum::name).sorted().forEach(name -> putField(hasher, name));
    putField(hasher, String.valueOf(timestampFrom));
    putField(hasher, String.valueOf(timestampTo));
    tables.forEach(table -> putField(hasher, table));
    byte[] digest = hasher.hash().asBytes();
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(Arrays.copyOf(digest, FINGERPRINT_BYTES));
  }

  private static void putField(Hasher hasher, String value) {
    hasher.putInt(value.length()).putString(value, StandardCharsets.UTF_8);
  }

  /** A verified page token: the lower bound the listing started with and the backend's token. */
  private record ResumedPage(long timestampFrom, String backendToken) {}

  /**
   * Splits a client-supplied page token into its query fingerprint, the pinned lower bound and the
   * backend token, and verifies that the token was issued for this query.
   *
   * @return the parts of the token, or {@code null} when the request starts a new listing
   */
  private static ResumedPage unwrapPageToken(String pageToken, String expectedFingerprint) {
    if (pageToken == null || pageToken.isEmpty()) {
      return null;
    }
    String[] parts = pageToken.split(Pattern.quote(String.valueOf(TOKEN_SEPARATOR)), 3);
    if (parts.length != 3 || parts[2].isEmpty()) {
      throw new IllegalArgumentException("Invalid page token");
    }
    long timestampFrom;
    try {
      timestampFrom = Long.parseLong(parts[1]);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid page token", e);
    }
    if (!expectedFingerprint.equals(parts[0])) {
      throw new IllegalArgumentException(
          "Page token was issued for a different query; repeat the original request parameters");
    }
    return new ResumedPage(timestampFrom, parts[2]);
  }

  /** Iceberg snapshot IDs are 64-bit integers; the API carries them as opaque strings. */
  private static long parseIcebergSnapshotId(String snapshotId) {
    try {
      return Long.parseLong(snapshotId);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid snapshot-id for an iceberg-table: " + snapshotId, e);
    }
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

  /**
   * Resolves the requested tables and keeps those that exist and that the caller may read metrics
   * of. Tables that do not exist and tables the caller is not authorized for are omitted alike, so
   * the response does not reveal which of them exist.
   */
  private ResolvedTables resolveAndAuthorizeTableMetrics(
      String catalogName, List<TableIdentifier> identifiers) {
    PolarisResolutionManifest manifest =
        resolutionManifestFactory.createResolutionManifest(polarisPrincipal, catalogName);
    for (TableIdentifier identifier : identifiers) {
      manifest.addPassthroughPath(
          new ResolverPath(
              Arrays.asList(identifier.namespace().levels()), PolarisEntityType.NAMESPACE, true));
      manifest.addPassthroughPath(
          new ResolverPath(
              PolarisCatalogHelpers.tableIdentifierToList(identifier),
              PolarisEntityType.TABLE_LIKE,
              true));
    }
    ResolverStatus status = manifest.resolveAll();

    if (status.getStatus() == ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED) {
      throw new NotFoundException(
          "TopLevelEntity of type %s does not exist: %s",
          status.getFailedToResolvedEntityType(), status.getFailedToResolvedEntityName());
    }

    Map<Long, TableIdentifier> existing = new LinkedHashMap<>();
    for (TableIdentifier identifier : identifiers) {
      PolarisResolvedPathWrapper tableWrapper =
          manifest.getResolvedPath(
              ResolvedPathKey.ofTableLike(identifier), PolarisEntitySubType.ICEBERG_TABLE, true);
      if (tableWrapper != null) {
        existing.put(tableWrapper.getRawLeafEntity().getId(), identifier);
      }
    }
    if (existing.isEmpty()) {
      return new ResolvedTables(null, Map.of());
    }

    List<AuthorizationIntent> intents =
        existing.values().stream().map(id -> metricsReadIntent(catalogName, id)).toList();
    AuthorizationState authorizationState = new AuthorizationState(manifest);
    authorizer.resolveAuthorizationInputs(
        authorizationState, new AuthorizationRequest(polarisPrincipal, intents));

    // Decide per table: a denial for one table must not fail, or reveal itself through, the rest.
    Map<Long, TableIdentifier> authorized = new LinkedHashMap<>();
    existing.forEach(
        (tableId, identifier) -> {
          AuthorizationRequest single =
              new AuthorizationRequest(
                  polarisPrincipal, List.of(metricsReadIntent(catalogName, identifier)));
          if (authorizer.authorize(authorizationState, single).isAllowed()) {
            authorized.put(tableId, identifier);
          }
        });

    CatalogEntity catalogEntity = manifest.getResolvedCatalogEntity();
    Preconditions.checkNotNull(catalogEntity, "No catalog available");
    return new ResolvedTables(catalogEntity, authorized);
  }

  private static AuthorizationIntent metricsReadIntent(
      String catalogName, TableIdentifier identifier) {
    return new SingleTargetAuthorizationIntent(
        PolarisAuthorizableOperation.LIST_TABLE_METRICS,
        PolarisSecurableMapper.tableLike(catalogName, identifier));
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
    return new CatalogObject(
        levels, OBJECT_TYPE_ICEBERG_TABLE, snapshotId == null ? null : snapshotId.toString());
  }
}
