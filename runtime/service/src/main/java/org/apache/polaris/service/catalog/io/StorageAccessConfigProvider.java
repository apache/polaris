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

package org.apache.polaris.service.catalog.io;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageCredentialsVendor;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides temporary, scoped credentials for accessing table data in object storage (S3, GCS, Azure
 * Blob Storage).
 *
 * <p>This provider decouples credential vending from catalog implementations, and should be the
 * primary entrypoint to get sub-scoped credentials for accessing table data.
 */
@RequestScoped
public class StorageAccessConfigProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageAccessConfigProvider.class);

  private final StorageCredentialCache storageCredentialCache;
  private final StorageCredentialsVendor storageCredentialsVendor;
  private final PolarisPrincipal polarisPrincipal;

  @Inject
  public StorageAccessConfigProvider(
      StorageCredentialCache storageCredentialCache,
      StorageCredentialsVendor storageCredentialsVendor,
      PolarisPrincipal polarisPrincipal) {
    this.storageCredentialCache = storageCredentialCache;
    this.storageCredentialsVendor = storageCredentialsVendor;
    this.polarisPrincipal = polarisPrincipal;
  }

  /**
   * Vends credentials for accessing table storage at explicit locations.
   *
   * @param tableIdentifier the table identifier, used for logging and refresh endpoint construction
   * @param tableLocations set of storage location URIs to scope credentials to
   * @param storageActions the storage operations (READ, WRITE, LIST, DELETE) to scope credentials
   *     to
   * @param refreshCredentialsEndpoint optional endpoint URL for clients to refresh credentials
   * @param resolvedPath the entity hierarchy to search for storage configuration
   * @return {@link StorageAccessConfig} with scoped credentials and metadata; empty if no storage
   *     config found
   */
  public StorageAccessConfig getStorageAccessConfig(
      @Nonnull TableIdentifier tableIdentifier,
      @Nonnull Set<String> tableLocations,
      @Nonnull Set<PolarisStorageActions> storageActions,
      @Nonnull Optional<String> refreshCredentialsEndpoint,
      @Nonnull PolarisResolvedPathWrapper resolvedPath) {
    LOGGER
        .atDebug()
        .addKeyValue("tableIdentifier", tableIdentifier)
        .addKeyValue("tableLocation", tableLocations)
        .log("Fetching client credentials for table");
    Optional<PolarisEntity> storageInfo = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);
    if (storageInfo.isEmpty()) {
      LOGGER
          .atWarn()
          .addKeyValue("tableIdentifier", tableIdentifier)
          .log("Table entity has no storage configuration in its hierarchy");
      return StorageAccessConfig.builder().supportsCredentialVending(false).build();
    }
    PolarisEntity storageInfoEntity = storageInfo.get();

    boolean skipCredentialSubscopingIndirection =
        storageCredentialsVendor
            .getRealmConfig()
            .getConfig(FeatureConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION);
    if (skipCredentialSubscopingIndirection) {
      LOGGER
          .atDebug()
          .addKeyValue("tableIdentifier", tableIdentifier)
          .log("Skipping generation of subscoped creds for table");
      return StorageAccessConfig.builder().build();
    }

    boolean allowList =
        storageActions.contains(PolarisStorageActions.LIST)
            || storageActions.contains(PolarisStorageActions.ALL);
    Set<String> writeLocations =
        storageActions.contains(PolarisStorageActions.WRITE)
                || storageActions.contains(PolarisStorageActions.DELETE)
                || storageActions.contains(PolarisStorageActions.ALL)
            ? tableLocations
            : Set.of();

    // Build credential vending context for session tags
    CredentialVendingContext credentialVendingContext =
        buildCredentialVendingContext(tableIdentifier, resolvedPath);

    StorageAccessConfig accessConfig =
        storageCredentialCache.getOrGenerateSubScopeCreds(
            storageCredentialsVendor,
            storageInfoEntity,
            allowList,
            tableLocations,
            writeLocations,
            polarisPrincipal,
            refreshCredentialsEndpoint,
            credentialVendingContext);

    LOGGER
        .atDebug()
        .addKeyValue("tableIdentifier", tableIdentifier)
        .addKeyValue("credentialKeys", accessConfig.credentials().keySet())
        .addKeyValue("extraProperties", accessConfig.extraProperties())
        .log("Loaded scoped credentials for table");
    if (accessConfig.credentials().isEmpty()) {
      LOGGER.debug("No credentials found for table");
    }
    return accessConfig;
  }

  /**
   * Builds a credential vending context from the table identifier and resolved path. This context
   * is used to populate session tags in cloud provider credentials for audit/correlation purposes.
   *
   * <p>The activated roles are included in this context (rather than extracted from
   * PolarisPrincipal during session tag generation) to ensure they are part of the cache key when
   * session tags are enabled. This prevents false positive cache hits when a principal's roles
   * change.
   *
   * @param tableIdentifier the table identifier containing namespace and table name
   * @param resolvedPath the resolved entity path containing the catalog entity
   * @return a credential vending context with catalog, namespace, table, and activated roles
   */
  private CredentialVendingContext buildCredentialVendingContext(
      TableIdentifier tableIdentifier, PolarisResolvedPathWrapper resolvedPath) {
    CredentialVendingContext.Builder builder = CredentialVendingContext.builder();

    // Extract catalog name from the first entity in the resolved path
    List<PolarisEntity> fullPath = resolvedPath.getRawFullPath();
    if (fullPath != null && !fullPath.isEmpty()) {
      builder.catalogName(Optional.of(fullPath.get(0).getName()));
    }

    // Extract namespace from table identifier
    Namespace namespace = tableIdentifier.namespace();
    if (namespace != null && namespace.length() > 0) {
      builder.namespace(Optional.of(String.join(".", namespace.levels())));
    }

    // Extract table name from table identifier
    builder.tableName(Optional.of(tableIdentifier.name()));

    // Extract activated roles from principal - included in context to be part of cache key
    Set<String> roles = polarisPrincipal.getRoles();
    if (roles != null && !roles.isEmpty()) {
      String rolesString = roles.stream().sorted().collect(Collectors.joining(","));
      builder.activatedRoles(Optional.of(rolesString));
    }

    // Extract OpenTelemetry trace ID for end-to-end correlation
    // This enables correlation between credential vending (CloudTrail), catalog operations
    // (Polaris events), and metrics reports from compute engines
    builder.traceId(getCurrentTraceId());

    return builder.build();
  }

  /**
   * Extracts the current OpenTelemetry trace ID from the active span context.
   *
   * @return the trace ID if a valid span context exists, empty otherwise
   */
  private Optional<String> getCurrentTraceId() {
    SpanContext spanContext = Span.current().getSpanContext();
    if (spanContext.isValid()) {
      return Optional.of(spanContext.getTraceId());
    }
    return Optional.empty();
  }
}
