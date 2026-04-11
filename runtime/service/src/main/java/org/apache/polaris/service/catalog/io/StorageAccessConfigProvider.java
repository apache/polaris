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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Request-scoped entry point for vending scoped storage credentials. Resolves the storage
 * integration for the given entity path via {@link PolarisStorageIntegrationProvider}, builds a
 * {@link CredentialVendingContext} from request-scoped state, and delegates credential vending to
 * the integration.
 */
@RequestScoped
public class StorageAccessConfigProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageAccessConfigProvider.class);

  private final CallContext callContext;
  private final PolarisPrincipal polarisPrincipal;
  private final RealmContext realmContext;
  private final PolarisStorageIntegrationProvider storageIntegrationProvider;

  @Inject
  public StorageAccessConfigProvider(
      CallContext callContext,
      PolarisPrincipal polarisPrincipal,
      RealmContext realmContext,
      PolarisStorageIntegrationProvider storageIntegrationProvider) {
    this.callContext = callContext;
    this.polarisPrincipal = polarisPrincipal;
    this.realmContext = realmContext;
    this.storageIntegrationProvider = storageIntegrationProvider;
  }

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

    StorageAccessConfig accessConfig =
        getStorageAccessConfig(
            resolvedPath.getRawFullPath(),
            tableLocations,
            storageActions,
            refreshCredentialsEndpoint);

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

  private StorageAccessConfig getStorageAccessConfig(
      @Nonnull List<PolarisEntity> resolvedEntityPath,
      @Nonnull Set<String> locations,
      @Nonnull Set<PolarisStorageActions> storageActions,
      @Nonnull Optional<String> refreshCredentialsEndpoint) {

    boolean skipCredentialSubscopingIndirection =
        callContext
            .getRealmConfig()
            .getConfig(FeatureConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION);
    if (skipCredentialSubscopingIndirection) {
      return StorageAccessConfig.builder().build();
    }

    PolarisStorageIntegration<?> integration =
        storageIntegrationProvider.getStorageIntegration(resolvedEntityPath);
    if (integration == null) {
      return StorageAccessConfig.builder().supportsCredentialVending(false).build();
    }

    boolean allowList =
        storageActions.contains(PolarisStorageActions.LIST)
            || storageActions.contains(PolarisStorageActions.ALL);
    Set<String> writeLocations =
        storageActions.contains(PolarisStorageActions.WRITE)
                || storageActions.contains(PolarisStorageActions.DELETE)
                || storageActions.contains(PolarisStorageActions.ALL)
            ? locations
            : Set.of();

    CredentialVendingContext credentialVendingContext =
        buildCredentialVendingContext(resolvedEntityPath);

    return integration.getOrLoadSubscopedCreds(
        allowList, locations, writeLocations, refreshCredentialsEndpoint, credentialVendingContext);
  }

  private CredentialVendingContext buildCredentialVendingContext(
      List<PolarisEntity> resolvedEntityPath) {
    CredentialVendingContext.Builder builder = CredentialVendingContext.builder();

    List<String> sessionTagFields =
        callContext
            .getRealmConfig()
            .getConfig(FeatureConfiguration.SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL);

    builder.realm(Optional.of(realmContext.getRealmIdentifier()));

    if (!resolvedEntityPath.isEmpty()) {
      // First entity is the catalog
      builder.catalogName(Optional.of(resolvedEntityPath.get(0).getName()));

      // Last entity is the table/view
      PolarisEntity leaf = resolvedEntityPath.get(resolvedEntityPath.size() - 1);
      if (leaf.getType() == PolarisEntityType.TABLE_LIKE
          || leaf.getType() == PolarisEntityType.TASK) {
        builder.tableName(Optional.of(leaf.getName()));
      }

      // Namespace entities are between catalog and leaf
      if (resolvedEntityPath.size() > 2) {
        String namespace =
            resolvedEntityPath.subList(1, resolvedEntityPath.size() - 1).stream()
                .map(PolarisEntity::getName)
                .collect(Collectors.joining("."));
        builder.namespace(Optional.of(namespace));
      }
    }

    builder.principalName(Optional.of(polarisPrincipal.getName()));

    Set<String> roles = polarisPrincipal.getRoles();
    if (roles != null && !roles.isEmpty()) {
      String rolesString = roles.stream().sorted().collect(Collectors.joining(","));
      builder.activatedRoles(Optional.of(rolesString));
    }

    if (sessionTagFields.contains(FeatureConfiguration.SESSION_TAG_FIELD_TRACE_ID)) {
      builder.traceId(getCurrentTraceId());
    }

    return builder.build();
  }

  private Optional<String> getCurrentTraceId() {
    SpanContext spanContext = Span.current().getSpanContext();
    if (spanContext.isValid()) {
      return Optional.of(spanContext.getTraceId());
    }
    return Optional.empty();
  }
}
