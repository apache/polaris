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
package org.apache.polaris.service.storage;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.BaseMetaStoreManager;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.PolarisCredentialVendor;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link PolarisCredentialVendor} that resolves storage configuration
 * from the entity hierarchy, obtains a {@link
 * org.apache.polaris.core.storage.PolarisStorageIntegration} via {@link
 * PolarisStorageIntegrationProvider}, and vends credentials in-memory without persistence
 * involvement.
 */
@RequestScoped
public class PolarisCredentialVendorImpl implements PolarisCredentialVendor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisCredentialVendorImpl.class);

  private final CallContext callContext;
  private final PolarisPrincipal polarisPrincipal;
  private final RealmContext realmContext;
  private final PolarisStorageIntegrationProvider storageIntegrationProvider;
  private final PolarisDiagnostics diagnostics;

  @Inject
  public PolarisCredentialVendorImpl(
      CallContext callContext,
      PolarisPrincipal polarisPrincipal,
      RealmContext realmContext,
      PolarisStorageIntegrationProvider storageIntegrationProvider,
      PolarisDiagnostics diagnostics) {
    this.callContext = callContext;
    this.polarisPrincipal = polarisPrincipal;
    this.realmContext = realmContext;
    this.storageIntegrationProvider = storageIntegrationProvider;
    this.diagnostics = diagnostics;
  }

  @Override
  @Nonnull
  public StorageAccessConfig getStorageAccessConfig(
      @Nonnull List<PolarisEntity> resolvedEntityPath,
      @Nonnull Set<String> locations,
      @Nonnull Set<PolarisStorageActions> storageActions,
      @Nonnull Optional<String> refreshCredentialsEndpoint) {

    Optional<PolarisEntity> storageInfo =
        PolarisStorageConfigurationInfo.findStorageInfoFromHierarchy(resolvedEntityPath);
    if (storageInfo.isEmpty()) {
      return StorageAccessConfig.builder().supportsCredentialVending(false).build();
    }
    PolarisEntity storageInfoEntity = storageInfo.get();

    if (!isTypeSupported(storageInfoEntity.getType())) {
      diagnostics.fail(
          "entity_type_not_suppported_to_scope_creds", "type={}", storageInfoEntity.getType());
    }

    boolean skipCredentialSubscopingIndirection =
        callContext
            .getRealmConfig()
            .getConfig(FeatureConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION);
    if (skipCredentialSubscopingIndirection) {
      return StorageAccessConfig.builder().build();
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

    PolarisStorageConfigurationInfo storageConfig =
        BaseMetaStoreManager.extractStorageConfiguration(diagnostics, storageInfoEntity);
    var integration = storageIntegrationProvider.getStorageIntegration(storageConfig);
    diagnostics.checkNotNull(
        integration,
        "storage_integration_not_exists",
        "catalogId={}, entityId={}",
        storageInfoEntity.getCatalogId(),
        storageInfoEntity.getId());

    return integration.getOrLoadSubscopedCreds(
        storageConfig,
        allowList,
        locations,
        writeLocations,
        refreshCredentialsEndpoint,
        credentialVendingContext);
  }

  private boolean isTypeSupported(PolarisEntityType type) {
    return type == PolarisEntityType.CATALOG
        || type == PolarisEntityType.NAMESPACE
        || type == PolarisEntityType.TABLE_LIKE
        || type == PolarisEntityType.TASK;
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
