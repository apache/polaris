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
package org.apache.polaris.service.catalog.iceberg;

import static org.apache.polaris.service.catalog.AccessDelegationMode.VENDED_CREDENTIALS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.catalog.LocalCatalogFactory;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.service.catalog.AccessDelegationModeResolver;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.events.EventAttributeMap;
import org.apache.polaris.service.idempotency.IdempotencyConfiguration;
import org.apache.polaris.service.reporting.PolarisMetricsReporter;
import org.junit.jupiter.api.Test;

class IcebergCatalogHandlerTest {

  private static final String CATALOG_NAME = "test";
  private static final Namespace NS1 = Namespace.of("ns1");
  private static final TableIdentifier TABLE2 = TableIdentifier.of(NS1, "table2");

  private final PolarisResolutionManifest resolutionManifest =
      mock(PolarisResolutionManifest.class);
  private final PolarisResolvedPathWrapper resolvedPath = mock(PolarisResolvedPathWrapper.class);
  private final CatalogEntity catalogEntity = mock(CatalogEntity.class);
  private final CallContext callContext = mock(CallContext.class);
  private final RealmConfig realmConfig = mock(RealmConfig.class);
  private final LocalCatalogFactory localCatalogFactory = mock(LocalCatalogFactory.class);
  private final AccessDelegationModeResolver accessDelegationModeResolver =
      mock(AccessDelegationModeResolver.class);
  private final StorageAccessConfigProvider storageAccessConfigProvider =
      mock(StorageAccessConfigProvider.class);

  @SuppressWarnings({"unchecked"})
  private IcebergCatalogHandler newHandler() {
    when(callContext.getRealmConfig()).thenReturn(realmConfig);
    when(callContext.getRealmContext()).thenReturn(mock(RealmContext.class));

    // Resolution manifest factory always returns our pre-configured manifest mock so we can
    // observe and stub interactions with it.
    ResolutionManifestFactory resolutionManifestFactory = mock(ResolutionManifestFactory.class);
    when(resolutionManifestFactory.createResolutionManifest(any(), any()))
        .thenReturn(resolutionManifest);

    // Authorization path: any resolved path lookup returns a non-null wrapper so the
    // "not found" check in CatalogHandler#authorizeBasicTableLikeOperationsOrThrow passes.
    when(resolutionManifest.getResolvedPath(any(), any(), anyBoolean())).thenReturn(resolvedPath);
    when(resolutionManifest.getResolvedPath(any(), any())).thenReturn(resolvedPath);
    when(resolutionManifest.getResolvedPath(any())).thenReturn(resolvedPath);
    when(resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles()).thenReturn(Set.of());

    // initializeCatalog() reads the resolved catalog entity to decide federated vs. local.
    // Return a CatalogEntity without a connection config so we take the local-catalog path.
    when(resolutionManifest.getResolvedCatalogEntity()).thenReturn(catalogEntity);
    when(catalogEntity.getConnectionConfigInfoDpo()).thenReturn(null);

    IdempotencyConfiguration idempotencyConfiguration = mock(IdempotencyConfiguration.class);
    when(idempotencyConfiguration.enabled()).thenReturn(false);

    return ImmutableIcebergCatalogHandler.builder()
        .catalogName(CATALOG_NAME)
        .polarisPrincipal(PolarisPrincipal.of("test", Map.of(), Set.of()))
        .callContext(callContext)
        .metaStoreManager(mock(PolarisMetaStoreManager.class))
        .resolutionManifestFactory(resolutionManifestFactory)
        .authorizer(mock(PolarisAuthorizer.class))
        .diagnostics(mock(PolarisDiagnostics.class))
        .credentialManager(mock(PolarisCredentialManager.class))
        .federatedCatalogFactories(mock(Instance.class))
        .prefixParser(mock(CatalogPrefixParser.class))
        .resolverFactory(mock(ResolverFactory.class))
        .localCatalogFactory(localCatalogFactory)
        .reservedProperties(mock(ReservedProperties.class))
        .catalogHandlerUtils(mock(CatalogHandlerUtils.class))
        .storageAccessConfigProvider(storageAccessConfigProvider)
        .eventAttributeMap(mock(EventAttributeMap.class))
        .metricsReporter(mock(PolarisMetricsReporter.class))
        .clock(mock(Clock.class))
        .accessDelegationModeResolver(accessDelegationModeResolver)
        .idempotencyConfiguration(idempotencyConfiguration)
        .build();
  }

  /**
   * For external (non-Polaris) catalogs, loadCredentials must skip the optimized
   * entity-properties-based path and fall through to a full loadTable on the underlying catalog,
   * propagating the credentials the storage provider returns for that table.
   */
  @Test
  void loadCredentialsFallsBackForExternalCatalog() {
    String tableLocation = "s3://fake-bucket/tables/table2";
    Map<String, String> fakeCredentials =
        Map.of("fake.access.key", "AKIAFAKE", "fake.secret.key", "fakeSecret");

    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.location()).thenReturn(tableLocation);
    when(metadata.properties()).thenReturn(Map.of());
    TableOperations ops = mock(TableOperations.class);
    when(ops.current()).thenReturn(metadata);
    BaseTable table = mock(BaseTable.class);
    when(table.operations()).thenReturn(ops);

    Catalog externalCatalog = mock(Catalog.class);
    when(externalCatalog.loadTable(TABLE2)).thenReturn(table);
    when(localCatalogFactory.createCatalog(any())).thenReturn(externalCatalog);

    // VENDED_CREDENTIALS is what triggers the handler to attach credentials to the response.
    when(accessDelegationModeResolver.resolve(any(), any()))
        .thenReturn(Optional.of(VENDED_CREDENTIALS));

    StorageAccessConfig storageAccessConfig =
        StorageAccessConfig.builder()
            .putCredential("fake.access.key", "AKIAFAKE")
            .putCredential("fake.secret.key", "fakeSecret")
            .build();
    when(storageAccessConfigProvider.getStorageAccessConfig(any(), any(), any(), any(), any()))
        .thenReturn(storageAccessConfig);

    @SuppressWarnings("resource")
    IcebergCatalogHandler handler = newHandler();

    ImmutableLoadCredentialsResponse response = handler.loadCredentials(TABLE2, Optional.empty());

    verify(externalCatalog).loadTable(TABLE2);
    assertThat(response.credentials())
        .singleElement()
        .satisfies(
            (Credential c) -> {
              assertThat(c.prefix()).isEqualTo(tableLocation);
              assertThat(c.config()).containsExactlyInAnyOrderEntriesOf(fakeCredentials);
            });
  }

  /**
   * For native Polaris (Iceberg) catalogs, loadCredentials takes the optimized path: it reads the
   * table location from the entity's internal properties and vends credentials without loading the
   * full table metadata. The underlying catalog's loadTable must NOT be invoked.
   */
  @Test
  void loadCredentialsUsesOptimizedPathForIcebergCatalog() {
    String tableLocation = "s3://fake-bucket/tables/table2";
    Map<String, String> fakeCredentials =
        Map.of("fake.access.key", "AKIAFAKE", "fake.secret.key", "fakeSecret");

    // The entity returned via the resolution manifest carries the table location in its internal
    // properties; that's what lets the optimized path skip a full loadTable.
    PolarisEntity leafEntity =
        new PolarisEntity(
            new PolarisBaseEntity.Builder()
                .typeCode(PolarisEntityType.TABLE_LIKE.getCode())
                .subTypeCode(PolarisEntitySubType.ICEBERG_TABLE.getCode())
                .name(TABLE2.name())
                .internalPropertiesAsMap(Map.of(IcebergTableLikeEntity.LOCATION, tableLocation))
                .build());
    when(resolvedPath.getRawLeafEntity()).thenReturn(leafEntity);

    LocalIcebergCatalog icebergCatalog = mock(LocalIcebergCatalog.class);
    when(localCatalogFactory.createCatalog(any())).thenReturn(icebergCatalog);

    StorageAccessConfig storageAccessConfig =
        StorageAccessConfig.builder()
            .putCredential("fake.access.key", "AKIAFAKE")
            .putCredential("fake.secret.key", "fakeSecret")
            .build();
    when(storageAccessConfigProvider.getStorageAccessConfig(any(), any(), any(), any(), any()))
        .thenReturn(storageAccessConfig);

    @SuppressWarnings("resource")
    IcebergCatalogHandler handler = newHandler();

    ImmutableLoadCredentialsResponse response = handler.loadCredentials(TABLE2, Optional.empty());

    // The whole point of the optimized path is to skip loadTable on the underlying catalog.
    verify(icebergCatalog, never()).loadTable(any());
    assertThat(response.credentials())
        .singleElement()
        .satisfies(
            (Credential c) -> {
              assertThat(c.prefix()).isEqualTo(tableLocation);
              assertThat(c.config()).containsExactlyInAnyOrderEntriesOf(fakeCredentials);
            });
  }

  /**
   * If the entity's internal properties are missing the LOCATION key, the optimized path cannot
   * vend credentials (it has nothing to scope them to), so loadCredentials must fall back to a full
   * loadTable on the underlying catalog. This guards the backfill path noted in the handler: an
   * entity that pre-dates the location-in-properties write should still serve credentials.
   */
  @Test
  void loadCredentialsFallsBackWhenEntityLocationMissing() {
    String tableLocation = "s3://fake-bucket/tables/table2";
    Map<String, String> fakeCredentials =
        Map.of("fake.access.key", "AKIAFAKE", "fake.secret.key", "fakeSecret");

    // Leaf entity is an Iceberg table-like entity but has no LOCATION in its internal properties,
    // forcing the optimized path to fall back.
    PolarisEntity leafEntity =
        new PolarisEntity(
            new PolarisBaseEntity.Builder()
                .typeCode(PolarisEntityType.TABLE_LIKE.getCode())
                .subTypeCode(PolarisEntitySubType.ICEBERG_TABLE.getCode())
                .name(TABLE2.name())
                .internalPropertiesAsMap(Map.of())
                .build());
    when(resolvedPath.getRawLeafEntity()).thenReturn(leafEntity);

    // The fallback path calls loadTable on the underlying catalog and reads location from the
    // returned table metadata, so we need a BaseTable with a current() TableMetadata.
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.location()).thenReturn(tableLocation);
    when(metadata.properties()).thenReturn(Map.of());
    TableOperations ops = mock(TableOperations.class);
    when(ops.current()).thenReturn(metadata);
    BaseTable table = mock(BaseTable.class);
    when(table.operations()).thenReturn(ops);

    LocalIcebergCatalog icebergCatalog = mock(LocalIcebergCatalog.class);
    when(icebergCatalog.loadTable(TABLE2)).thenReturn(table);
    when(localCatalogFactory.createCatalog(any())).thenReturn(icebergCatalog);

    when(accessDelegationModeResolver.resolve(any(), any()))
        .thenReturn(Optional.of(VENDED_CREDENTIALS));

    StorageAccessConfig storageAccessConfig =
        StorageAccessConfig.builder()
            .putCredential("fake.access.key", "AKIAFAKE")
            .putCredential("fake.secret.key", "fakeSecret")
            .build();
    when(storageAccessConfigProvider.getStorageAccessConfig(any(), any(), any(), any(), any()))
        .thenReturn(storageAccessConfig);

    @SuppressWarnings("resource")
    IcebergCatalogHandler handler = newHandler();

    ImmutableLoadCredentialsResponse response = handler.loadCredentials(TABLE2, Optional.empty());

    // Missing LOCATION on the entity must force the fallback — loadTable is the proof.
    verify(icebergCatalog).loadTable(TABLE2);
    assertThat(response.credentials())
        .singleElement()
        .satisfies(
            (Credential c) -> {
              assertThat(c.prefix()).isEqualTo(tableLocation);
              assertThat(c.config()).containsExactlyInAnyOrderEntriesOf(fakeCredentials);
            });
  }
}
