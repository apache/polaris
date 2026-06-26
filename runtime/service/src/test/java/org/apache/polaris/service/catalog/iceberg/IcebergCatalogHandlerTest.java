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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import java.time.Clock;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
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
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.service.catalog.AccessDelegationModeResolver;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.events.EventAttributeMap;
import org.apache.polaris.service.reporting.PolarisMetricsReporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class IcebergCatalogHandlerTest {

  private static final String CATALOG_NAME = "test";
  private static final Namespace NS1 = Namespace.of("ns1");
  private static final TableIdentifier TABLE2 = TableIdentifier.of(NS1, "table2");
  private static final String TABLE_LOCATION = "s3://fake-bucket/tables/table2";

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
  private final PolarisAuthorizer authorizer = mock(PolarisAuthorizer.class);

  @BeforeEach
  void setUp() {
    StorageAccessConfig storageAccessConfig =
        StorageAccessConfig.builder()
            .putCredential("fake.access.key", "AKIAFAKE")
            .putCredential("fake.secret.key", "fakeSecret")
            .build();
    when(storageAccessConfigProvider.getStorageAccessConfig(any(), any(), any(), any(), any()))
        .thenReturn(storageAccessConfig);
  }

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
    when(resolutionManifest.getResolvedPath(any(), anyBoolean())).thenReturn(resolvedPath);
    when(resolutionManifest.getResolvedPath(any(), any())).thenReturn(resolvedPath);
    when(resolutionManifest.getResolvedPath(any())).thenReturn(resolvedPath);
    when(resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles()).thenReturn(Set.of());

    // initializeCatalog() reads the resolved catalog entity to decide federated vs. local.
    // Return a CatalogEntity without a connection config so we take the local-catalog path.
    when(resolutionManifest.getResolvedCatalogEntity()).thenReturn(catalogEntity);
    when(catalogEntity.getConnectionConfigInfoDpo()).thenReturn(null);

    return ImmutableIcebergCatalogHandler.builder()
        .catalogName(CATALOG_NAME)
        .polarisPrincipal(PolarisPrincipal.of("test", Map.of(), Set.of()))
        .callContext(callContext)
        .authorizationState(new AuthorizationState())
        .metaStoreManager(mock(PolarisMetaStoreManager.class))
        .resolutionManifestFactory(resolutionManifestFactory)
        .authorizer(authorizer)
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
        .build();
  }

  private Catalog mockRegisterTableCatalog(boolean overwrite) {
    Catalog catalog = mock(Catalog.class);
    BaseTable table = baseTable();
    when(catalog.registerTable(TABLE2, TABLE_LOCATION, overwrite)).thenReturn(table);
    when(localCatalogFactory.createCatalog(any())).thenReturn(catalog);
    return catalog;
  }

  private static RegisterTableRequest registerTableRequest(boolean overwrite) {
    return ImmutableRegisterTableRequest.builder()
        .name(TABLE2.name())
        .metadataLocation(TABLE_LOCATION)
        .overwrite(overwrite)
        .build();
  }

  private static BaseTable baseTable() {
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.location()).thenReturn(TABLE_LOCATION);
    when(metadata.properties()).thenReturn(Map.of());

    TableOperations ops = mock(TableOperations.class);
    when(ops.current()).thenReturn(metadata);

    BaseTable table = mock(BaseTable.class);
    when(table.operations()).thenReturn(ops);
    return table;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void assertVendedActions(PolarisStorageActions... actions) {
    ArgumentCaptor<Set<PolarisStorageActions>> actionsCaptor =
        (ArgumentCaptor) ArgumentCaptor.forClass(Set.class);

    verify(storageAccessConfigProvider)
        .getStorageAccessConfig(
            eq(TABLE2), any(), actionsCaptor.capture(), eq(Optional.empty()), eq(resolvedPath));
    assertThat(actionsCaptor.getValue()).containsExactlyInAnyOrder(actions);
  }

  @Test
  void registerTableWithVendedCredentialsVendsReadWriteActionsWhenWriteDelegationAuthorized() {
    Catalog catalog = mockRegisterTableCatalog(false);
    when(accessDelegationModeResolver.resolve(eq(EnumSet.of(VENDED_CREDENTIALS)), any()))
        .thenReturn(Optional.of(VENDED_CREDENTIALS));

    @SuppressWarnings("resource")
    IcebergCatalogHandler handler = newHandler();

    LoadTableResponse response =
        handler.registerTable(
            NS1, registerTableRequest(false), EnumSet.of(VENDED_CREDENTIALS), Optional.empty());

    verify(catalog).registerTable(TABLE2, TABLE_LOCATION, false);
    assertThat(response.credentials()).hasSize(1);
    assertVendedActions(
        PolarisStorageActions.READ, PolarisStorageActions.LIST, PolarisStorageActions.WRITE);
  }

  @Test
  void registerTableWithVendedCredentialsVendsReadActionsWhenWriteDelegationFallsBackToRead() {
    Catalog catalog = mockRegisterTableCatalog(false);
    when(accessDelegationModeResolver.resolve(eq(EnumSet.of(VENDED_CREDENTIALS)), any()))
        .thenReturn(Optional.of(VENDED_CREDENTIALS));
    doThrow(new ForbiddenException("write delegation denied"))
        .when(authorizer)
        .authorizeOrThrow(
            any(),
            any(),
            eq(PolarisAuthorizableOperation.REGISTER_TABLE_WITH_WRITE_DELEGATION),
            nullable(PolarisResolvedPathWrapper.class),
            nullable(PolarisResolvedPathWrapper.class));

    @SuppressWarnings("resource")
    IcebergCatalogHandler handler = newHandler();

    LoadTableResponse response =
        handler.registerTable(
            NS1, registerTableRequest(false), EnumSet.of(VENDED_CREDENTIALS), Optional.empty());

    verify(catalog).registerTable(TABLE2, TABLE_LOCATION, false);
    assertThat(response.credentials()).hasSize(1);
    assertVendedActions(PolarisStorageActions.READ, PolarisStorageActions.LIST);
  }

  @Test
  void registerTableOverwriteWithVendedCredentialsVendsReadWriteActionsWhenTableExists() {
    Catalog catalog = mockRegisterTableCatalog(true);
    when(catalogEntity.isExternal()).thenReturn(false);
    when(accessDelegationModeResolver.resolve(eq(EnumSet.of(VENDED_CREDENTIALS)), any()))
        .thenReturn(Optional.of(VENDED_CREDENTIALS));

    @SuppressWarnings("resource")
    IcebergCatalogHandler handler = newHandler();

    LoadTableResponse response =
        handler.registerTable(
            NS1, registerTableRequest(true), EnumSet.of(VENDED_CREDENTIALS), Optional.empty());

    verify(catalog).registerTable(TABLE2, TABLE_LOCATION, true);
    assertThat(response.credentials()).hasSize(1);
    assertVendedActions(
        PolarisStorageActions.READ, PolarisStorageActions.LIST, PolarisStorageActions.WRITE);
  }

  @Test
  void registerTableOverwriteWithVendedCredentialsVendsReadActionsForMissingTable() {
    Catalog catalog = mockRegisterTableCatalog(true);
    when(catalogEntity.isExternal()).thenReturn(false);
    when(accessDelegationModeResolver.resolve(eq(EnumSet.of(VENDED_CREDENTIALS)), any()))
        .thenReturn(Optional.of(VENDED_CREDENTIALS));
    doThrow(new ForbiddenException("write delegation denied"))
        .when(authorizer)
        .authorizeOrThrow(
            any(),
            any(),
            eq(PolarisAuthorizableOperation.REGISTER_TABLE_WITH_WRITE_DELEGATION),
            nullable(PolarisResolvedPathWrapper.class),
            nullable(PolarisResolvedPathWrapper.class));

    @SuppressWarnings("resource")
    IcebergCatalogHandler handler = newHandler();
    when(resolutionManifest.getResolvedPath(
            eq(ResolvedPathKey.ofTableLike(TABLE2)),
            eq(PolarisEntitySubType.ICEBERG_TABLE),
            anyBoolean()))
        .thenReturn(null);

    LoadTableResponse response =
        handler.registerTable(
            NS1, registerTableRequest(true), EnumSet.of(VENDED_CREDENTIALS), Optional.empty());

    verify(catalog).registerTable(TABLE2, TABLE_LOCATION, true);
    assertThat(response.credentials()).hasSize(1);
    assertVendedActions(PolarisStorageActions.READ, PolarisStorageActions.LIST);
  }

  /**
   * For external (non-Polaris) catalogs, loadCredentials must skip the optimized
   * entity-properties-based path and fall through to a full loadTable on the underlying catalog,
   * propagating the credentials the storage provider returns for that table.
   */
  @Test
  void loadCredentialsFallsBackForExternalCatalog() {
    Map<String, String> fakeCredentials =
        Map.of("fake.access.key", "AKIAFAKE", "fake.secret.key", "fakeSecret");

    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.location()).thenReturn(TABLE_LOCATION);
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
              assertThat(c.prefix()).isEqualTo(TABLE_LOCATION);
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
    verify(accessDelegationModeResolver).resolve(eq(EnumSet.of(VENDED_CREDENTIALS)), any());
    assertThat(response.credentials())
        .singleElement()
        .satisfies(
            (Credential c) -> {
              assertThat(c.prefix()).isEqualTo(tableLocation);
              assertThat(c.config()).containsExactlyInAnyOrderEntriesOf(fakeCredentials);
            });
  }

  @Test
  void loadCredentialsFallbackPreservesDelegationModeValidation() {
    PolarisEntity leafEntity =
        new PolarisEntity(
            new PolarisBaseEntity.Builder()
                .typeCode(PolarisEntityType.TABLE_LIKE.getCode())
                .subTypeCode(PolarisEntitySubType.ICEBERG_TABLE.getCode())
                .name(TABLE2.name())
                .internalPropertiesAsMap(Map.of())
                .build());
    when(resolvedPath.getRawLeafEntity()).thenReturn(leafEntity);

    LocalIcebergCatalog icebergCatalog = mock(LocalIcebergCatalog.class);
    when(localCatalogFactory.createCatalog(any())).thenReturn(icebergCatalog);

    when(accessDelegationModeResolver.resolve(eq(EnumSet.of(VENDED_CREDENTIALS)), any()))
        .thenThrow(new IllegalArgumentException("credential vending disabled"));

    @SuppressWarnings("resource")
    IcebergCatalogHandler handler = newHandler();

    assertThatThrownBy(() -> handler.loadCredentials(TABLE2, Optional.empty()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("credential vending disabled");
    verify(icebergCatalog, never()).loadTable(any());
  }
}
