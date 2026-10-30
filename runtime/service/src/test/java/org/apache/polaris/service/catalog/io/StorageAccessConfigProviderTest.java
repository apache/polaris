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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.collection.AttributeMap;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

class StorageAccessConfigProviderTest {

  private static final TableIdentifier TABLE =
      TableIdentifier.of(Namespace.of("ns1", "ns2"), "tbl");
  private static final Set<String> LOCATIONS = Set.of("s3://bucket/path");
  private static final Set<PolarisStorageActions> ACTIONS = Set.of(PolarisStorageActions.ALL);

  private PolarisStorageIntegration integration;
  private PolarisStorageIntegrationProvider integrationProvider;
  private StorageAccessConfigProvider provider;

  @BeforeEach
  void setUp() {
    RealmContext realmContext = () -> "test-realm";
    CallContext callContext = mock(CallContext.class);
    when(callContext.getRealmConfig())
        .thenReturn(new RealmConfigImpl((rc, name) -> null, realmContext));

    integration = mock(PolarisStorageIntegration.class);
    when(integration.getStorageAccessConfig(any(), any(), any()))
        .thenReturn(StorageAccessConfig.builder().build());
    integrationProvider = mock(PolarisStorageIntegrationProvider.class);
    when(integrationProvider.getStorageIntegration(any())).thenReturn(integration);

    provider =
        new StorageAccessConfigProvider(
            callContext,
            PolarisPrincipal.of("principal", AttributeMap.EMPTY, Set.of()),
            realmContext,
            integrationProvider);
  }

  @Test
  void testContextFromResolvedTablePath() {
    PolarisResolvedPathWrapper tablePath =
        resolvedPath(
            entity(PolarisEntityType.CATALOG, "cat"),
            entity(PolarisEntityType.NAMESPACE, "ns1"),
            entity(PolarisEntityType.NAMESPACE, "ns2"),
            entity(PolarisEntityType.TABLE_LIKE, "tbl"));
    provider.getStorageAccessConfig(TABLE, LOCATIONS, ACTIONS, Optional.empty(), tablePath);

    verify(integrationProvider).getStorageIntegration(tablePath.getRawFullPath());

    assertThat(capturedContext())
        .returns(Optional.of("cat"), CredentialVendingContext::catalogName)
        .returns(Optional.of("ns1.ns2"), CredentialVendingContext::namespace)
        .returns(Optional.of("tbl"), CredentialVendingContext::tableName);
  }

  @Test
  void testContextFromResolvedPathWithoutCatalogRoot() {
    provider.getStorageAccessConfig(
        TABLE,
        LOCATIONS,
        ACTIONS,
        Optional.empty(),
        resolvedPath(entity(PolarisEntityType.TASK, "entityCleanup_1")));

    assertThat(capturedContext())
        .returns(Optional.empty(), CredentialVendingContext::catalogName)
        .returns(Optional.empty(), CredentialVendingContext::namespace)
        .returns(Optional.empty(), CredentialVendingContext::tableName);
  }

  static Stream<Arguments> taskContexts() {
    return Stream.of(
        Arguments.of(Optional.of("cat"), TABLE, Optional.of("ns1.ns2")),
        Arguments.of(Optional.empty(), TABLE, Optional.of("ns1.ns2")),
        Arguments.of(Optional.of("cat"), TableIdentifier.of("tbl"), Optional.empty()));
  }

  @ParameterizedTest
  @MethodSource("taskContexts")
  void testTaskContextFromCatalogNameAndTableIdentifier(
      Optional<String> catalogName,
      TableIdentifier tableIdentifier,
      Optional<String> expectedNamespace) {
    PolarisResolvedPathWrapper taskPath =
        resolvedPath(entity(PolarisEntityType.TASK, "entityCleanup_1"));
    provider.getStorageAccessConfigForTask(
        catalogName, tableIdentifier, LOCATIONS, ACTIONS, taskPath);

    verify(integrationProvider).getStorageIntegration(taskPath.getRawFullPath());

    assertThat(capturedContext())
        .returns(catalogName, CredentialVendingContext::catalogName)
        .returns(expectedNamespace, CredentialVendingContext::namespace)
        .returns(Optional.of("tbl"), CredentialVendingContext::tableName);
  }

  private CredentialVendingContext capturedContext() {
    ArgumentCaptor<CredentialVendingContext> captor =
        ArgumentCaptor.forClass(CredentialVendingContext.class);
    verify(integration).getStorageAccessConfig(any(), any(), captor.capture());
    return captor.getValue();
  }

  private static PolarisEntity entity(PolarisEntityType type, String name) {
    return new PolarisEntity.Builder().setType(type).setName(name).build();
  }

  private static PolarisResolvedPathWrapper resolvedPath(PolarisEntity... entities) {
    return new PolarisResolvedPathWrapper(
        Stream.of(entities)
            .map(entity -> new ResolvedPolarisEntity(entity, List.of(), List.of()))
            .toList());
  }
}
