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
package org.apache.polaris.core.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TransactionWorkspaceMetaStoreManagerTest {

  private TransactionWorkspaceMetaStoreManager transactionManager;
  private PolarisDiagnostics diagnostics;
  private PolarisMetaStoreManager delegate;

  @BeforeEach
  void setUp() {
    diagnostics = mock(PolarisDiagnostics.class);
    delegate = mock(PolarisMetaStoreManager.class);
    transactionManager = new TransactionWorkspaceMetaStoreManager(diagnostics, delegate);
  }

  @Test
  void testEntityUpdateFunctionality() {
    PolarisCallContext callCtx = mock(PolarisCallContext.class);

    // Test with normal catalog path
    List<PolarisEntityCore> catalogPath = Arrays.asList(mock(PolarisEntityCore.class));
    PolarisBaseEntity entity1 = mock(PolarisBaseEntity.class);

    EntityResult result1 =
        transactionManager.updateEntityPropertiesIfNotChanged(callCtx, catalogPath, entity1);
    assertThat(result1).isNotNull();
    assertThat(result1.getEntity()).isEqualTo(entity1);

    // Test with null catalog path (top-level entity)
    PolarisBaseEntity entity2 = mock(PolarisBaseEntity.class);
    EntityResult result2 =
        transactionManager.updateEntityPropertiesIfNotChanged(callCtx, null, entity2);
    assertThat(result2.getEntity()).isEqualTo(entity2);

    // Test with empty catalog path
    List<PolarisEntityCore> emptyCatalogPath = Collections.emptyList();
    PolarisBaseEntity entity3 = mock(PolarisBaseEntity.class);
    EntityResult result3 =
        transactionManager.updateEntityPropertiesIfNotChanged(callCtx, emptyCatalogPath, entity3);
    assertThat(result3.getEntity()).isEqualTo(entity3);

    // Verify all updates are staged correctly
    List<EntityWithPath> pendingUpdates = transactionManager.getPendingUpdates();
    assertThat(pendingUpdates).hasSize(3);

    assertThat(pendingUpdates.get(0).getCatalogPath()).isEqualTo(catalogPath);
    assertThat(pendingUpdates.get(0).getEntity()).isEqualTo(entity1);

    assertThat(pendingUpdates.get(1).getCatalogPath()).isNull();
    assertThat(pendingUpdates.get(1).getEntity()).isEqualTo(entity2);

    assertThat(pendingUpdates.get(2).getCatalogPath()).isEqualTo(emptyCatalogPath);
    assertThat(pendingUpdates.get(2).getEntity()).isEqualTo(entity3);
  }

  @Test
  void testStageEventFunctionality() {
    // Test one event
    String catalogName = "test_catalog";
    TableIdentifier identifier = TableIdentifier.of("namespace", "table");
    TableMetadata metadataBefore = mock(TableMetadata.class);
    TableMetadata metadataAfter = mock(TableMetadata.class);

    transactionManager.stageEvent(catalogName, identifier, metadataBefore, metadataAfter);

    List<TransactionWorkspaceMetaStoreManager.StageEvent> events =
        transactionManager.getPendingEvents();
    assertThat(events).hasSize(1);

    TransactionWorkspaceMetaStoreManager.StageEvent event = events.get(0);
    assertThat(event.catalogName()).isEqualTo(catalogName);
    assertThat(event.identifier()).isEqualTo(identifier);
    assertThat(event.metadataBefore()).isEqualTo(metadataBefore);
    assertThat(event.metadataAfter()).isEqualTo(metadataAfter);

    // Test another event
    String catalog2 = "catalog2";
    TableIdentifier id2 = TableIdentifier.of("ns2", "table2");
    TableMetadata meta2Before = mock(TableMetadata.class);
    TableMetadata meta2After = mock(TableMetadata.class);

    transactionManager.stageEvent(catalog2, id2, meta2Before, meta2After);

    events = transactionManager.getPendingEvents();
    assertThat(events).hasSize(2);
    assertThat(events.get(1).catalogName()).isEqualTo(catalog2);
    assertThat(events.get(1).identifier()).isEqualTo(id2);
  }

  @Test
  void testPendingUpdatesAndEventsTogether() {
    PolarisCallContext callCtx = mock(PolarisCallContext.class);
    PolarisBaseEntity entity = mock(PolarisBaseEntity.class);
    List<PolarisEntityCore> catalogPath = Arrays.asList(mock(PolarisEntityCore.class));

    String catalogName = "test_catalog";
    TableIdentifier identifier = TableIdentifier.of("namespace", "table");
    TableMetadata metadataBefore = mock(TableMetadata.class);
    TableMetadata metadataAfter = mock(TableMetadata.class);

    transactionManager.updateEntityPropertiesIfNotChanged(callCtx, catalogPath, entity);
    transactionManager.stageEvent(catalogName, identifier, metadataBefore, metadataAfter);

    List<EntityWithPath> pendingUpdates = transactionManager.getPendingUpdates();
    List<TransactionWorkspaceMetaStoreManager.StageEvent> pendingEvents =
        transactionManager.getPendingEvents();

    assertThat(pendingUpdates).hasSize(1);
    assertThat(pendingEvents).hasSize(1);

    // Verify update
    EntityWithPath update = pendingUpdates.get(0);
    assertThat(update.getCatalogPath()).isEqualTo(catalogPath);
    assertThat(update.getEntity()).isEqualTo(entity);

    // Verify event
    TransactionWorkspaceMetaStoreManager.StageEvent event = pendingEvents.get(0);
    assertThat(event.catalogName()).isEqualTo(catalogName);
    assertThat(event.identifier()).isEqualTo(identifier);
  }
}
