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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Regression tests for {@link AtomicOperationMetaStoreManager#refreshResolvedEntity} (#3836). */
public class AtomicOperationMetaStoreManagerRefreshTest {

  private static final long CATALOG_ID = 100L;
  private static final long ENTITY_ID = 200L;
  private static final long PARENT_ID = 50L;
  private static final String ENTITY_NAME = "catalog_admin";

  private PolarisDiagnostics diagnostics;
  private BasePersistence metaStore;
  private PolarisCallContext callCtx;
  private AtomicOperationMetaStoreManager manager;

  @BeforeEach
  public void setUp() {
    diagnostics = new PolarisDefaultDiagServiceImpl();
    metaStore = Mockito.mock(BasePersistence.class);
    callCtx = new PolarisCallContext(() -> "testRealm", metaStore, new MetricsPersistence() {});
    manager = new AtomicOperationMetaStoreManager(Clock.systemUTC(), diagnostics);
  }

  private PolarisBaseEntity buildEntity(int entityVersion, int grantRecordsVersion) {
    return new PolarisBaseEntity.Builder()
        .catalogId(CATALOG_ID)
        .id(ENTITY_ID)
        .parentId(PARENT_ID)
        .typeCode(PolarisEntityType.CATALOG_ROLE.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .name(ENTITY_NAME)
        .entityVersion(entityVersion)
        .grantRecordsVersion(grantRecordsVersion)
        .createTimestamp(System.currentTimeMillis())
        .build();
  }

  /**
   * A concurrent writer bumps grant_records_version between lookupEntityVersions (t0=5) and
   * lookupEntity (t1=7). The returned grantRecordsVersion must come from the entity, so the pair
   * (entity, grantRecordsVersion) satisfies {@link ResolvedPolarisEntity}'s invariant.
   */
  @Test
  public void testRefreshPairsEntityWithItsOwnGrantRecordsVersion() {
    PolarisBaseEntity liveEntity = buildEntity(2, 7);

    when(metaStore.lookupEntityVersions(any(), any()))
        .thenReturn(List.of(new PolarisChangeTrackingVersions(2, 5)));
    when(metaStore.lookupEntity(any(), anyLong(), anyLong(), anyInt())).thenReturn(liveEntity);
    when(metaStore.loadAllGrantRecordsOnGrantee(any(), anyLong(), anyLong())).thenReturn(List.of());
    when(metaStore.loadAllGrantRecordsOnSecurable(any(), anyLong(), anyLong()))
        .thenReturn(List.of());

    ResolvedEntityResult result =
        manager.refreshResolvedEntity(
            callCtx, 1, 3, PolarisEntityType.CATALOG_ROLE, CATALOG_ID, ENTITY_ID);

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.getEntity()).isNotNull();
    assertThat(result.getEntity().getGrantRecordsVersion()).isEqualTo(7);
    assertThat(result.getEntity().getEntityVersion()).isEqualTo(2);
    assertThat(result.getGrantRecordsVersion()).isEqualTo(7);
  }

  /** When entityVersion matches, the entity is not reloaded and the returned entity is null. */
  @Test
  public void testRefreshReturnsNullEntityWhenVersionUnchanged() {
    when(metaStore.lookupEntityVersions(any(), any()))
        .thenReturn(List.of(new PolarisChangeTrackingVersions(2, 9)));
    when(metaStore.loadAllGrantRecordsOnGrantee(any(), anyLong(), anyLong())).thenReturn(List.of());
    when(metaStore.loadAllGrantRecordsOnSecurable(any(), anyLong(), anyLong()))
        .thenReturn(List.of());

    ResolvedEntityResult result =
        manager.refreshResolvedEntity(
            callCtx, 2, 3, PolarisEntityType.CATALOG_ROLE, CATALOG_ID, ENTITY_ID);

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.getEntity()).isNull();
    assertThat(result.getGrantRecordsVersion()).isEqualTo(9);
    assertThat(result.getEntityGrantRecords()).isNotNull();
    Mockito.verify(metaStore, Mockito.never()).lookupEntity(any(), anyLong(), anyLong(), anyInt());
  }

  @Test
  public void testRefreshReturnsNotFoundWhenEntityPurged() {
    when(metaStore.lookupEntityVersions(any(), any())).thenReturn(Collections.singletonList(null));

    ResolvedEntityResult result =
        manager.refreshResolvedEntity(
            callCtx, 1, 1, PolarisEntityType.CATALOG_ROLE, CATALOG_ID, ENTITY_ID);

    assertThat(result.getReturnStatus()).isEqualTo(BaseResult.ReturnStatus.ENTITY_NOT_FOUND);
  }

  /** Both versions match: no reloads, fast path. */
  @Test
  public void testRefreshIsNoopWhenNothingChanged() {
    when(metaStore.lookupEntityVersions(any(), any()))
        .thenReturn(List.of(new PolarisChangeTrackingVersions(4, 12)));

    ResolvedEntityResult result =
        manager.refreshResolvedEntity(
            callCtx, 4, 12, PolarisEntityType.CATALOG_ROLE, CATALOG_ID, ENTITY_ID);

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.getEntity()).isNull();
    assertThat(result.getGrantRecordsVersion()).isEqualTo(12);
    assertThat(result.getEntityGrantRecords()).isNull();

    Mockito.verify(metaStore, Mockito.never()).lookupEntity(any(), anyLong(), anyLong(), anyInt());
    Mockito.verify(metaStore, Mockito.never())
        .loadAllGrantRecordsOnGrantee(any(), anyLong(), anyLong());
    Mockito.verify(metaStore, Mockito.never())
        .loadAllGrantRecordsOnSecurable(any(), anyLong(), anyLong());
  }

  /**
   * Caller's grants snapshot agrees with lookupEntityVersions (both 5) but a concurrent bump means
   * the reloaded entity carries gv=6. Grants must be reloaded so the caller receives a
   * (grantRecords, grantRecordsVersion) pair consistent with the reloaded entity.
   */
  @Test
  public void testRefreshReloadsGrantsWhenEntityCarriesNewerGrantRecordsVersion() {
    PolarisBaseEntity liveEntity = buildEntity(2, 6);

    when(metaStore.lookupEntityVersions(any(), any()))
        .thenReturn(List.of(new PolarisChangeTrackingVersions(2, 5)));
    when(metaStore.lookupEntity(any(), anyLong(), anyLong(), anyInt())).thenReturn(liveEntity);
    when(metaStore.loadAllGrantRecordsOnGrantee(any(), anyLong(), anyLong())).thenReturn(List.of());
    when(metaStore.loadAllGrantRecordsOnSecurable(any(), anyLong(), anyLong()))
        .thenReturn(List.of());

    ResolvedEntityResult result =
        manager.refreshResolvedEntity(
            callCtx, 1, 5, PolarisEntityType.CATALOG_ROLE, CATALOG_ID, ENTITY_ID);

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.getEntity()).isNotNull();
    assertThat(result.getEntity().getGrantRecordsVersion()).isEqualTo(6);
    assertThat(result.getGrantRecordsVersion()).isEqualTo(6);
    assertThat(result.getEntityGrantRecords()).isNotNull();
    Mockito.verify(metaStore).loadAllGrantRecordsOnGrantee(any(), anyLong(), anyLong());
    Mockito.verify(metaStore).loadAllGrantRecordsOnSecurable(any(), anyLong(), anyLong());
  }
}
