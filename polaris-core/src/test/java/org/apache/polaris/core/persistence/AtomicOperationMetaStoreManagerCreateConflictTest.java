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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;

import java.time.Clock;
import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Regression tests for the create-conflict mapping: when the persistence layer reports a unique
 * constraint violation whose conflicting row is not visible in the current transaction snapshot
 * (e.g. under SERIALIZABLE isolation) via {@link RetryOnConcurrencyException}, create operations
 * must surface {@link BaseResult.ReturnStatus#ENTITY_ALREADY_EXISTS} instead of an opaque error.
 */
public class AtomicOperationMetaStoreManagerCreateConflictTest {

  private BasePersistence metaStore;
  private PolarisCallContext callCtx;
  private AtomicOperationMetaStoreManager manager;

  @BeforeEach
  public void setUp() {
    PolarisDiagnostics diagnostics = new PolarisDefaultDiagServiceImpl();
    metaStore = Mockito.mock(BasePersistence.class);
    callCtx = new PolarisCallContext(() -> "testRealm", metaStore);
    manager = new AtomicOperationMetaStoreManager(Clock.systemUTC(), diagnostics);
  }

  private PolarisBaseEntity buildEntity() {
    return new PolarisBaseEntity.Builder()
        .catalogId(100L)
        .id(200L)
        .parentId(50L)
        .typeCode(PolarisEntityType.CATALOG_ROLE.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .name("catalog_admin")
        .entityVersion(1)
        .grantRecordsVersion(0)
        .createTimestamp(System.currentTimeMillis())
        .build();
  }

  @Test
  public void testCreateEntityIfNotExistsMapsRetryOnConcurrencyToAlreadyExists() {
    Mockito.doThrow(new RetryOnConcurrencyException("conflict not visible in snapshot"))
        .when(metaStore)
        .writeEntity(any(), any(), anyBoolean(), any());

    EntityResult result = manager.createEntityIfNotExists(callCtx, null, buildEntity());

    assertThat(result.isSuccess()).isFalse();
    assertThat(result.getReturnStatus()).isEqualTo(BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS);
  }

  @Test
  public void testCreateEntitiesIfNotExistMapsRetryOnConcurrencyToAlreadyExists() {
    Mockito.doThrow(new RetryOnConcurrencyException("conflict not visible in snapshot"))
        .when(metaStore)
        .writeEntities(any(), anyList(), any());

    EntitiesResult result = manager.createEntitiesIfNotExist(callCtx, null, List.of(buildEntity()));

    assertThat(result.isSuccess()).isFalse();
    assertThat(result.getReturnStatus()).isEqualTo(BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS);
  }
}
