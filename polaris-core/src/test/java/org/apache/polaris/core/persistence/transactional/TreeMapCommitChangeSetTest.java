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
package org.apache.polaris.core.persistence.transactional;

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.EntityMutation;
import org.apache.polaris.core.persistence.GrantMutation;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TreeMapCommitChangeSetTest {

  private PolarisCallContext callCtx;
  private TreeMapTransactionalPersistenceImpl persistence;

  @BeforeEach
  public void setUp() {
    PolarisDiagnostics diagnostics = new PolarisDefaultDiagServiceImpl();
    TreeMapMetaStore store = new TreeMapMetaStore(diagnostics);
    persistence =
        new TreeMapTransactionalPersistenceImpl(diagnostics, store, Mockito.mock(), RANDOM_SECRETS);
    callCtx = new PolarisCallContext(() -> "testRealm", persistence);
  }

  private PolarisBaseEntity entity(long id, String name, int version) {
    return new PolarisBaseEntity.Builder()
        .catalogId(0L)
        .id(id)
        .parentId(0L)
        .typeCode(PolarisEntityType.CATALOG.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .name(name)
        .entityVersion(version)
        .grantRecordsVersion(0)
        .createTimestamp(1L)
        .lastUpdateTimestamp(1L)
        .build();
  }

  @Test
  public void testSupportsAtomicMixedCommit() {
    assertThat(persistence.supportsAtomicMixedCommit()).isTrue();
  }

  @Test
  public void testCommitChangeSetCreatesEntityAndGrantAtomically() {
    PolarisBaseEntity catalog = entity(1L, "catalog", 1);
    PolarisGrantRecord grant = new PolarisGrantRecord(0L, 1L, 0L, 2L, 3);

    persistence.commitChangeSet(
        callCtx, List.of(EntityMutation.create(catalog)), List.of(GrantMutation.create(grant)));

    PolarisBaseEntity loaded =
        persistence.lookupEntity(
            callCtx, catalog.getCatalogId(), catalog.getId(), catalog.getTypeCode());
    assertThat(loaded).isNotNull();
    assertThat(loaded.getName()).isEqualTo("catalog");
    assertThat(persistence.lookupGrantRecord(callCtx, 0L, 1L, 0L, 2L, 3)).isNotNull();
  }

  @Test
  public void testCommitChangeSetUpdateRequiresOriginalEntity() {
    PolarisBaseEntity catalog = entity(1L, "catalog", 1);
    persistence.writeEntity(callCtx, catalog, true, null);

    PolarisBaseEntity updated = entity(1L, "catalog-renamed", 2);
    persistence.commitChangeSet(
        callCtx, List.of(EntityMutation.update(updated, catalog)), List.of());

    PolarisBaseEntity loaded =
        persistence.lookupEntity(
            callCtx, catalog.getCatalogId(), catalog.getId(), catalog.getTypeCode());
    assertThat(loaded.getName()).isEqualTo("catalog-renamed");
    assertThat(loaded.getEntityVersion()).isEqualTo(2);
  }

  @Test
  public void testCommitChangeSetUpdateFailsOnStaleOriginal() {
    PolarisBaseEntity catalog = entity(1L, "catalog", 1);
    persistence.writeEntity(callCtx, catalog, true, null);

    // Simulate a concurrent modification by bumping the version directly.
    PolarisBaseEntity concurrent = entity(1L, "catalog", 2);
    persistence.writeEntity(callCtx, concurrent, false, catalog);

    PolarisBaseEntity updateAttempt = entity(1L, "catalog-renamed", 2);
    assertThatThrownBy(
            () ->
                persistence.commitChangeSet(
                    callCtx, List.of(EntityMutation.update(updateAttempt, catalog)), List.of()))
        .isInstanceOf(RetryOnConcurrencyException.class);
  }

  @Test
  public void testCommitChangeSetDeleteGrantRecord() {
    PolarisBaseEntity catalog = entity(1L, "catalog", 1);
    PolarisGrantRecord grant = new PolarisGrantRecord(0L, 1L, 0L, 2L, 3);
    persistence.commitChangeSet(
        callCtx, List.of(EntityMutation.create(catalog)), List.of(GrantMutation.create(grant)));

    persistence.commitChangeSet(callCtx, List.of(), List.of(GrantMutation.delete(grant)));

    assertThat(persistence.lookupGrantRecord(callCtx, 0L, 1L, 0L, 2L, 3)).isNull();
  }
}
