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

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;

import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.transactional.TransactionalMetaStoreManagerImpl;
import org.apache.polaris.core.persistence.transactional.TreeMapMetaStore;
import org.apache.polaris.core.persistence.transactional.TreeMapTransactionalPersistenceImpl;
import org.apache.polaris.core.tag.TagEntity;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PolarisTreeMapMetaStoreManagerTest extends BasePolarisMetaStoreManagerTest {
  @Override
  public PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    TreeMapMetaStore store = new TreeMapMetaStore(diagServices);
    TreeMapTransactionalPersistenceImpl metaStore =
        new TreeMapTransactionalPersistenceImpl(
            diagServices, store, Mockito.mock(), RANDOM_SECRETS);
    TransactionalMetaStoreManagerImpl metaStoreManager =
        new TransactionalMetaStoreManagerImpl(clock, diagServices);
    PolarisCallContext callCtx = new PolarisCallContext(() -> "testRealm", metaStore);
    return new PolarisTestMetaStoreManager(metaStoreManager, callCtx);
  }

  /**
   * Doc-required fault injection on the transactional manager: tag-assignment cleanup during a
   * target entity drop is best-effort, so a persistence failure inside the cleanup must not fail
   * the target drop itself.
   */
  @Test
  void targetDropSucceedsWhenTagCleanupFails() {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    TreeMapMetaStore store = new TreeMapMetaStore(diagServices);
    TreeMapTransactionalPersistenceImpl metaStore =
        Mockito.spy(
            new TreeMapTransactionalPersistenceImpl(
                diagServices, store, Mockito.mock(), RANDOM_SECRETS));
    TransactionalMetaStoreManagerImpl metaStoreManager =
        new TransactionalMetaStoreManagerImpl(clock, diagServices);
    PolarisCallContext callCtx = new PolarisCallContext(() -> "testRealm", metaStore);
    PolarisTestMetaStoreManager fixture =
        new PolarisTestMetaStoreManager(metaStoreManager, callCtx);

    PolarisBaseEntity catalog = fixture.createTestCatalog("cleanup_test");
    PolarisBaseEntity n1 =
        fixture.ensureExistsByName(List.of(catalog), PolarisEntityType.NAMESPACE, "N1");
    PolarisBaseEntity t1 =
        fixture.createEntity(
            List.of(catalog, n1),
            PolarisEntityType.TABLE_LIKE,
            PolarisEntitySubType.ICEBERG_TABLE,
            "TBL1");
    TagEntity tag = fixture.createTag(catalog, "T1", List.of("v1"));
    List<PolarisEntityCore> catalogPath = List.of(catalog);
    List<PolarisEntityCore> tablePath = List.of(catalog, n1);
    Assertions.assertThat(
            metaStoreManager
                .assignTagToEntity(callCtx, tablePath, t1, 0, catalogPath, tag, "v1")
                .isSuccess())
        .isTrue();

    // arm the failure on exactly the seam the drop-path cleanup reads through
    Mockito.doThrow(new RuntimeException("injected cleanup failure"))
        .when(metaStore)
        .loadAllTagAssignmentsOnTargetEntityInCurrentTxn(
            Mockito.any(), Mockito.eq(t1.getCatalogId()), Mockito.eq(t1.getId()));

    var dropped = metaStoreManager.dropEntityIfExists(callCtx, tablePath, t1, Map.of(), false);
    Assertions.assertThat(dropped.isSuccess()).isTrue();
    Assertions.assertThat(
            metaStoreManager
                .loadEntity(callCtx, t1.getCatalogId(), t1.getId(), t1.getType())
                .isSuccess())
        .isFalse();
  }
}
