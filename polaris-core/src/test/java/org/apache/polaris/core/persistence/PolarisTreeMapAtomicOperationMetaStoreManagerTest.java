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
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.transactional.TreeMapMetaStore;
import org.apache.polaris.core.persistence.transactional.TreeMapTransactionalPersistenceImpl;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class PolarisTreeMapAtomicOperationMetaStoreManagerTest
    extends BasePolarisMetaStoreManagerTest {
  @Override
  public PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    TreeMapMetaStore store = new TreeMapMetaStore(diagServices);
    TreeMapTransactionalPersistenceImpl metaStore =
        new TreeMapTransactionalPersistenceImpl(
            diagServices, store, Mockito.mock(), RANDOM_SECRETS);
    AtomicOperationMetaStoreManager metaStoreManager =
        new AtomicOperationMetaStoreManager(clock, diagServices);
    PolarisCallContext callCtx = new PolarisCallContext(() -> "testRealm", metaStore);
    return new PolarisTestMetaStoreManager(metaStoreManager, callCtx);
  }

  @Override
  @Test
  @Disabled(
      "AtomicOperationMetaStoreManager calls storePrincipalSecrets outside a transaction, which is incompatible with "
          + "TreeMap's transactional slice reads. Collision detection is covered by JDBC and NoSQL backend tests.")
  protected void testResetCredentialsClientIdCollision() {}

  @Test
  void dropCatalogListsAtMostTwoCatalogRoleIdentities() {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    TreeMapMetaStore store = new TreeMapMetaStore(diagServices);
    TreeMapTransactionalPersistenceImpl spiedStore =
        Mockito.spy(
            new TreeMapTransactionalPersistenceImpl(
                diagServices, store, Mockito.mock(), RANDOM_SECRETS));
    PolarisTestMetaStoreManager tm =
        new PolarisTestMetaStoreManager(
            new AtomicOperationMetaStoreManager(clock, diagServices),
            new PolarisCallContext(() -> "testRealm", spiedStore));

    PolarisBaseEntity catalog =
        new PolarisBaseEntity(
            PolarisEntityConstants.getNullId(),
            tm.polarisMetaStoreManager().generateNewEntityId(tm.polarisCallContext()).getId(),
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootEntityId(),
            "counted");
    catalog =
        tm.polarisMetaStoreManager()
            .createCatalog(tm.polarisCallContext(), catalog, List.of())
            .getCatalog();
    Mockito.clearInvocations(spiedStore);

    tm.polarisMetaStoreManager()
        .dropEntityIfExists(tm.polarisCallContext(), null, catalog, Map.of(), false);

    Mockito.verify(spiedStore, Mockito.never())
        .listFullEntities(
            Mockito.any(),
            Mockito.anyLong(),
            Mockito.anyLong(),
            Mockito.eq(PolarisEntityType.CATALOG_ROLE),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());

    ArgumentCaptor<PageToken> token = ArgumentCaptor.forClass(PageToken.class);
    Mockito.verify(spiedStore)
        .listEntities(
            Mockito.any(),
            Mockito.anyLong(),
            Mockito.anyLong(),
            Mockito.eq(PolarisEntityType.CATALOG_ROLE),
            Mockito.any(),
            token.capture());
    assertThat(token.getValue().pageSize()).hasValue(2);
  }
}
