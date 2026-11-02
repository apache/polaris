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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.dao.entity.ConditionalEntityWrite;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TransactionWorkspaceMetaStoreManagerTest {
  private TransactionWorkspaceMetaStoreManager workspace;
  private PolarisCallContext callCtx;

  @BeforeEach
  public void setUp() {
    workspace =
        new TransactionWorkspaceMetaStoreManager(
            new PolarisDefaultDiagServiceImpl(), Mockito.mock(PolarisMetaStoreManager.class));
    callCtx = new PolarisCallContext(() -> "testRealm", Mockito.mock(BasePersistence.class));
  }

  private static PolarisBaseEntity table(long id) {
    return new PolarisBaseEntity(
        1L, id, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_TABLE, 1L, "t" + id);
  }

  @Test
  public void recordsCheckedEntitiesSeparatelyFromUpdates() {
    PolarisBaseEntity updated = table(10);
    PolarisBaseEntity checked = table(11);

    workspace.updateEntityPropertiesIfNotChanged(callCtx, null, updated);
    workspace.addUnchangedEntity(null, checked);

    assertThat(workspace.getPendingUpdates())
        .extracting(EntityWithPath::entity)
        .containsExactly(updated);
    assertThat(workspace.getPendingUnchanged())
        .extracting(EntityWithPath::entity)
        .containsExactly(checked);
  }

  @Test
  public void pendingListsAreSnapshots() {
    workspace.addUnchangedEntity(null, table(11));
    List<EntityWithPath> snapshot = workspace.getPendingUnchanged();

    workspace.addUnchangedEntity(null, table(12));

    assertThat(snapshot).hasSize(1);
    assertThat(workspace.getPendingUnchanged()).hasSize(2);
  }

  @Test
  public void cannotApplyAConditionalWriteInsideTheWorkspace() {
    assertThatThrownBy(
            () ->
                workspace.writeEntitiesIfConditionsHold(
                    callCtx, ConditionalEntityWrite.builder().build()))
        .isInstanceOf(RuntimeException.class);
  }
}
