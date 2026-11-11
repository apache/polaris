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

package org.apache.polaris.persistence.nosql.metastore.mutation;

import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings.mapToEntity;
import static org.apache.polaris.persistence.nosql.metastore.mutation.UpdateKeyForCatalogAndEntityType.updateKeyForCatalogAndEntityType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.commit.CommitterState;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.UpdatableIndex;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.content.LocalNamespaceObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalObj;
import org.apache.polaris.persistence.nosql.metastore.committers.ChangeResult;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;

public class TestMutationAttempt {
  @ParameterizedTest
  @CsvSource({"false,false", "true,false", "false,true", "true,true"})
  public void updatePreservesTechnicalAttributeSemantics(boolean changed, boolean principal) {
    ObjBase.Builder<?, ?> originalBuilder =
        principal
            ? PrincipalObj.builder()
                .clientId("test-client")
                .mainSecretHash("test-main-hash")
                .secondarySecretHash("test-secondary-hash")
                .secretSalt("test-salt")
                .credentialRotationRequired(true)
            : LocalNamespaceObj.builder();
    var original =
        originalBuilder
            .id(123)
            .stableId(42)
            .parentStableId(10)
            .name("namespace")
            .entityVersion(7)
            .numParts(3)
            .createTimestamp(Instant.ofEpochSecond(1, 123456789))
            .updateTimestamp(Instant.ofEpochSecond(2, 987654321))
            .putProperty("property", "original")
            .build();
    var entity =
        new PolarisBaseEntity.Builder(mapToEntity(original, principal ? 0 : 10))
            .createTimestamp(5000)
            .propertiesAsMap(Map.of("property", changed ? "updated" : "original"))
            .build();
    var now = Instant.ofEpochSecond(10);
    Persistence persistence = mock();
    CommitterState<CatalogStateObj, MutationResults> state = mock();
    UpdatableIndex<ObjRef> byName = mock();
    UpdatableIndex<IndexKey> byId = mock();
    when(state.persistence()).thenReturn(persistence);
    when(persistence.currentInstant()).thenReturn(now);
    when(persistence.fetch(objRef(original), original.type().targetClass()))
        .thenAnswer(ignored -> original);
    when(byId.get(key(original.stableId()))).thenReturn(key(original.name()));
    when(byName.get(key(original.name()))).thenReturn(objRef(original));
    if (changed) {
      when(persistence.generateId()).thenReturn(456L);
    }

    var result =
        new MutationAttempt(
                updateKeyForCatalogAndEntityType(entity),
                List.of(new EntityUpdate(EntityUpdate.Operation.UPDATE, entity)),
                state,
                byName,
                byId,
                null,
                null,
                null)
            .apply();

    if (changed) {
      assertThat(result).isInstanceOf(ChangeResult.CommitChange.class);
      var written = ArgumentCaptor.forClass(ObjBase.class);
      ObjBase.Builder<?, ?> expectedBuilder =
          original instanceof PrincipalObj principalObj
              ? PrincipalObj.builder().from(principalObj)
              : LocalNamespaceObj.builder().from((LocalNamespaceObj) original);
      verify(state).writeOrReplace(anyString(), written.capture());
      assertThat(written.getValue())
          .isEqualTo(
              expectedBuilder
                  .id(456)
                  .numParts(1)
                  .createTimestamp(Instant.ofEpochMilli(5000))
                  .updateTimestamp(now)
                  .entityVersion(8)
                  .properties(Map.of("property", "updated"))
                  .build());
    } else {
      assertThat(result).isInstanceOf(ChangeResult.NoChange.class);
      verify(state, never()).writeOrReplace(anyString(), any());
      verify(persistence, never()).generateId();
    }
  }
}
