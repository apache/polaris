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

import java.util.List;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.junit.jupiter.api.Test;

public class MetaStoreChangeSetTest {

  private PolarisBaseEntity entity(long id, String name) {
    return new PolarisBaseEntity.Builder()
        .catalogId(0L)
        .id(id)
        .parentId(0L)
        .typeCode(PolarisEntityType.CATALOG.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .name(name)
        .entityVersion(1)
        .grantRecordsVersion(0)
        .createTimestamp(1L)
        .build();
  }

  @Test
  public void testEmptyChangeSet() {
    assertThat(MetaStoreChangeSet.EMPTY.isEmpty()).isTrue();
  }

  @Test
  public void testCreateCarriesNoOriginal() {
    PolarisBaseEntity entity = entity(1L, "cat");
    MetaStoreChangeSet changeSet = MetaStoreChangeSet.ofCreate(null, entity);
    assertThat(changeSet.isEmpty()).isFalse();
    assertThat(changeSet.creates()).containsExactly(new EntityWithPath(null, entity));
    assertThat(changeSet.updates()).isEmpty();
  }

  @Test
  public void testUpdateCarriesOriginalForCas() {
    PolarisBaseEntity original = entity(1L, "cat");
    PolarisBaseEntity updated = entity(1L, "cat2");
    MetaStoreChangeSet changeSet = MetaStoreChangeSet.ofUpdate(null, updated, original);
    assertThat(changeSet.isEmpty()).isFalse();
    assertThat(changeSet.creates()).isEmpty();
    EntityWithPath update = changeSet.updates().get(0);
    assertThat(update.entity()).isEqualTo(updated);
    assertThat(update.originalEntity()).isEqualTo(original);
  }

  @Test
  public void testBuilderAccumulatesGrants() {
    PolarisBaseEntity e1 = entity(1L, "e1");
    PolarisBaseEntity e2 = entity(2L, "e2");
    PolarisGrantRecord grant = new PolarisGrantRecord(0L, 1L, 0L, 2L, 3);
    MetaStoreChangeSet changeSet =
        new MetaStoreChangeSet.Builder()
            .addCreate(null, e1)
            .addUpdate(null, e2, e2)
            .addGrantRecordCreate(grant)
            .build();

    assertThat(changeSet.creates()).hasSize(1);
    assertThat(changeSet.updates()).hasSize(1);
    assertThat(changeSet.grantRecordsToCreate()).containsExactly(grant);
    assertThat(changeSet.grantRecordsToDelete()).isEmpty();
  }

  @Test
  public void testToBuilderPreservesContents() {
    PolarisBaseEntity entity = entity(1L, "cat");
    PolarisGrantRecord grant = new PolarisGrantRecord(0L, 1L, 0L, 2L, 3);
    MetaStoreChangeSet original =
        new MetaStoreChangeSet.Builder()
            .addCreate(null, entity)
            .addGrantRecordCreate(grant)
            .build();
    MetaStoreChangeSet copy = original.toBuilder().build();
    assertThat(copy.creates()).containsExactlyElementsOf(original.creates());
    assertThat(copy.grantRecordsToCreate())
        .containsExactlyElementsOf(original.grantRecordsToCreate());
  }

  @Test
  public void testBatchFactory() {
    PolarisBaseEntity create = entity(1L, "create");
    PolarisBaseEntity original = entity(2L, "original");
    PolarisBaseEntity update = entity(2L, "update");
    MetaStoreChangeSet changeSet =
        MetaStoreChangeSet.ofBatch(
            List.of(new EntityWithPath(null, create)),
            List.of(new EntityWithPath(null, update, original)));
    assertThat(changeSet.creates()).hasSize(1);
    assertThat(changeSet.updates()).hasSize(1);
    assertThat(changeSet.updates().get(0).originalEntity()).isEqualTo(original);
  }
}
