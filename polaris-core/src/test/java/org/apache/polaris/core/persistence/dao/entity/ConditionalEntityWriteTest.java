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
package org.apache.polaris.core.persistence.dao.entity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.dao.entity.ConditionalEntityWrite.Condition;
import org.apache.polaris.core.persistence.dao.entity.ConditionalEntityWrite.Write;
import org.junit.jupiter.api.Test;

public class ConditionalEntityWriteTest {

  private static PolarisBaseEntity entity(long catalogId, long id) {
    return new PolarisBaseEntity(
        catalogId,
        id,
        PolarisEntityType.TABLE_LIKE,
        PolarisEntitySubType.ICEBERG_TABLE,
        catalogId,
        "table_" + id);
  }

  @Test
  void keepsConditionsAndWritesInOrder() {
    ConditionalEntityWrite batch =
        ConditionalEntityWrite.builder()
            .update(null, entity(1, 1))
            .unchanged(null, entity(1, 2))
            .update(null, entity(1, 3))
            .unchanged(null, entity(1, 4))
            .build();

    assertThat(batch.writes())
        .extracting(w -> ((Write.Update) w).entity().getId())
        .containsExactly(1L, 3L);
    assertThat(batch.conditions())
        .extracting(c -> ((Condition.Unchanged) c).entity().getId())
        .containsExactly(2L, 4L);
  }

  @Test
  void rejectsAnEntityThatIsWrittenTwice() {
    assertThatThrownBy(
            () ->
                ConditionalEntityWrite.builder()
                    .update(null, entity(1, 1))
                    .update(null, entity(1, 1))
                    .build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsAnEntityThatIsCheckedTwice() {
    assertThatThrownBy(
            () ->
                ConditionalEntityWrite.builder()
                    .unchanged(null, entity(1, 1))
                    .unchanged(null, entity(1, 1))
                    .build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsAnEntityThatIsBothCheckedAndWritten() {
    assertThatThrownBy(
            () ->
                ConditionalEntityWrite.builder()
                    .update(null, entity(1, 1))
                    .unchanged(null, entity(1, 1))
                    .build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void entitiesWithTheSameIdInDifferentCatalogsAreDifferentEntities() {
    ConditionalEntityWrite batch =
        ConditionalEntityWrite.builder()
            .update(null, entity(1, 7))
            .unchanged(null, entity(2, 7))
            .build();

    assertThat(batch.conditions()).hasSize(1);
    assertThat(batch.writes()).hasSize(1);
  }

  @Test
  void isImmutable() {
    List<Condition> conditions = List.of(new Condition.Unchanged(null, entity(1, 2)));
    ConditionalEntityWrite batch = new ConditionalEntityWrite(conditions, List.of());

    assertThatThrownBy(() -> batch.conditions().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
