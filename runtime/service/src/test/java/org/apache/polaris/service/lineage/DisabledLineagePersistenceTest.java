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
package org.apache.polaris.service.lineage;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.lineage.LineageDirection;
import org.apache.polaris.core.lineage.LineageGranularity;
import org.apache.polaris.core.lineage.LineageQueryRequest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DisabledLineagePersistenceTest {
  private final DisabledLineagePersistence persistence = new DisabledLineagePersistence();
  private final RealmContext realmContext = Mockito.mock(RealmContext.class);

  @Test
  void throwsForDatasetUpsert() {
    assertThatThrownBy(() -> persistence.upsertDatasets(realmContext, List.of()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("No lineage persistence implementation");
  }

  @Test
  void throwsForDatasetEdgeUpsert() {
    assertThatThrownBy(
            () ->
                persistence.replaceDatasetEdges(realmContext, List.of(), List.of(), Instant.EPOCH))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("No lineage persistence implementation");
  }

  @Test
  void throwsForColumnEdgeUpsert() {
    assertThatThrownBy(() -> persistence.upsertColumnEdges(realmContext, List.of(), Instant.EPOCH))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("No lineage persistence implementation");
  }

  @Test
  void throwsForLoadLineage() {
    LineageQueryRequest request =
        new LineageQueryRequest(
            "dataset:test:orders", LineageDirection.BOTH, LineageGranularity.DATASET);

    assertThatThrownBy(() -> persistence.loadLineage(realmContext, request))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("No lineage persistence implementation");
  }
}
