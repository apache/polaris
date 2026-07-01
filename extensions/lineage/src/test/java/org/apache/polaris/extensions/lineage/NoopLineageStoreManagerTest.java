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
package org.apache.polaris.extensions.lineage;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class NoopLineageStoreManagerTest {
  private final NoopLineageStoreManager storeManager = new NoopLineageStoreManager();

  @Test
  void ignoresDatasetUpsert() {
    storeManager.upsertDatasets(List.of());
  }

  @Test
  void ignoresDatasetEdgeReplace() {
    storeManager.replaceDatasetEdges(List.of(), Instant.EPOCH);
  }

  @Test
  void ignoresColumnEdgeUpsert() {
    storeManager.upsertColumnEdges(List.of(), Instant.EPOCH);
  }

  @Test
  void returnsEmptyGraphForLoadLineage() {
    LineageQueryRequest request =
        new LineageQueryRequest(
            "dataset:test:orders", LineageDirection.BOTH, LineageGranularity.DATASET);

    var graph = storeManager.loadLineage(request);

    assertThat(graph.node().id()).isEqualTo("dataset:test:orders");
    assertThat(graph.upstream()).isEmpty();
    assertThat(graph.downstream()).isEmpty();
  }
}
