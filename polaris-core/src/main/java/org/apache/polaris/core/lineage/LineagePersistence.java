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
package org.apache.polaris.core.lineage;

import java.time.Instant;
import java.util.List;
import org.apache.polaris.core.context.RealmContext;

/**
 * Persistence SPI for lineage storage backends.
 *
 * <p>This contract is intentionally expressed in terms of Polaris's local lineage graph rather than
 * OpenLineage run events. The service layer owns event parsing, forwarding, authorization, and
 * dataset resolution. Persistence backends only need to atomically upsert dataset nodes, dataset
 * edges, column edges, and load normalized lineage graphs.
 */
public interface LineagePersistence {

  /**
   * Upserts dataset nodes.
   *
   * <p>Implementations should treat {@code (realm, catalog, namespace, name)} as the stable dataset
   * identity and update metadata such as the linked Polaris entity when the dataset already exists.
   */
  void upsertDatasets(RealmContext realmContext, List<LineageDataset> datasets);

  /**
   * Upserts directed dataset-level lineage edges.
   *
   * <p>Implementations should treat {@code (realm, source_dataset, target_dataset)} as unique and
   * update the edge timestamp when the same relationship is asserted again.
   */
  void upsertDatasetEdges(
      RealmContext realmContext, List<LineageEdge> edges, Instant lastEventAt);

  /**
   * Upserts directed column-level lineage edges.
   *
   * <p>Implementations should treat {@code (realm, source_dataset, source_field, target_dataset,
   * target_field)} as unique and update the edge timestamp when the same relationship is asserted
   * again.
   */
  void upsertColumnEdges(
      RealmContext realmContext, List<LineageColumnEdge> columnEdges, Instant lastEventAt);

  /** Loads a normalized lineage graph for the requested node and direction. */
  LineageGraph loadLineage(RealmContext realmContext, LineageQueryRequest request);
}
