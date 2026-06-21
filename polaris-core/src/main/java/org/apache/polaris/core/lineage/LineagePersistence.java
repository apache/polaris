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
 * <p>This contract is expressed in terms of Polaris's local lineage graph rather than raw
 * OpenLineage run events. The service layer owns event parsing, forwarding, authorization, and
 * dataset resolution. Persistence backends persist dataset nodes, dataset edges, and column edges,
 * and load normalized lineage graphs.
 */
public interface LineagePersistence {

  /**
   * Persists dataset nodes in {@code lineage_datasets}.
   *
   * <p>Implementations should treat dataset {@code (realm, namespace, name)} values as unique. If a
   * dataset already exists, implementations should update mutable metadata such as optional linkage
   * to a Polaris-managed entity.
   */
  void upsertDatasets(RealmContext realmContext, List<LineageDataset> datasets);

  /**
   * Persists dataset-level directed edges in {@code lineage_edges}.
   *
   * <p>Repeated events asserting the same relationship should update the stored edge timestamp to
   * {@code lastEventAt} rather than creating duplicate edges.
   */
  void replaceDatasetEdges(
      RealmContext realmContext, List<LineageEdge> edges, Instant lastEventAt);

  /**
   * Persists field-level directed edges in {@code lineage_column_edges}.
   *
   * <p>Repeated events asserting the same column mapping should update the stored edge timestamp to
   * {@code lastEventAt} rather than creating duplicate edges.
   */
  void upsertColumnEdges(
      RealmContext realmContext, List<LineageColumnEdge> columnEdges, Instant lastEventAt);

  /** Loads a normalized lineage graph for the requested node and direction. */
  LineageGraph loadLineage(RealmContext realmContext, LineageQueryRequest request);
}
