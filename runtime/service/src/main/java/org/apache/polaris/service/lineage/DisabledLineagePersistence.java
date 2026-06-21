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

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.List;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.lineage.LineageColumnEdge;
import org.apache.polaris.core.lineage.LineageDataset;
import org.apache.polaris.core.lineage.LineageEdge;
import org.apache.polaris.core.lineage.LineageGraph;
import org.apache.polaris.core.lineage.LineagePersistence;
import org.apache.polaris.core.lineage.LineageQueryRequest;

/** Placeholder persistence until a concrete lineage backend is added. */
@DefaultBean
@ApplicationScoped
public class DisabledLineagePersistence implements LineagePersistence {
  private static final String MESSAGE =
      "No lineage persistence implementation is configured for this deployment.";

  @Override
  public void upsertDatasets(RealmContext realmContext, List<LineageDataset> datasets) {
    throw new UnsupportedOperationException(MESSAGE);
  }

  @Override
  public void replaceDatasetEdges(
      RealmContext realmContext, List<LineageEdge> edges, Instant lastEventAt) {
    throw new UnsupportedOperationException(MESSAGE);
  }

  @Override
  public void upsertColumnEdges(
      RealmContext realmContext, List<LineageColumnEdge> columnEdges, Instant lastEventAt) {
    throw new UnsupportedOperationException(MESSAGE);
  }

  @Override
  public LineageGraph loadLineage(RealmContext realmContext, LineageQueryRequest request) {
    throw new UnsupportedOperationException(MESSAGE);
  }
}
