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
import java.util.Objects;
import java.util.Optional;

/** Extracted lineage payload that can be persisted independent of the transport event shape. */
public record LineageIngestRequest(
    List<LineageDataset> datasets,
    List<LineageEdge> edges,
    List<LineageColumnEdge> columnEdges,
    Optional<Instant> eventTime) {
  public LineageIngestRequest {
    datasets = List.copyOf(Objects.requireNonNull(datasets, "datasets must be non-null"));
    edges = List.copyOf(Objects.requireNonNull(edges, "edges must be non-null"));
    columnEdges = List.copyOf(Objects.requireNonNull(columnEdges, "columnEdges must be non-null"));
    Objects.requireNonNull(eventTime, "eventTime must be non-null");
  }
}
