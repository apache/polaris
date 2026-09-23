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

import java.util.List;
import java.util.Objects;

/**
 * Internal response model for lineage queries.
 *
 * <p>The neighbor lists are filtered according to the {@linkplain LineageQueryRequest#direction()
 * requested direction}:
 *
 * <ul>
 *   <li>{@link LineageDirection#UPSTREAM} populates {@code upstream} and leaves {@code downstream}
 *       empty.
 *   <li>{@link LineageDirection#DOWNSTREAM} populates {@code downstream} and leaves {@code
 *       upstream} empty.
 *   <li>{@link LineageDirection#BOTH} populates both lists as applicable.
 * </ul>
 *
 * <p>All lists are non-null. An empty requested list means that no neighbors were found in that
 * direction within the requested scope. An empty unrequested list means that direction was not
 * queried. Populating an unrequested list violates this contract.
 */
record LineageGraph(LineageNode node, List<LineageNode> upstream, List<LineageNode> downstream) {
  LineageGraph {
    Objects.requireNonNull(node, "node must be non-null");
    upstream = List.copyOf(Objects.requireNonNull(upstream, "upstream must be non-null"));
    downstream = List.copyOf(Objects.requireNonNull(downstream, "downstream must be non-null"));
  }
}
