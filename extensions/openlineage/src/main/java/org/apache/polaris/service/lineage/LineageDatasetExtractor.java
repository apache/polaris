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

import io.openlineage.server.OpenLineage;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import org.apache.polaris.service.lineage.LineageDatasetKey.Role;

/**
 * Enumerates the datasets an OpenLineage event references, each tagged with the role it plays.
 *
 * <p>The OpenLineage spec models an event as a {@code oneOf} over three shapes, and the dataset
 * role is a property of the shape rather than of the dataset: {@code RunEvent} and {@code JobEvent}
 * carry separate input and output lists, while {@code DatasetEvent} carries exactly one dataset
 * with no input/output role at all.
 *
 * <p>Enumeration must be exhaustive, because the authorization pass can only check what this class
 * reports. An event shape it cannot enumerate is therefore not treated as "has no datasets" — see
 * {@link #isEnumerable}.
 */
public final class LineageDatasetExtractor {

  private LineageDatasetExtractor() {}

  /**
   * Whether every dataset reference in {@code event} can be enumerated by {@link #extract}.
   *
   * <p>Separate from an empty {@link #extract} result on purpose, and the distinction is a security
   * one. An event with no datasets carries no claim about any Polaris entity and is safe to ingest
   * as-is. An event whose shape is unrecognized may carry dataset references this class cannot see,
   * so it cannot be authorized at all and must not be forwarded — {@code extract} returning empty
   * for it would otherwise read as "nothing to check".
   *
   * <p>In practice only the three spec shapes reach the adapter, because {@code
   * PolarisLineageEventDeserializer} produces nothing else. This guards the case where that stops
   * being true.
   */
  public static boolean isEnumerable(OpenLineage.BaseEvent event) {
    return event instanceof OpenLineage.RunEvent
        || event instanceof OpenLineage.JobEvent
        || event instanceof OpenLineage.DatasetEvent;
  }

  /**
   * Returns every dataset occurrence in {@code event}, deduplicated by {@link LineageDatasetKey}
   * and in encounter order. Returns empty for an event that is null or not {@link #isEnumerable}.
   */
  public static List<LineageDatasetKey> extract(OpenLineage.BaseEvent event) {
    LinkedHashSet<LineageDatasetKey> keys = new LinkedHashSet<>();
    if (event instanceof OpenLineage.RunEvent runEvent) {
      addAll(keys, Role.INPUT, runEvent.getInputs());
      addAll(keys, Role.OUTPUT, runEvent.getOutputs());
    } else if (event instanceof OpenLineage.JobEvent jobEvent) {
      addAll(keys, Role.INPUT, jobEvent.getInputs());
      addAll(keys, Role.OUTPUT, jobEvent.getOutputs());
    } else if (event instanceof OpenLineage.DatasetEvent datasetEvent) {
      keys.add(LineageDatasetKey.of(Role.STANDALONE, datasetEvent.getDataset()));
    }
    return List.copyOf(keys);
  }

  private static void addAll(
      LinkedHashSet<LineageDatasetKey> keys,
      Role role,
      Collection<? extends OpenLineage.Dataset> datasets) {
    if (datasets == null) {
      return;
    }
    for (OpenLineage.Dataset dataset : datasets) {
      keys.add(LineageDatasetKey.of(role, dataset));
    }
  }
}
