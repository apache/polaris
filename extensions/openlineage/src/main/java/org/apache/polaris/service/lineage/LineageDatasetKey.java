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

/**
 * Identifies one dataset occurrence inside an OpenLineage event: the role the dataset plays in that
 * event, plus the {@code namespace}/{@code name} pair that names it.
 *
 * <p>The role is part of the key because one table may legitimately appear as both an input and an
 * output of a single event — a {@code MERGE INTO} reads and writes the same table — and the two
 * occurrences require different privileges. Deduplicating across roles would silently keep only one
 * of the two requirements.
 *
 * <p>{@code namespace} and {@code name} are carried exactly as they arrived, including case.
 * Polaris entity names are case-sensitive, so folding case here would let a dataset authorize
 * against one entity while addressing another.
 *
 * <p>Both fields may be {@code null}: OpenLineage does not guarantee they are populated, and an
 * event that omits them must be classified rather than rejected with an exception.
 */
public record LineageDatasetKey(Role role, String namespace, String name) {

  /** The part a dataset plays in the event that carried it. */
  public enum Role {
    /** A dataset the event's job read from. */
    INPUT,

    /** A dataset the event's job wrote to. */
    OUTPUT,

    /**
     * The single dataset carried by a {@code DatasetEvent}, which has no input/output role at all.
     */
    STANDALONE
  }

  /**
   * Builds a key for {@code dataset} in {@code role}. A {@code null} dataset yields a key with null
   * namespace and name, which the identity predicate classifies as external.
   */
  public static LineageDatasetKey of(Role role, OpenLineage.Dataset dataset) {
    return dataset == null
        ? new LineageDatasetKey(role, null, null)
        : new LineageDatasetKey(role, dataset.getNamespace(), dataset.getName());
  }
}
