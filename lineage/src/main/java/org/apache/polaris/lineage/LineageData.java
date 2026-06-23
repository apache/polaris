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
package org.apache.polaris.lineage;

import java.util.Objects;
import java.util.OptionalLong;

/** Dataset metadata returned in a lineage query response. */
public record LineageData(
    OptionalLong catalogId,
    OptionalLong datasetId,
    String namespace,
    String name,
    String subType,
    OptionalLong createdAt,
    OptionalLong updatedAt) {
  public LineageData {
    Objects.requireNonNull(catalogId, "catalogId must be non-null");
    Objects.requireNonNull(datasetId, "datasetId must be non-null");
    Objects.requireNonNull(namespace, "namespace must be non-null");
    Objects.requireNonNull(name, "name must be non-null");
    Objects.requireNonNull(createdAt, "createdAt must be non-null");
    Objects.requireNonNull(updatedAt, "updatedAt must be non-null");
  }

  public LineageData(
      long catalogId,
      long datasetId,
      String namespace,
      String name,
      String subType,
      long createdAt,
      long updatedAt) {
    this(
        OptionalLong.of(catalogId),
        OptionalLong.of(datasetId),
        namespace,
        name,
        subType,
        OptionalLong.of(createdAt),
        OptionalLong.of(updatedAt));
  }
}
