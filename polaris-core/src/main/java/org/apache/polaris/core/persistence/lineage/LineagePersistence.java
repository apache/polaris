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
package org.apache.polaris.core.persistence.lineage;

import com.google.common.annotations.Beta;
import jakarta.annotation.Nonnull;

/** Service Provider Interface for persisting collapsed OpenLineage graph records. */
@Beta
public interface LineagePersistence {
  default void upsertLineageDataset(@Nonnull LineageDatasetRecord record) {
    // No-op by default - backends that don't support lineage silently ignore.
  }

  default void upsertLineageEdge(@Nonnull LineageEdgeRecord record) {
    // No-op by default - backends that don't support lineage silently ignore.
  }

  default void upsertLineageColumnEdge(@Nonnull LineageColumnEdgeRecord record) {
    // No-op by default - backends that don't support lineage silently ignore.
  }
}
