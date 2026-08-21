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
package org.apache.polaris.service.catalog.iceberg;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import jakarta.validation.constraints.Min;

/**
 * Configuration for the process-wide cache of table metadata JSON documents, keyed by metadata file
 * location. Metadata files are immutable, so cached entries never go stale.
 */
@ConfigMapping(prefix = "polaris.table-metadata-cache")
public interface TableMetadataCacheConfiguration {

  /**
   * Approximate upper bound on the heap used by cached table metadata JSON documents, in bytes,
   * including an estimate of per-entry overhead. Eviction may briefly lag writes, so budget this
   * cache with headroom alongside the other in-memory caches (such as the entity cache). Zero
   * disables caching.
   */
  @WithName("max-bytes")
  @WithDefault("134217728")
  @Min(0)
  long maxBytes();
}
