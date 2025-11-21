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
package org.apache.polaris.persistence.nosql.impl.cache;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import org.apache.polaris.persistence.nosql.api.cache.CacheBackend;
import org.apache.polaris.persistence.nosql.api.cache.CacheConfig;

public final class PersistenceCaches {
  private PersistenceCaches() {}

  /** Produces a {@link CacheBackend} with the given maximum capacity. */
  public static CacheBackend newBackend(
      CacheConfig cacheConfig, Optional<MeterRegistry> meterRegistry) {
    return new CaffeineCacheBackend(cacheConfig, meterRegistry);
  }
}
