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
package org.apache.polaris.core.storage.cache;

import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.StorageAccessConfig;

/**
 * Cache key for vended storage credentials. Each backend defines its own implementation with the
 * fields that affect credential vending for that backend.
 *
 * <p>The cache calls {@link #load} on miss to mint a new {@link StorageAccessConfig}; that result
 * must be fully derived from the key's data fields (those participating in {@code equals} / {@code
 * hashCode}) plus app-scoped invariants carried as auxiliary fields. This guarantees that two
 * cached values whose keys are equal are logically equivalent.
 *
 * <p>Caffeine cache uses {@code equals()}/{@code hashCode()} for key lookup. Since Immutables
 * generates both based on the concrete class and its data fields, different backend parameter types
 * will never collide even when sharing the same cache.
 */
public interface StorageCredentialCacheKey {

  /** Realm config used to scope cache-duration settings for the resulting cache entry. */
  RealmConfig realmConfig();

  /** Mint a fresh {@link StorageAccessConfig} for this key. Called by the cache on miss. */
  StorageAccessConfig load();
}
