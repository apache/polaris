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

import java.time.Instant;
import org.apache.polaris.core.storage.AccessConfig;

/** A storage credential cached entry. */
public class StorageCredentialCacheEntry {
  /** The scoped creds map that is fetched from a creds vending service */
  public final AccessConfig accessConfig;

  private final long maxCacheDurationMs;

  public StorageCredentialCacheEntry(AccessConfig accessConfig, long maxCacheDurationMs) {
    this.accessConfig = accessConfig;
    this.maxCacheDurationMs = maxCacheDurationMs;
  }

  public long getMaxCacheDurationMs() {
    return maxCacheDurationMs;
  }

  /** Get the expiration time in millisecond for the cached entry */
  public long getExpirationTime() {
    return accessConfig.expiresAt().map(Instant::toEpochMilli).orElse(Long.MAX_VALUE);
  }

  /**
   * Get the map of string creds that is needed for the query engine.
   *
   * @return a map of string representing the subscoped creds info.
   */
  AccessConfig toAccessConfig() {
    return accessConfig;
  }
}
