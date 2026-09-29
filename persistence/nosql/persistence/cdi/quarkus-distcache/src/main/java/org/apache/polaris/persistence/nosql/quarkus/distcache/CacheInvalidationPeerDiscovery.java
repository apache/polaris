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
package org.apache.polaris.persistence.nosql.quarkus.distcache;

import io.vertx.core.Future;
import java.util.List;

/**
 * Discovers peers that receive distributed cache invalidations.
 *
 * <p>This is an implementation-level CDI integration point. Applications can replace the default
 * DNS-based bean with an implementation that obtains peers from their deployment environment.
 */
public interface CacheInvalidationPeerDiscovery {

  /** Whether this discovery implementation has been configured to discover peers. */
  boolean isConfigured();

  /** Obtains the current peer snapshot. A successful empty result means that no peers exist yet. */
  Future<List<CacheInvalidationPeer>> discoverPeers();
}
