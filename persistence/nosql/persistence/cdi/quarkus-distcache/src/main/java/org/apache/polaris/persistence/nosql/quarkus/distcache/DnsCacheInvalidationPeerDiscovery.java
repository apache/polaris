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

import io.quarkus.arc.DefaultBean;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Default peer discovery implementation using DNS service names. */
@ApplicationScoped
@DefaultBean
class DnsCacheInvalidationPeerDiscovery implements CacheInvalidationPeerDiscovery {
  private final AddressResolver addressResolver;
  private final List<String> serviceNames;

  @Inject
  DnsCacheInvalidationPeerDiscovery(
      @SuppressWarnings("CdiInjectionPointsInspection") Vertx vertx,
      QuarkusDistributedCacheInvalidationsConfig config) {
    this.addressResolver = new AddressResolver(vertx, config.dnsQueryTimeout().toMillis());
    this.serviceNames = config.cacheInvalidationServiceNames().orElse(List.of());
  }

  @Override
  public boolean isConfigured() {
    return !serviceNames.isEmpty();
  }

  @Override
  public Future<List<CacheInvalidationPeer>> discoverPeers() {
    return addressResolver
        .resolveAll(serviceNames)
        .map(addresses -> addresses.stream().map(CacheInvalidationPeer::new).toList());
  }
}
