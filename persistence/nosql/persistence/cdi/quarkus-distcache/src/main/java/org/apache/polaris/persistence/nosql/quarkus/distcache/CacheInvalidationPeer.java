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

import static java.util.Objects.requireNonNull;

import java.util.OptionalInt;
import org.jspecify.annotations.NonNull;

/**
 * A peer that receives cache invalidations.
 *
 * <p>A peer without an explicit management port uses the sender's management port. This preserves
 * the behavior of DNS-based discovery, while other discovery implementations can provide ports for
 * individually addressed peers.
 */
public record CacheInvalidationPeer(@NonNull String host, OptionalInt managementPort) {

  public CacheInvalidationPeer {
    requireNonNull(host, "host");
    requireNonNull(managementPort, "managementPort");
    if (managementPort.isPresent()
        && (managementPort.getAsInt() < 1 || managementPort.getAsInt() > 65535)) {
      throw new IllegalArgumentException("managementPort must be between 1 and 65535");
    }
  }

  /** Creates a peer that uses the sender's management port. */
  public CacheInvalidationPeer(String host) {
    this(host, OptionalInt.empty());
  }

  /** Creates a peer with an explicitly advertised management port. */
  public CacheInvalidationPeer(String host, int managementPort) {
    this(host, OptionalInt.of(managementPort));
  }

  int managementPortOrDefault(int defaultManagementPort) {
    return managementPort.orElse(defaultManagementPort);
  }
}
