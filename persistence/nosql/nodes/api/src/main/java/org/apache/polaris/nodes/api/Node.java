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
package org.apache.polaris.nodes.api;

import jakarta.annotation.Nullable;
import java.time.Instant;
import org.apache.polaris.immutables.PolarisImmutable;

/** Represents the local node's ID and informative, mutable state information. */
@PolarisImmutable
public interface Node {
  /**
   * Returns the ID of this node.
   *
   * @return ID of this node
   * @throws IllegalStateException if the lease is no longer valid, for example, expired before it
   *     being renewed
   */
  int id();

  default boolean valid(long nowInMillis) {
    return nowInMillis < expirationTimestamp().toEpochMilli();
  }

  Instant leaseTimestamp();

  @Nullable
  Instant renewLeaseTimestamp();

  /** Timestamp since which this node's lease is no longer valid. */
  Instant expirationTimestamp();
}
