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

import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.SnowflakeIdGenerator;

/**
 * API to lease node IDs, primarily to generate {@linkplain SnowflakeIdGenerator snowflake IDs}.
 *
 * <p>The default configuration for the snowflake IDs allows generation of 4096 IDs per millisecond
 * (12 sequence bits), which should be more than enough. As a consequence, it is very likely enough
 * to have only one ID generator per JVM, across all realms and catalogs.
 *
 * <p>Implementation is provided as an {@link ApplicationScoped @ApplicationScoped} bean
 */
public interface NodeManagement extends AutoCloseable {
  /**
   * Build a <em>new</em> and <em>independent</em> ID generator instance of a {@linkplain #lease()
   * leased node} using the given clock.
   *
   * <p>This function must only be called from {@link ApplicationScoped @ApplicationScoped} CDI
   * producers providing the same {@link IdGenerator} for the lifetime of the given {@link Node},
   * aka at most once for a {@link Node} instance.
   */
  IdGenerator buildIdGenerator(@Nonnull NodeLease leasedNode);

  /** The maximum number of concurrently leased nodes that are supported. */
  int maxNumberOfNodes();

  /** Retrieve information about a specific node. */
  Optional<Node> getNodeInfo(int nodeId);

  /** Get the persistence ID for a node by its ID. */
  long systemIdForNode(int nodeId);

  /**
   * Lease a node.
   *
   * <p>The implementation takes care of periodically renewing the lease. It is not necessary to
   * explicitly call {@link NodeLease#renew()}.
   *
   * <p>It is possible that the {@linkplain NodeLease#nodeIdIfValid() ID of the leased node} changes
   * over time.
   *
   * <p>Each invocation returns a new, independent lease.
   *
   * @return the leased node
   * @throws IllegalStateException if no node ID could be leased
   */
  @Nonnull
  NodeLease lease();
}
