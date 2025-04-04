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
package org.apache.polaris.extension.persistence.relational.jdbc;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import java.util.Optional;

@StaticInitSafe
@ConfigMapping(prefix = "polaris.persistence.relational")
public interface RelationalJdbcConfiguration {

  Optional<String> jdbcUrl();

  Optional<String> userName();

  Optional<String> password();

  Optional<String> driverClassName();

  /** The initial pool size of DBCP2 connection pool */
  Optional<Integer> initialPoolSize();

  /**
   * The maximum total number of idle and borrows connections that can be active at the same time.
   * Use a negative value for no limit.
   */
  Optional<Integer> maxTotal();

  /**
   * The maximum number of connections that can remain idle in the pool. Excess idle connections are
   * destroyed on return to the pool.
   */
  Optional<Integer> maxIdle();

  /**
   * Sets the minimum number of idle connections in the pool. The pool attempts to ensure that
   * minIdle connections are available when the idle object evictor runs. The value of this property
   * has no effect unless timeBetweenEvictionRunsMillis has a positive value.
   */
  Optional<Integer> minIdle();

  /**
   * The maximum number of milliseconds that the pool will wait (when there are no available
   * connections) for a connection to be returned before throwing an exception, or <= 0 to wait
   * indefinitely.
   */
  Optional<Long> maxWaitMillis();

  /**
   * The SQL query that will be used to validate connections from this pool before returning them to
   * the caller. If specified, this query MUST be an SQL SELECT statement that returns at least one
   * row.
   */
  Optional<String> validationQuery();

  /**
   * The number of milliseconds to sleep between runs of the idle object evictor thread. When
   * non-positive, no idle object evictor thread will be run.
   */
  Optional<Long> timeBetweenEvictionRunsMillis();

  /**
   * The minimum amount of time an object may sit idle in the pool before it is eligible for
   * eviction by the idle object evictor (if any).
   */
  Optional<Integer> minEvictableIdleTimeMillis();
}
