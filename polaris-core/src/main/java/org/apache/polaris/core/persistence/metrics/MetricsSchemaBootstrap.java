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
package org.apache.polaris.core.persistence.metrics;

import com.google.common.annotations.Beta;

/**
 * Service Provider Interface (SPI) for bootstrapping the metrics schema.
 *
 * <p>This interface enables different persistence backends (JDBC, NoSQL, custom) to implement
 * metrics schema initialization in a way appropriate for their storage model. The metrics schema is
 * separate from the entity schema and can be bootstrapped independently.
 *
 * <p>Implementations should be idempotent - calling {@link #bootstrap(String)} multiple times on
 * the same realm should have no effect after the first successful call.
 *
 * <h3>Dependency Injection</h3>
 *
 * <p>This interface is designed to be injected via CDI (Contexts and Dependency Injection). The
 * deployment module should provide a {@code @Produces} method that creates the appropriate
 * implementation based on the configured persistence backend.
 *
 * <p>Metrics schema versioning is independent of entity schema versioning, allowing metrics to be
 * added to existing deployments without re-bootstrapping the entity schema.
 *
 * <h3>Usage</h3>
 *
 * <p>The metrics schema can be bootstrapped:
 *
 * <ul>
 *   <li>Independently via the {@code bootstrap-metrics} CLI command
 *   <li>Programmatically by injecting this interface and calling {@link #bootstrap(String)} or
 *       {@link #bootstrap(String, int)}
 * </ul>
 *
 * <p><b>Note:</b> This SPI is currently experimental. The API may change in future releases.
 *
 * @see MetricsPersistence
 */
@Beta
public interface MetricsSchemaBootstrap {

  /**
   * Bootstraps the metrics schema for the specified realm to the latest version.
   *
   * <p>This is a convenience method that delegates to {@link #bootstrap(String, int)} with the
   * latest version.
   *
   * @param realmId the realm identifier to bootstrap the metrics schema for
   * @throws RuntimeException if the bootstrap operation fails
   */
  default void bootstrap(String realmId) {
    bootstrap(realmId, getLatestVersion());
  }

  /**
   * Bootstraps the metrics schema for the specified realm to the specified target version.
   *
   * <p>This operation is idempotent - calling it multiple times on the same realm should have no
   * effect after the first successful call when already at the target version or higher.
   *
   * <p>If the schema is already bootstrapped at a lower version, this will upgrade to the target
   * version. If already at a higher version, this operation has no effect.
   *
   * <p>Implementations should:
   *
   * <ul>
   *   <li>Create the necessary tables/collections for storing metrics data
   *   <li>Create any required indexes for efficient querying
   *   <li>Record the metrics schema version for future migrations
   *   <li>Apply any migration scripts needed to upgrade from current to target version
   * </ul>
   *
   * @param realmId the realm identifier to bootstrap the metrics schema for
   * @param targetVersion the target schema version to bootstrap to
   * @throws RuntimeException if the bootstrap operation fails
   * @throws IllegalArgumentException if targetVersion is invalid (e.g., less than 1 or greater than
   *     latest version)
   */
  void bootstrap(String realmId, int targetVersion);

  /**
   * Checks if the metrics schema has been bootstrapped for the specified realm.
   *
   * @param realmId the realm identifier to check
   * @return {@code true} if the metrics schema is already bootstrapped, {@code false} otherwise
   */
  boolean isBootstrapped(String realmId);

  /**
   * Returns the current metrics schema version for the specified realm.
   *
   * @param realmId the realm identifier to check
   * @return the current schema version, or 0 if not bootstrapped
   */
  int getCurrentVersion(String realmId);

  /**
   * Returns the latest available metrics schema version.
   *
   * @return the latest schema version supported by this implementation
   */
  int getLatestVersion();
}
