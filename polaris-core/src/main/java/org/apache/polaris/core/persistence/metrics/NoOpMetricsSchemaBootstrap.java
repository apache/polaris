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

/**
 * A no-op implementation of {@link MetricsSchemaBootstrap} for backends that don't support metrics
 * schema bootstrapping.
 *
 * <p>This implementation is used for persistence backends (like NoSQL) that don't have a separate
 * metrics schema to bootstrap. All operations are no-ops or return default values.
 */
public final class NoOpMetricsSchemaBootstrap implements MetricsSchemaBootstrap {

  /** Singleton instance. */
  public static final NoOpMetricsSchemaBootstrap INSTANCE = new NoOpMetricsSchemaBootstrap();

  private NoOpMetricsSchemaBootstrap() {}

  @Override
  public void bootstrap(String realmId, int targetVersion) {
    // No-op - NoSQL backends don't have a metrics schema to bootstrap
  }

  @Override
  public boolean isBootstrapped(String realmId) {
    // Always return true since there's no schema to bootstrap
    return true;
  }

  @Override
  public int getCurrentVersion(String realmId) {
    // Return 0 since there's no versioned schema
    return 0;
  }

  @Override
  public int getLatestVersion() {
    // Return 0 since there's no versioned schema
    return 0;
  }
}
