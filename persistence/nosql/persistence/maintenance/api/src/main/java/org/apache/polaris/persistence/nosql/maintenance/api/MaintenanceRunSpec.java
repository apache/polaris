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
package org.apache.polaris.persistence.nosql.maintenance.api;

import java.util.Set;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.Realms;
import org.immutables.value.Value;

/**
 * Configures a maintenance run.
 *
 * <p>Must specify both the realms to purge <em>and</em> the realms to retain. The two sets are
 * distinct to allow certain database specific and implementation detail optimizations. Existing
 * data of realms that are in neither of the sets {@linkplain #realmsToPurge() to purge} and
 * {@linkplain #realmsToProcess() to process} will be ignored, not processed at all.
 *
 * <p>Reserved realms, realm IDs that start with {@code ::}, except {@value Realms#SYSTEM_REALM_ID},
 * are considered to be "special" and are not processed, and all references and objects in those
 * realms are retained.
 */
@PolarisImmutable
public interface MaintenanceRunSpec {
  Set<String> realmsToPurge();

  Set<String> realmsToProcess();

  /** Whether to run maintenance on the system realm, defaults to {@code true}. */
  @Value.Default
  default boolean includeSystemRealm() {
    return true;
  }

  static ImmutableMaintenanceRunSpec.Builder builder() {
    return ImmutableMaintenanceRunSpec.builder();
  }
}
