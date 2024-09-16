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
package org.apache.polaris.core;

import java.time.Clock;
import java.time.ZoneId;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;

/**
 * The Call context is allocated each time a new REST request is processed. It contains instances of
 * low-level services required to process that request
 */
@PolarisImmutable
public interface PolarisCallContext {

  @Value.Parameter(order = 0)
  PolarisMetaStoreSession getMetaStore();

  @Value.Parameter(order = 1)
  PolarisDiagnostics getDiagServices();

  @Value.Parameter(order = 2)
  PolarisConfigurationStore getConfigurationStore();

  @Value.Parameter(order = 3)
  Clock getClock();

  static PolarisCallContext of(
      @NotNull PolarisMetaStoreSession metaStoreSession,
      @NotNull PolarisDiagnostics diagServices,
      @NotNull PolarisConfigurationStore configurationStore,
      @NotNull Clock clock) {
    return ImmutablePolarisCallContext.of(
        metaStoreSession, diagServices, configurationStore, clock);
  }

  static PolarisCallContext of(
      @NotNull PolarisMetaStoreSession metaStore, @NotNull PolarisDiagnostics diagServices) {
    return of(
        metaStore,
        diagServices,
        new PolarisConfigurationStore() {},
        Clock.system(ZoneId.systemDefault()));
  }
}
