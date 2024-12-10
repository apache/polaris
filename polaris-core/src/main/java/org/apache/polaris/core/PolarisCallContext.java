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

import jakarta.annotation.Nonnull;
import java.time.Clock;
import java.time.ZoneId;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;

/**
 * The Call context is allocated each time a new REST request is processed. It contains instances of
 * low-level services required to process that request
 */
public class PolarisCallContext {

  // meta store which is used to persist Polaris entity metadata
  private final PolarisMetaStoreSession metaStore;

  // diag services
  private final PolarisDiagnostics diagServices;

  private final PolarisConfigurationStore configurationStore;

  private final Clock clock;

  public PolarisCallContext(
      @Nonnull PolarisMetaStoreSession metaStore,
      @Nonnull PolarisDiagnostics diagServices,
      @Nonnull PolarisConfigurationStore configurationStore,
      @Nonnull Clock clock) {
    this.metaStore = metaStore;
    this.diagServices = diagServices;
    this.configurationStore = configurationStore;
    this.clock = clock;
  }

  public PolarisCallContext(
      @Nonnull PolarisMetaStoreSession metaStore, @Nonnull PolarisDiagnostics diagServices) {
    this.metaStore = metaStore;
    this.diagServices = diagServices;
    this.configurationStore = new PolarisConfigurationStore() {};
    this.clock = Clock.system(ZoneId.systemDefault());
  }

  public PolarisMetaStoreSession getMetaStore() {
    return metaStore;
  }

  public PolarisDiagnostics getDiagServices() {
    return diagServices;
  }

  public PolarisConfigurationStore getConfigurationStore() {
    return configurationStore;
  }

  public Clock getClock() {
    return clock;
  }
}
