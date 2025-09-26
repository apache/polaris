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
package org.apache.polaris.persistence.nosql.maintenance.impl;

import static org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceConfig.DEFAULT_RETAINED_RUNS;
import static org.apache.polaris.persistence.nosql.maintenance.impl.MaintenanceRunsObj.MAINTENANCE_RUNS_REF_NAME;

import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceConfig;
import org.apache.polaris.persistence.nosql.maintenance.spi.CountDownPredicate;
import org.apache.polaris.persistence.nosql.maintenance.spi.PerRealmRetainedIdentifier;
import org.apache.polaris.persistence.nosql.maintenance.spi.RetainedCollector;

/** Retained-identifier for the maintenance service's own reference and objects. */
@SuppressWarnings("CdiInjectionPointsInspection")
@ApplicationScoped
class MaintenanceServiceRealmRetainedIdentifier implements PerRealmRetainedIdentifier {
  @Inject MaintenanceConfig maintenanceConfig;

  @Override
  public String name() {
    return "Maintenance service";
  }

  @Override
  public boolean identifyRetained(@Nonnull RetainedCollector collector) {
    if (!collector.isSystemRealm()) {
      return false;
    }

    collector.refRetain(
        MAINTENANCE_RUNS_REF_NAME,
        MaintenanceRunsObj.class,
        new CountDownPredicate<>(maintenanceConfig.retainedRuns().orElse(DEFAULT_RETAINED_RUNS)),
        maintenance -> collector.retainObject(maintenance.maintenanceRunId()));

    return true;
  }
}
