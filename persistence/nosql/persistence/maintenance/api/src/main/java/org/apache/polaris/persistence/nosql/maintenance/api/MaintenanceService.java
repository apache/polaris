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

import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus;

public interface MaintenanceService {
  /**
   * Generates a maintenance service run-specification containing realms in states {@link
   * RealmStatus#ACTIVE ACTIVE} and {@link RealmStatus#INACTIVE INACTIVE} as "to retain" and realms
   * in state {@link RealmStatus#PURGING PURGING} as "to purge".
   */
  @Nonnull
  MaintenanceRunSpec buildMaintenanceRunSpec();

  /**
   * Perform maintenance.
   *
   * @param maintenanceRunSpec define the mandatory run-specification, see {@link
   *     #buildMaintenanceRunSpec()}
   * @return information about the maintenance run
   */
  @Nonnull
  MaintenanceRunInformation performMaintenance(@Nonnull MaintenanceRunSpec maintenanceRunSpec);

  /**
   * Retrieve information about recent maintenance runs. The number of available elements is
   * configured via {@link MaintenanceConfig#retainedRuns()}.
   */
  @Nonnull
  List<MaintenanceRunInformation> maintenanceRunLog();
}
