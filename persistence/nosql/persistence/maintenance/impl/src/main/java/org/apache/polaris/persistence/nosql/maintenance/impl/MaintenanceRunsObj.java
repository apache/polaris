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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;

@PolarisImmutable
@JsonSerialize(as = ImmutableMaintenanceRunsObj.class)
@JsonDeserialize(as = ImmutableMaintenanceRunsObj.class)
public interface MaintenanceRunsObj extends BaseCommitObj {

  String MAINTENANCE_RUNS_REF_NAME = "maintenance-runs";

  ObjType TYPE = new MaintenanceRunsObjType();

  static ImmutableMaintenanceRunsObj.Builder builder() {
    return ImmutableMaintenanceRunsObj.builder();
  }

  /**
   * The ID of the object holding the maintenance run information.
   *
   * <p>The {@linkplain MaintenanceRunObj#runInformation() maintenance run information} is
   * <em>not</em> included in this object, because {@link MaintenanceRunObj} is initially written as
   * "currently running" and then updated with the final state of the maintenance run. Updating the
   * {@link MaintenanceRunObj} is not great but okay, but updating a {@link BaseCommitObj} is an
   * absolute no-go.
   */
  ObjRef maintenanceRunId();

  @Override
  default ObjType type() {
    return TYPE;
  }

  final class MaintenanceRunsObjType extends AbstractObjType<MaintenanceRunsObj> {
    public MaintenanceRunsObjType() {
      super("mtrs", "Maintenance Runs", MaintenanceRunsObj.class);
    }
  }

  interface Builder extends BaseCommitObj.Builder<MaintenanceRunsObj, Builder> {}
}
