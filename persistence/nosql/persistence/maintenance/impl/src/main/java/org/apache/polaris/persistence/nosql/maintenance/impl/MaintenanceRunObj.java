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
import java.util.function.LongSupplier;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceRunInformation;

/**
 * Holds information about one maintenance run.
 *
 * <p>This object is <em>eventually overwritten</em> with the final result of the maintenance run.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableMaintenanceRunObj.class)
@JsonDeserialize(as = ImmutableMaintenanceRunObj.class)
public interface MaintenanceRunObj extends Obj {

  ObjType TYPE = new MaintenanceRunObjType();

  @Override
  default ObjType type() {
    return TYPE;
  }

  MaintenanceRunInformation runInformation();

  static ImmutableMaintenanceRunObj.Builder builder() {
    return ImmutableMaintenanceRunObj.builder();
  }

  final class MaintenanceRunObjType extends AbstractObjType<MaintenanceRunObj> {
    public MaintenanceRunObjType() {
      super("mtr", "Maintenance Run", MaintenanceRunObj.class);
    }

    @Override
    public long cachedObjectExpiresAtMicros(Obj obj, LongSupplier clockMicros) {
      var mo = (MaintenanceRunObj) obj;
      if (mo.runInformation().finished().isPresent()) {
        return CACHE_UNLIMITED;
      }
      return NOT_CACHED;
    }
  }
}
