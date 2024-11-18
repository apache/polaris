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
package org.apache.polaris.core.entity;

import static org.apache.polaris.core.entity.PolarisEntityConstants.MAINTENANCE_PREFIX;

public enum PolarisMaintenanceProperties {
  COMPACTION(MAINTENANCE_PREFIX + "compaction"),
  SNAPSHOT_RETENTION(MAINTENANCE_PREFIX + "snapshot_expiration");

  private final String value;

  PolarisMaintenanceProperties(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  public static PolarisMaintenanceProperties fromValue(String value) {
    for (PolarisMaintenanceProperties property : PolarisMaintenanceProperties.values()) {
      if (property.value.equals(value)) {
        return property;
      }
    }
    throw new IllegalArgumentException("Unknown value: " + value);
  }
}
