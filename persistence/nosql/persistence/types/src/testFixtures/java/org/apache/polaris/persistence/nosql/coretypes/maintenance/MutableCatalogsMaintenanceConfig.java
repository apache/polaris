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
package org.apache.polaris.persistence.nosql.coretypes.maintenance;

import java.util.Optional;

public class MutableCatalogsMaintenanceConfig implements CatalogsMaintenanceConfig {

  private static CatalogsMaintenanceConfig current =
      CatalogsMaintenanceConfig.BuildableCatalogsMaintenanceConfig.builder().build();

  public static void setCurrent(CatalogsMaintenanceConfig config) {
    current = config;
  }

  @Override
  public Optional<String> principalsRetain() {
    return current.principalsRetain();
  }

  @Override
  public Optional<String> principalRolesRetain() {
    return current.principalRolesRetain();
  }

  @Override
  public Optional<String> grantsRetain() {
    return current.grantsRetain();
  }

  @Override
  public Optional<String> immediateTasksRetain() {
    return current.immediateTasksRetain();
  }

  @Override
  public Optional<String> catalogsHistoryRetain() {
    return current.catalogsHistoryRetain();
  }

  @Override
  public Optional<String> catalogRolesRetain() {
    return current.catalogRolesRetain();
  }

  @Override
  public Optional<String> catalogStateRetain() {
    return current.catalogStateRetain();
  }

  @Override
  public Optional<String> catalogPoliciesRetain() {
    return current.catalogPoliciesRetain();
  }
}
