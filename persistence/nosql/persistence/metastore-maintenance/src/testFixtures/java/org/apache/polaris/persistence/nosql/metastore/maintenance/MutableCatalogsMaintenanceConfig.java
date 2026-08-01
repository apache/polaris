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
package org.apache.polaris.persistence.nosql.metastore.maintenance;

import java.time.Duration;

public class MutableCatalogsMaintenanceConfig implements CatalogsMaintenanceConfig {

  private static CatalogsMaintenanceConfig current =
      CatalogsMaintenanceConfig.BuildableCatalogsMaintenanceConfig.builder().build();

  public static void setCurrent(CatalogsMaintenanceConfig config) {
    current = config;
  }

  @Override
  public Duration paginationTokenRetention() {
    return current.paginationTokenRetention();
  }

  @Override
  public int principalsRetain() {
    return current.principalsRetain();
  }

  @Override
  public Duration principalsRetainDuration() {
    return current.principalsRetainDuration();
  }

  @Override
  public boolean principalsRetainAll() {
    return current.principalsRetainAll();
  }

  @Override
  public int principalRolesRetain() {
    return current.principalRolesRetain();
  }

  @Override
  public Duration principalRolesRetainDuration() {
    return current.principalRolesRetainDuration();
  }

  @Override
  public boolean principalRolesRetainAll() {
    return current.principalRolesRetainAll();
  }

  @Override
  public int grantsRetain() {
    return current.grantsRetain();
  }

  @Override
  public Duration grantsRetainDuration() {
    return current.grantsRetainDuration();
  }

  @Override
  public boolean grantsRetainAll() {
    return current.grantsRetainAll();
  }

  @Override
  public int immediateTasksRetain() {
    return current.immediateTasksRetain();
  }

  @Override
  public Duration immediateTasksRetainDuration() {
    return current.immediateTasksRetainDuration();
  }

  @Override
  public boolean immediateTasksRetainAll() {
    return current.immediateTasksRetainAll();
  }

  @Override
  public int catalogsHistoryRetain() {
    return current.catalogsHistoryRetain();
  }

  @Override
  public Duration catalogsHistoryRetainDuration() {
    return current.catalogsHistoryRetainDuration();
  }

  @Override
  public boolean catalogsHistoryRetainAll() {
    return current.catalogsHistoryRetainAll();
  }

  @Override
  public int catalogRolesRetain() {
    return current.catalogRolesRetain();
  }

  @Override
  public Duration catalogRolesRetainDuration() {
    return current.catalogRolesRetainDuration();
  }

  @Override
  public boolean catalogRolesRetainAll() {
    return current.catalogRolesRetainAll();
  }

  @Override
  public int catalogStateRetain() {
    return current.catalogStateRetain();
  }

  @Override
  public Duration catalogStateRetainDuration() {
    return current.catalogStateRetainDuration();
  }

  @Override
  public boolean catalogStateRetainAll() {
    return current.catalogStateRetainAll();
  }

  @Override
  public int catalogPoliciesRetain() {
    return current.catalogPoliciesRetain();
  }

  @Override
  public Duration catalogPoliciesRetainDuration() {
    return current.catalogPoliciesRetainDuration();
  }

  @Override
  public boolean catalogPoliciesRetainAll() {
    return current.catalogPoliciesRetainAll();
  }
}
