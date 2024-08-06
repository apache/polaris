/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.core.persistence.resolver;

import io.polaris.core.entity.PolarisEntityType;

public class ResolverStatus {

  /**
   * Status code for the caller to know if all entities were resolved successfully or if resolution
   * failed. Anything but success is a failure
   */
  public enum StatusEnum {
    // success
    SUCCESS,

    // error, principal making the call does not exist
    CALLER_PRINCIPAL_DOES_NOT_EXIST,

    // error, the path could not be resolved. The payload of the status will provide the path and
    // the index in that
    // path for the segment of the path which could not be resolved
    PATH_COULD_NOT_BE_FULLY_RESOLVED,

    // error, an entity could not be resolved
    ENTITY_COULD_NOT_BE_RESOLVED,
  }

  private final StatusEnum status;

  // if status is ENTITY_COULD_NOT_BE_RESOLVED, will be set to the entity type which couldn't be
  // resolved
  private final PolarisEntityType failedToResolvedEntityType;

  // if status is ENTITY_COULD_NOT_BE_RESOLVED, will be set to the entity name which couldn't be
  // resolved
  private final String failedToResolvedEntityName;

  // if status is PATH_COULD_NOT_BE_FULLY_RESOLVED, path which we failed to resolve
  private final ResolverPath failedToResolvePath;

  // if status is PATH_COULD_NOT_BE_FULLY_RESOLVED, index in the path which we failed to
  // resolve
  private final int failedToResolvedEntityIndex;

  public ResolverStatus(StatusEnum status) {
    this.status = status;
    this.failedToResolvedEntityType = null;
    this.failedToResolvedEntityName = null;
    this.failedToResolvePath = null;
    this.failedToResolvedEntityIndex = 0;
  }

  public ResolverStatus(
      PolarisEntityType failedToResolvedEntityType, String failedToResolvedEntityName) {
    this.status = StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED;
    this.failedToResolvedEntityType = failedToResolvedEntityType;
    this.failedToResolvedEntityName = failedToResolvedEntityName;
    this.failedToResolvePath = null;
    this.failedToResolvedEntityIndex = 0;
  }

  public ResolverStatus(ResolverPath failedToResolvePath, int failedToResolvedEntityIndex) {
    this.status = StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED;
    this.failedToResolvedEntityType = null;
    this.failedToResolvedEntityName = null;
    this.failedToResolvePath = failedToResolvePath;
    this.failedToResolvedEntityIndex = failedToResolvedEntityIndex;
  }

  public StatusEnum getStatus() {
    return status;
  }

  public PolarisEntityType getFailedToResolvedEntityType() {
    return failedToResolvedEntityType;
  }

  public String getFailedToResolvedEntityName() {
    return failedToResolvedEntityName;
  }

  public ResolverPath getFailedToResolvePath() {
    return failedToResolvePath;
  }

  public int getFailedToResolvedEntityIndex() {
    return failedToResolvedEntityIndex;
  }
}
