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
package org.apache.polaris.core.persistence.dao.entity;

import java.util.List;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** The result of applying an {@link ConditionalEntityWrite}. */
public class ConditionalEntityWriteResult extends BaseResult {

  // null if not success: the written entities, in the order of the batch's writes
  private final List<PolarisBaseEntity> entities;

  // set on failure when the entity that made the batch fail is known
  private final PolarisEntityCore failedEntity;

  /**
   * Constructor for success.
   *
   * @param entities the written entities, in the order of the batch's writes. Entities that were
   *     only checked are not included.
   */
  public ConditionalEntityWriteResult(@NonNull List<PolarisBaseEntity> entities) {
    super(ReturnStatus.SUCCESS);
    this.entities = List.copyOf(entities);
    this.failedEntity = null;
  }

  /**
   * Constructor for an error. Nothing was written.
   *
   * @param errorStatus error code, cannot be SUCCESS
   * @param extraInformation extra information
   * @param failedEntity the entity whose condition or write failed, if known
   */
  public ConditionalEntityWriteResult(
      @NonNull ReturnStatus errorStatus,
      @Nullable String extraInformation,
      @Nullable PolarisEntityCore failedEntity) {
    super(errorStatus, extraInformation);
    this.entities = null;
    this.failedEntity = failedEntity;
  }

  /** The written entities if the batch was applied, else null. */
  public @Nullable List<PolarisBaseEntity> getEntities() {
    return entities;
  }

  /** The entity that made the batch fail, when known. */
  public @Nullable PolarisEntityCore getFailedEntity() {
    return failedEntity;
  }
}
