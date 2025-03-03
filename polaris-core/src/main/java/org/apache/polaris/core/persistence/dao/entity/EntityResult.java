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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.persistence.BaseResult;

/** the return for an entity lookup call */
public class EntityResult extends BaseResult {

  // null if not success
  private final PolarisBaseEntity entity;

  /**
   * Constructor for an error
   *
   * @param errorCode error code, cannot be SUCCESS
   * @param extraInformation extra information if error. Implementation specific
   */
  public EntityResult(
      @Nonnull BaseResult.ReturnStatus errorCode, @Nullable String extraInformation) {
    super(errorCode, extraInformation);
    this.entity = null;
  }

  /**
   * Constructor for success
   *
   * @param entity the entity being looked-up
   */
  public EntityResult(@Nonnull PolarisBaseEntity entity) {
    super(ReturnStatus.SUCCESS);
    this.entity = entity;
  }

  /**
   * Constructor for an object already exists error where the subtype of the existing entity is
   * returned
   *
   * @param errorStatus error status, cannot be SUCCESS
   * @param subTypeCode existing entity subtype code
   */
  public EntityResult(@Nonnull BaseResult.ReturnStatus errorStatus, int subTypeCode) {
    super(errorStatus, Integer.toString(subTypeCode));
    this.entity = null;
  }

  /**
   * For object already exist error, we use the extra information to serialize the subtype code of
   * the existing object. Return the subtype
   *
   * @return object subtype or NULL (should not happen) if subtype code is missing or cannot be
   *     deserialized
   */
  @Nullable
  public PolarisEntitySubType getAlreadyExistsEntitySubType() {
    if (this.getExtraInformation() == null) {
      return null;
    } else {
      int subTypeCode;
      try {
        subTypeCode = Integer.parseInt(this.getExtraInformation());
      } catch (NumberFormatException e) {
        return null;
      }
      return PolarisEntitySubType.fromCode(subTypeCode);
    }
  }

  @JsonCreator
  private EntityResult(
      @JsonProperty("returnStatus") @Nonnull ReturnStatus returnStatus,
      @JsonProperty("extraInformation") @Nullable String extraInformation,
      @JsonProperty("entity") @Nullable PolarisBaseEntity entity) {
    super(returnStatus, extraInformation);
    this.entity = entity;
  }

  public PolarisBaseEntity getEntity() {
    return entity;
  }
}
