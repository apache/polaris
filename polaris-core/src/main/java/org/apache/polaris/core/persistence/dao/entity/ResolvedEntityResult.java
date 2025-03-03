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
import java.util.List;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.BaseResult;

/**
 * Represents an entity with its grants. If we "refresh" a previously fetched entity, we will only
 * refresh the information which has changed, based on the version of the entity.
 */
public class ResolvedEntityResult extends BaseResult {

  // the entity itself if it was loaded
  private final @Nullable PolarisBaseEntity entity;

  // version for the grant records, in case the entity was not loaded
  private final int grantRecordsVersion;

  private final @Nullable List<PolarisGrantRecord> entityGrantRecords;

  /**
   * Constructor for an error
   *
   * @param errorCode error code, cannot be SUCCESS
   * @param extraInformation extra information
   */
  public ResolvedEntityResult(@Nonnull ReturnStatus errorCode, @Nullable String extraInformation) {
    super(errorCode, extraInformation);
    this.entity = null;
    this.entityGrantRecords = null;
    this.grantRecordsVersion = 0;
  }

  /**
   * Constructor with success
   *
   * @param entity the main entity
   * @param grantRecordsVersion the version of the grant records
   * @param entityGrantRecords the list of grant records
   */
  public ResolvedEntityResult(
      @Nullable PolarisBaseEntity entity,
      int grantRecordsVersion,
      @Nullable List<PolarisGrantRecord> entityGrantRecords) {
    super(ReturnStatus.SUCCESS);
    this.entity = entity;
    this.entityGrantRecords = entityGrantRecords;
    this.grantRecordsVersion = grantRecordsVersion;
  }

  @JsonCreator
  public ResolvedEntityResult(
      @JsonProperty("returnStatus") @Nonnull ReturnStatus returnStatus,
      @JsonProperty("extraInformation") String extraInformation,
      @Nullable @JsonProperty("entity") PolarisBaseEntity entity,
      @JsonProperty("grantRecordsVersion") int grantRecordsVersion,
      @Nullable @JsonProperty("entityGrantRecords") List<PolarisGrantRecord> entityGrantRecords) {
    super(returnStatus, extraInformation);
    this.entity = entity;
    this.entityGrantRecords = entityGrantRecords;
    this.grantRecordsVersion = grantRecordsVersion;
  }

  public @Nullable PolarisBaseEntity getEntity() {
    return entity;
  }

  public int getGrantRecordsVersion() {
    return grantRecordsVersion;
  }

  public @Nullable List<PolarisGrantRecord> getEntityGrantRecords() {
    return entityGrantRecords;
  }
}
