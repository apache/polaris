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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisGrantRecord;

/** Result of a load grants call */
public class LoadGrantsResult extends BaseResult {
  // true if success. If false, the caller should retry because of some concurrent change
  private final int grantsVersion;

  // null if not success. Else set of grants records on a securable or to a grantee
  private final List<PolarisGrantRecord> grantRecords;

  // null if not success. Else, for each grant record, list of securable or grantee entities
  private final List<PolarisBaseEntity> entities;

  /**
   * Constructor for an error
   *
   * @param errorCode error code, cannot be SUCCESS
   * @param extraInformation extra information
   */
  public LoadGrantsResult(@Nonnull ReturnStatus errorCode, @Nullable String extraInformation) {
    super(errorCode, extraInformation);
    this.grantsVersion = 0;
    this.grantRecords = null;
    this.entities = null;
  }

  /**
   * Constructor for success
   *
   * @param grantsVersion version of the grants
   * @param grantRecords set of grant records
   */
  public LoadGrantsResult(
      int grantsVersion,
      @Nonnull List<PolarisGrantRecord> grantRecords,
      List<PolarisBaseEntity> entities) {
    super(ReturnStatus.SUCCESS);
    this.grantsVersion = grantsVersion;
    this.grantRecords = grantRecords;
    this.entities = entities;
  }

  @JsonCreator
  private LoadGrantsResult(
      @JsonProperty("returnStatus") @Nonnull ReturnStatus returnStatus,
      @JsonProperty("extraInformation") String extraInformation,
      @JsonProperty("grantsVersion") int grantsVersion,
      @JsonProperty("grantRecords") List<PolarisGrantRecord> grantRecords,
      @JsonProperty("entities") List<PolarisBaseEntity> entities) {
    super(returnStatus, extraInformation);
    this.grantsVersion = grantsVersion;
    this.grantRecords = grantRecords;
    // old GS code might not serialize this argument
    this.entities = entities;
  }

  public int getGrantsVersion() {
    return grantsVersion;
  }

  public List<PolarisGrantRecord> getGrantRecords() {
    return grantRecords;
  }

  public List<PolarisBaseEntity> getEntities() {
    return entities;
  }

  @JsonIgnore
  public Map<Long, PolarisBaseEntity> getEntitiesAsMap() {
    return (this.getEntities() == null)
        ? null
        : this.getEntities().stream()
            .collect(Collectors.toMap(PolarisBaseEntity::getId, entity -> entity));
  }

  @Override
  public String toString() {
    return "LoadGrantsResult{"
        + "grantsVersion="
        + grantsVersion
        + ", grantRecords="
        + grantRecords
        + ", entities="
        + entities
        + ", returnStatus="
        + getReturnStatus()
        + '}';
  }
}
