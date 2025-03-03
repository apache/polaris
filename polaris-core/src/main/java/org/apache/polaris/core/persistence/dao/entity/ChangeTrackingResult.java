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
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.persistence.BaseResult;

/** Result of a loadEntitiesChangeTracking call */
public class ChangeTrackingResult extends BaseResult {

  // null if not success. Else, will be null if the grant to revoke was not found
  private final List<PolarisChangeTrackingVersions> changeTrackingVersions;

  /**
   * Constructor for an error
   *
   * @param errorCode error code, cannot be SUCCESS
   * @param extraInformation extra information
   */
  public ChangeTrackingResult(@Nonnull ReturnStatus errorCode, @Nullable String extraInformation) {
    super(errorCode, extraInformation);
    this.changeTrackingVersions = null;
  }

  /**
   * Constructor for success
   *
   * @param changeTrackingVersions change tracking versions
   */
  public ChangeTrackingResult(@Nonnull List<PolarisChangeTrackingVersions> changeTrackingVersions) {
    super(ReturnStatus.SUCCESS);
    this.changeTrackingVersions = changeTrackingVersions;
  }

  @JsonCreator
  private ChangeTrackingResult(
      @JsonProperty("returnStatus") @Nonnull ReturnStatus returnStatus,
      @JsonProperty("extraInformation") String extraInformation,
      @JsonProperty("changeTrackingVersions")
          List<PolarisChangeTrackingVersions> changeTrackingVersions) {
    super(returnStatus, extraInformation);
    this.changeTrackingVersions = changeTrackingVersions;
  }

  public List<PolarisChangeTrackingVersions> getChangeTrackingVersions() {
    return changeTrackingVersions;
  }
}
