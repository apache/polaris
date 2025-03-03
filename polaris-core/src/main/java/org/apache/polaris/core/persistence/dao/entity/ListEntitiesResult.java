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
import org.apache.polaris.core.entity.EntityNameLookupRecord;

/** the return the result for a list entities call */
public class ListEntitiesResult extends BaseResult {

  // null if not success. Else the list of entities being returned
  private final List<EntityNameLookupRecord> entities;

  /**
   * Constructor for an error
   *
   * @param errorCode error code, cannot be SUCCESS
   * @param extraInformation extra information
   */
  public ListEntitiesResult(@Nonnull ReturnStatus errorCode, @Nullable String extraInformation) {
    super(errorCode, extraInformation);
    this.entities = null;
  }

  /**
   * Constructor for success
   *
   * @param entities list of entities being returned, implies success
   */
  public ListEntitiesResult(@Nonnull List<EntityNameLookupRecord> entities) {
    super(ReturnStatus.SUCCESS);
    this.entities = entities;
  }

  @JsonCreator
  private ListEntitiesResult(
      @JsonProperty("returnStatus") @Nonnull ReturnStatus returnStatus,
      @JsonProperty("extraInformation") String extraInformation,
      @JsonProperty("entities") List<EntityNameLookupRecord> entities) {
    super(returnStatus, extraInformation);
    this.entities = entities;
  }

  public List<EntityNameLookupRecord> getEntities() {
    return entities;
  }
}
