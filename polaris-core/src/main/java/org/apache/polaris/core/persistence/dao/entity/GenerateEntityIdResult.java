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
import org.apache.polaris.core.persistence.BaseResult;

/** the return for a generate new entity id */
public class GenerateEntityIdResult extends BaseResult {

  // null if not success
  private final Long id;

  /**
   * Constructor for an error
   *
   * @param errorCode error code, cannot be SUCCESS
   * @param extraInformation extra information
   */
  public GenerateEntityIdResult(
      @Nonnull ReturnStatus errorCode, @Nullable String extraInformation) {
    super(errorCode, extraInformation);
    this.id = null;
  }

  /**
   * Constructor for success
   *
   * @param id the new id which was generated
   */
  public GenerateEntityIdResult(@Nonnull Long id) {
    super(ReturnStatus.SUCCESS);
    this.id = id;
  }

  @JsonCreator
  private GenerateEntityIdResult(
      @JsonProperty("returnStatus") @Nonnull ReturnStatus returnStatus,
      @JsonProperty("extraInformation") @Nullable String extraInformation,
      @JsonProperty("id") @Nullable Long id) {
    super(returnStatus, extraInformation);
    this.id = id;
  }

  public Long getId() {
    return id;
  }
}
