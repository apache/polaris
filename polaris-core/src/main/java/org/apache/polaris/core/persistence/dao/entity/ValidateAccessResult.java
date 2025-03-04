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
import java.util.Map;

/** Result of a validateAccessToLocations() call */
public class ValidateAccessResult extends BaseResult {

  // null if not success. Else, set of location/validationResult pairs for each location in the
  // set
  private final Map<String, String> validateResult;

  /**
   * Constructor for an error
   *
   * @param errorCode error code, cannot be SUCCESS
   * @param extraInformation extra information
   */
  public ValidateAccessResult(@Nonnull ReturnStatus errorCode, @Nullable String extraInformation) {
    super(errorCode, extraInformation);
    this.validateResult = null;
  }

  /**
   * Constructor for success
   *
   * @param validateResult validate result
   */
  public ValidateAccessResult(@Nonnull Map<String, String> validateResult) {
    super(ReturnStatus.SUCCESS);
    this.validateResult = validateResult;
  }

  @JsonCreator
  private ValidateAccessResult(
      @JsonProperty("returnStatus") @Nonnull ReturnStatus returnStatus,
      @JsonProperty("extraInformation") String extraInformation,
      @JsonProperty("validateResult") Map<String, String> validateResult) {
    super(returnStatus, extraInformation);
    this.validateResult = validateResult;
  }

  public Map<String, String> getValidateResult() {
    return this.validateResult;
  }
}
