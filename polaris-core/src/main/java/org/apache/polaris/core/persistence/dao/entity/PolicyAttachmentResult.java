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
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;

/** result of an attach/detach operation */
public class PolicyAttachmentResult extends BaseResult {
  // null if not success
  private final PolarisPolicyMappingRecord mappingRecord;

  /**
   * Constructor for an error
   *
   * @param errorStatus error code, cannot be SUCCESS
   * @param extraInformation extra information
   */
  public PolicyAttachmentResult(
      @Nonnull ReturnStatus errorStatus, @Nullable String extraInformation) {
    super(errorStatus, extraInformation);
    this.mappingRecord = null;
  }

  /**
   * Constructor for an error
   *
   * @param errorStatus error code, cannot be SUCCESS
   * @param policyTypeCode existing policy mapping record's policy type code
   */
  public PolicyAttachmentResult(@Nonnull ReturnStatus errorStatus, int policyTypeCode) {
    super(errorStatus, Integer.toString(policyTypeCode));
    this.mappingRecord = null;
  }

  /**
   * Constructor for success
   *
   * @param mappingRecord policy mapping record being attached/detached
   */
  public PolicyAttachmentResult(@Nonnull PolarisPolicyMappingRecord mappingRecord) {
    super(ReturnStatus.SUCCESS);
    this.mappingRecord = mappingRecord;
  }

  @JsonCreator
  private PolicyAttachmentResult(
      @JsonProperty("returnStatus") @Nonnull ReturnStatus returnStatus,
      @JsonProperty("extraInformation") String extraInformation,
      @JsonProperty("policyMappingRecord") PolarisPolicyMappingRecord mappingRecord) {
    super(returnStatus, extraInformation);
    this.mappingRecord = mappingRecord;
  }

  public PolarisPolicyMappingRecord getPolicyMappingRecord() {
    return mappingRecord;
  }
}
