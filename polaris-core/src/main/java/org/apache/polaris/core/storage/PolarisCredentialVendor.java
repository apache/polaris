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
package org.apache.polaris.core.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.persistence.BaseResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Manage credentials for storage locations. */
public interface PolarisCredentialVendor {
  /**
   * Get a sub-scoped credentials for an entity against the provided allowed read and write
   * locations.
   *
   * @param callCtx the polaris call context
   * @param catalogId the catalog id
   * @param entityId the entity id
   * @param allowListOperation whether to allow LIST operation on the allowedReadLocations and
   *     allowedWriteLocations
   * @param allowedReadLocations a set of allowed to read locations
   * @param allowedWriteLocations a set of allowed to write locations
   * @return an enum map containing the scoped credentials
   */
  @NotNull
  ScopedCredentialsResult getSubscopedCredsForEntity(
      @NotNull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      boolean allowListOperation,
      @NotNull Set<String> allowedReadLocations,
      @NotNull Set<String> allowedWriteLocations);

  /**
   * Validate whether the entity has access to the locations with the provided target operations
   *
   * @param callCtx the polaris call context
   * @param catalogId the catalog id
   * @param entityId the entity id
   * @param actions a set of operation actions: READ/WRITE/LIST/DELETE/ALL
   * @param locations a set of locations to verify
   * @return a Map of {@code <location, validate result>}, a validate result value looks like this
   *     <pre>
   * {
   *   "status" : "failure",
   *   "actions" : {
   *     "READ" : {
   *       "message" : "The specified file was not found",
   *       "status" : "failure"
   *     },
   *     "DELETE" : {
   *       "message" : "One or more objects could not be deleted (Status Code: 200; Error Code: null)",
   *       "status" : "failure"
   *     },
   *     "LIST" : {
   *       "status" : "success"
   *     },
   *     "WRITE" : {
   *       "message" : "Access Denied (Status Code: 403; Error Code: AccessDenied)",
   *       "status" : "failure"
   *     }
   *   },
   *   "message" : "Some of the integration checks failed. Check the Polaris documentation for more information."
   * }
   * </pre>
   */
  @NotNull
  ValidateAccessResult validateAccessToLocations(
      @NotNull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      @NotNull Set<PolarisStorageActions> actions,
      @NotNull Set<String> locations);

  /** Result of a getSubscopedCredsForEntity() call */
  class ScopedCredentialsResult extends BaseResult {

    // null if not success. Else, set of name/value pairs for the credentials
    private final EnumMap<PolarisCredentialProperty, String> credentials;

    /**
     * Constructor for an error
     *
     * @param errorCode error code, cannot be SUCCESS
     * @param extraInformation extra information
     */
    public ScopedCredentialsResult(
        @NotNull BaseResult.ReturnStatus errorCode, @Nullable String extraInformation) {
      super(errorCode, extraInformation);
      this.credentials = null;
    }

    /**
     * Constructor for success
     *
     * @param credentials credentials
     */
    public ScopedCredentialsResult(
        @NotNull EnumMap<PolarisCredentialProperty, String> credentials) {
      super(BaseResult.ReturnStatus.SUCCESS);
      this.credentials = credentials;
    }

    @JsonCreator
    private ScopedCredentialsResult(
        @JsonProperty("returnStatus") @NotNull BaseResult.ReturnStatus returnStatus,
        @JsonProperty("extraInformation") String extraInformation,
        @JsonProperty("credentials") Map<String, String> credentials) {
      super(returnStatus, extraInformation);
      this.credentials = new EnumMap<>(PolarisCredentialProperty.class);
      if (credentials != null) {
        credentials.forEach(
            (k, v) -> this.credentials.put(PolarisCredentialProperty.valueOf(k), v));
      }
    }

    public EnumMap<PolarisCredentialProperty, String> getCredentials() {
      return credentials;
    }
  }

  /** Result of a validateAccessToLocations() call */
  class ValidateAccessResult extends BaseResult {

    // null if not success. Else, set of location/validationResult pairs for each location in the
    // set
    private final Map<String, String> validateResult;

    /**
     * Constructor for an error
     *
     * @param errorCode error code, cannot be SUCCESS
     * @param extraInformation extra information
     */
    public ValidateAccessResult(
        @NotNull BaseResult.ReturnStatus errorCode, @Nullable String extraInformation) {
      super(errorCode, extraInformation);
      this.validateResult = null;
    }

    /**
     * Constructor for success
     *
     * @param validateResult validate result
     */
    public ValidateAccessResult(@NotNull Map<String, String> validateResult) {
      super(BaseResult.ReturnStatus.SUCCESS);
      this.validateResult = validateResult;
    }

    @JsonCreator
    private ValidateAccessResult(
        @JsonProperty("returnStatus") @NotNull BaseResult.ReturnStatus returnStatus,
        @JsonProperty("extraInformation") String extraInformation,
        @JsonProperty("validateResult") Map<String, String> validateResult) {
      super(returnStatus, extraInformation);
      this.validateResult = validateResult;
    }

    public Map<String, String> getValidateResult() {
      return this.validateResult;
    }
  }
}
