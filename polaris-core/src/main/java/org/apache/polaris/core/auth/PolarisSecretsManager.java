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
package org.apache.polaris.core.auth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.BaseResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Manages secrets for Polaris principals. */
public interface PolarisSecretsManager {
  /**
   * Load the principal secrets given the client_id.
   *
   * @param callCtx call context
   * @param clientId principal client id
   * @return the secrets associated to that principal, including the entity id of the principal
   */
  @NotNull
  PrincipalSecretsResult loadPrincipalSecrets(
      @NotNull PolarisCallContext callCtx, @NotNull String clientId);

  /**
   * Rotate secrets
   *
   * @param callCtx call context
   * @param clientId principal client id
   * @param principalId id of the principal
   * @param reset true if the principal's secrets should be disabled and replaced with a one-time
   *     password. if the principal's secret is already a one-time password, this flag is
   *     automatically true
   * @param oldSecretHash main secret hash for the principal
   * @return the secrets associated to that principal amd the id of the principal
   */
  @NotNull
  PrincipalSecretsResult rotatePrincipalSecrets(
      @NotNull PolarisCallContext callCtx,
      @NotNull String clientId,
      long principalId,
      boolean reset,
      @NotNull String oldSecretHash);

  /** the result of load/rotate principal secrets */
  class PrincipalSecretsResult extends BaseResult {

    // principal client identifier and associated secrets. Null if error
    private final PolarisPrincipalSecrets principalSecrets;

    /**
     * Constructor for an error
     *
     * @param errorCode error code, cannot be SUCCESS
     * @param extraInformation extra information
     */
    public PrincipalSecretsResult(
        @NotNull BaseResult.ReturnStatus errorCode, @Nullable String extraInformation) {
      super(errorCode, extraInformation);
      this.principalSecrets = null;
    }

    /**
     * Constructor for success
     *
     * @param principalSecrets and associated secret information
     */
    public PrincipalSecretsResult(@NotNull PolarisPrincipalSecrets principalSecrets) {
      super(BaseResult.ReturnStatus.SUCCESS);
      this.principalSecrets = principalSecrets;
    }

    @JsonCreator
    private PrincipalSecretsResult(
        @JsonProperty("returnStatus") @NotNull BaseResult.ReturnStatus returnStatus,
        @JsonProperty("extraInformation") @Nullable String extraInformation,
        @JsonProperty("principalSecrets") @NotNull PolarisPrincipalSecrets principalSecrets) {
      super(returnStatus, extraInformation);
      this.principalSecrets = principalSecrets;
    }

    public PolarisPrincipalSecrets getPrincipalSecrets() {
      return principalSecrets;
    }
  }
}
