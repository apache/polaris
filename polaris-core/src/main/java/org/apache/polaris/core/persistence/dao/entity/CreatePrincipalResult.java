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
import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PrincipalEntity;

/** the return the result of a create-principal method */
public class CreatePrincipalResult extends BaseResult {
  // the principal which has been created. Null if error
  private final PolarisBaseEntity principal;

  // principal client identifier and associated secrets. Null if error
  private final PolarisPrincipalSecrets principalSecrets;

  /**
   * Constructor for an error
   *
   * @param errorCode error code, cannot be SUCCESS
   * @param extraInformation extra information
   */
  public CreatePrincipalResult(@Nonnull ReturnStatus errorCode, @Nullable String extraInformation) {
    super(errorCode, extraInformation);
    this.principal = null;
    this.principalSecrets = null;
    validate();
  }

  /**
   * Constructor for success
   *
   * @param principal the principal
   * @param principalSecrets and associated secret information
   */
  public CreatePrincipalResult(
      @Nonnull PolarisBaseEntity principal, @Nonnull PolarisPrincipalSecrets principalSecrets) {
    super(ReturnStatus.SUCCESS);
    this.principal = principal;
    this.principalSecrets = principalSecrets;
    validate();
  }

  @JsonCreator
  private CreatePrincipalResult(
      @JsonProperty("returnStatus") @Nonnull ReturnStatus returnStatus,
      @JsonProperty("extraInformation") @Nullable String extraInformation,
      @JsonProperty("principal") @Nonnull PolarisBaseEntity principal,
      @JsonProperty("principalSecrets") @Nonnull PolarisPrincipalSecrets principalSecrets) {
    super(returnStatus, extraInformation);
    this.principal = principal;
    this.principalSecrets = principalSecrets;
    validate();
  }

  private void validate() {
    if (getReturnStatus() == ReturnStatus.SUCCESS) {
      Preconditions.checkNotNull(principal);
      Preconditions.checkNotNull(principalSecrets);
      Preconditions.checkState(getPrincipal() != null, "Entity is not a Principal");
    } else {
      Preconditions.checkState(principal == null);
      Preconditions.checkState(principalSecrets == null);
    }
  }

  public PrincipalEntity getPrincipal() {
    return PrincipalEntity.of(principal);
  }

  public PolarisPrincipalSecrets getPrincipalSecrets() {
    return principalSecrets;
  }
}
