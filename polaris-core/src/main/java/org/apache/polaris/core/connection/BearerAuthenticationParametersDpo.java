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
package org.apache.polaris.core.connection;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import jakarta.annotation.Nonnull;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.BearerAuthenticationParameters;
import org.apache.polaris.core.secrets.SecretReference;

/**
 * The internal persistence-object counterpart to BearerAuthenticationParameters defined in the API
 * model.
 */
public class BearerAuthenticationParametersDpo extends AuthenticationParametersDpo {

  @JsonProperty(value = "bearerTokenReference")
  private final SecretReference bearerTokenReference;

  public BearerAuthenticationParametersDpo(
      @JsonProperty(value = "bearerTokenReference", required = true) @Nonnull
          SecretReference bearerTokenReference) {
    super(AuthenticationType.BEARER.getCode());
    this.bearerTokenReference = bearerTokenReference;
  }

  public @Nonnull SecretReference getBearerTokenReference() {
    return bearerTokenReference;
  }

  @Override
  public @Nonnull AuthenticationParameters asAuthenticationParametersModel() {
    return BearerAuthenticationParameters.builder()
        .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.BEARER)
        .build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("authenticationTypeCode", getAuthenticationTypeCode())
        .add("bearerTokenReference", getBearerTokenReference())
        .toString();
  }
}
