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
import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.SigV4AuthenticationParameters;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.secrets.UserSecretsManager;

/**
 * The internal persistence-object counterpart to SigV4AuthenticationParameters defined in the API
 * model.
 */
public class SigV4AuthenticationParametersDpo extends AuthenticationParametersDpo {

  // The aws IAM role arn assumed by polaris userArn when signing requests
  @JsonProperty(value = "roleArn")
  private final String roleArn;

  // The session name used when assuming the role
  @JsonProperty(value = "roleSessionName")
  private final String roleSessionName;

  // An optional external id used to establish a trust relationship with AWS in the trust policy
  @JsonProperty(value = "externalId")
  private final String externalId;

  // Region to be used by the SigV4 protocol for signing requests
  @JsonProperty(value = "signingRegion")
  private final String signingRegion;

  // The service name to be used by the SigV4 protocol for signing requests, the default signing
  // name is "execute-api" is if not provided
  @JsonProperty(value = "signingName")
  private final String signingName;

  public SigV4AuthenticationParametersDpo(
      @JsonProperty(value = "roleArn", required = true) String roleArn,
      @JsonProperty(value = "roleSessionName", required = false) String roleSessionName,
      @JsonProperty(value = "externalId", required = false) String externalId,
      @JsonProperty(value = "signingRegion", required = true) String signingRegion,
      @JsonProperty(value = "signingName", required = false) String signingName) {
    super(AuthenticationType.SIGV4.getCode());
    this.roleArn = roleArn;
    this.roleSessionName = roleSessionName;
    this.externalId = externalId;
    this.signingRegion = signingRegion;
    this.signingName = signingName;
  }

  public @Nonnull String getRoleArn() {
    return roleArn;
  }

  public @Nullable String getRoleSessionName() {
    return roleSessionName;
  }

  public @Nullable String getExternalId() {
    return externalId;
  }

  public @Nonnull String getSigningRegion() {
    return signingRegion;
  }

  public @Nullable String getSigningName() {
    return signingName;
  }

  @Nonnull
  @Override
  public Map<String, String> asIcebergCatalogProperties(
      UserSecretsManager secretsManager, PolarisCredentialManager credentialManager) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_SIGV4);
    builder.put(AwsProperties.REST_SIGNER_REGION, getSigningRegion());
    if (getSigningName() != null) {
      builder.put(AwsProperties.REST_SIGNING_NAME, getSigningName());
    }
    // Connection credentials are handled by ConnectionConfigInfoDpo
    return builder.build();
  }

  @Override
  public @Nonnull AuthenticationParameters asAuthenticationParametersModel() {
    return SigV4AuthenticationParameters.builder()
        .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.SIGV4)
        .setRoleArn(getRoleArn())
        .setRoleSessionName(getRoleSessionName())
        .setExternalId(getExternalId())
        .setSigningRegion(getSigningRegion())
        .setSigningName(getSigningName())
        .build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("authenticationTypeCode", getAuthenticationTypeCode())
        .add("roleArn", getRoleArn())
        .add("roleSessionName", getRoleSessionName())
        .add("externalId", getExternalId())
        .add("signingRegion", getSigningRegion())
        .add("signingName", getSigningName())
        .toString();
  }
}
