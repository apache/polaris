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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.SigV4AuthenticationParameters;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The internal persistence-object counterpart to SigV4AuthenticationParameters defined in the API
 * model.
 */
public class SigV4AuthenticationParametersDpo extends AuthenticationParametersDpo {

  // The aws IAM role arn assumed by polaris userArn when signing requests. Null when a named
  // server-side credential (storageName) is used instead.
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

  // An optional IAM session policy (JSON) attached to the STS AssumeRole request
  @JsonProperty(value = "sessionPolicy")
  private final String sessionPolicy;

  // Name of a server-side AWS credential configuration whose static keys sign requests directly.
  // Only the name is stored; the keys stay in server configuration.
  @JsonProperty(value = "storageName")
  private final String storageName;

  /** Role-assumption form, for AWS services reached through STS. */
  public SigV4AuthenticationParametersDpo(
      String roleArn,
      String roleSessionName,
      String externalId,
      String signingRegion,
      String signingName,
      String sessionPolicy) {
    this(roleArn, roleSessionName, externalId, signingRegion, signingName, sessionPolicy, null);
  }

  @JsonCreator
  public SigV4AuthenticationParametersDpo(
      @JsonProperty(value = "roleArn", required = false) @Nullable String roleArn,
      @JsonProperty(value = "roleSessionName", required = false) @Nullable String roleSessionName,
      @JsonProperty(value = "externalId", required = false) @Nullable String externalId,
      @JsonProperty(value = "signingRegion", required = true) String signingRegion,
      @JsonProperty(value = "signingName", required = false) @Nullable String signingName,
      @JsonProperty(value = "sessionPolicy", required = false) @Nullable String sessionPolicy,
      @JsonProperty(value = "storageName", required = false) @Nullable String storageName) {
    // Deliberately unvalidated: this constructor is also the deserialization entry point, and a
    // persistence object has to be able to read back anything that was ever written. Input is
    // validated in validateAuthenticationParameters() on the way in instead.
    super(AuthenticationType.SIGV4.getCode());
    this.roleArn = roleArn;
    this.roleSessionName = roleSessionName;
    this.externalId = externalId;
    this.signingRegion = signingRegion;
    this.signingName = signingName;
    this.sessionPolicy = sessionPolicy;
    this.storageName = storageName;
  }

  /**
   * Rejects a caller-supplied combination that is not usable: exactly one of roleArn or
   * storageName. Called on the write path, not on read.
   */
  public static void validateAuthenticationParameters(
      @Nullable String roleArn,
      @Nullable String storageName,
      @Nullable String roleSessionName,
      @Nullable String externalId,
      @Nullable String sessionPolicy) {
    boolean hasRoleArn = StringUtils.isNotEmpty(roleArn);
    boolean hasStorageName = StringUtils.isNotEmpty(storageName);
    Preconditions.checkArgument(
        hasRoleArn || hasStorageName, "Either roleArn or storageName must be provided");
    Preconditions.checkArgument(
        !(hasRoleArn && hasStorageName), "roleArn and storageName are mutually exclusive");
    // These only shape an STS AssumeRole request. Accepting them alongside storageName would
    // silently ignore them, so they are rejected rather than quietly dropped.
    Preconditions.checkArgument(
        !hasStorageName
            || (StringUtils.isEmpty(roleSessionName)
                && StringUtils.isEmpty(externalId)
                && StringUtils.isEmpty(sessionPolicy)),
        "roleSessionName, externalId and sessionPolicy apply to role assumption only and cannot be "
            + "combined with storageName");
  }

  public @Nullable String getRoleArn() {
    return roleArn;
  }

  public @Nullable String getStorageName() {
    return storageName;
  }

  @JsonIgnore
  public boolean usesStorageCredentials() {
    return StringUtils.isNotEmpty(storageName);
  }

  public @Nullable String getRoleSessionName() {
    return roleSessionName;
  }

  public @Nullable String getExternalId() {
    return externalId;
  }

  public @NonNull String getSigningRegion() {
    return signingRegion;
  }

  public @Nullable String getSigningName() {
    return signingName;
  }

  public @Nullable String getSessionPolicy() {
    return sessionPolicy;
  }

  @NonNull
  @Override
  public Map<String, String> asIcebergCatalogProperties(
      PolarisCredentialManager credentialManager) {
    // Return only metadata properties - credentials are handled by ConnectionCredentialVendor
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_SIGV4);
    builder.put(AwsProperties.REST_SIGNER_REGION, getSigningRegion());
    if (getSigningName() != null) {
      builder.put(AwsProperties.REST_SIGNING_NAME, getSigningName());
    }
    return builder.build();
  }

  @Override
  public @NonNull AuthenticationParameters asAuthenticationParametersModel() {
    return SigV4AuthenticationParameters.builder()
        .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.SIGV4)
        .setRoleArn(getRoleArn())
        .setRoleSessionName(getRoleSessionName())
        .setExternalId(getExternalId())
        .setSigningRegion(getSigningRegion())
        .setSigningName(getSigningName())
        .setSessionPolicy(getSessionPolicy())
        .setStorageName(getStorageName())
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
        .add("hasSessionPolicy", StringUtils.isNotEmpty(getSessionPolicy()))
        .add("storageName", getStorageName())
        .toString();
  }
}
