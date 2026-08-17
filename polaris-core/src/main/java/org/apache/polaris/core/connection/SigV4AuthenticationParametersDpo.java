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
import org.apache.polaris.core.secrets.SecretReference;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The internal persistence-object counterpart to SigV4AuthenticationParameters defined in the API
 * model.
 */
public class SigV4AuthenticationParametersDpo extends AuthenticationParametersDpo {

  // The aws IAM role arn assumed by polaris userArn when signing requests. Null when static
  // credentials are used instead.
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

  // Static access key id used for signing. Not a secret, so stored inline like OAuth's clientId.
  @JsonProperty(value = "accessKeyId")
  private final String accessKeyId;

  // Offloaded to the user secrets manager, the way OAuth's client secret is.
  @JsonProperty(value = "secretAccessKeyReference")
  private final SecretReference secretAccessKeyReference;

  /** Role-assumption form, for AWS services reached through STS. */
  public SigV4AuthenticationParametersDpo(
      String roleArn,
      String roleSessionName,
      String externalId,
      String signingRegion,
      String signingName,
      String sessionPolicy) {
    this(
        roleArn,
        roleSessionName,
        externalId,
        signingRegion,
        signingName,
        sessionPolicy,
        null,
        null);
  }

  @JsonCreator
  public SigV4AuthenticationParametersDpo(
      @JsonProperty(value = "roleArn", required = false) @Nullable String roleArn,
      @JsonProperty(value = "roleSessionName", required = false) @Nullable String roleSessionName,
      @JsonProperty(value = "externalId", required = false) @Nullable String externalId,
      @JsonProperty(value = "signingRegion", required = true) String signingRegion,
      @JsonProperty(value = "signingName", required = false) @Nullable String signingName,
      @JsonProperty(value = "sessionPolicy", required = false) @Nullable String sessionPolicy,
      @JsonProperty(value = "accessKeyId", required = false) @Nullable String accessKeyId,
      @JsonProperty(value = "secretAccessKeyReference", required = false)
          @Nullable SecretReference secretAccessKeyReference) {
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
    this.accessKeyId = accessKeyId;
    this.secretAccessKeyReference = secretAccessKeyReference;
  }

  /**
   * Rejects a caller-supplied combination that is not usable: exactly one of roleArn or a complete
   * static credential pair. Called on the write path, not on read.
   */
  public static void validateAuthenticationParameters(
      @Nullable String roleArn, @Nullable String accessKeyId, @Nullable String secretAccessKey) {
    boolean hasStaticCredentials = accessKeyId != null || secretAccessKey != null;
    Preconditions.checkArgument(
        !hasStaticCredentials || (accessKeyId != null && secretAccessKey != null),
        "accessKeyId and secretAccessKey must be provided together");
    Preconditions.checkArgument(
        StringUtils.isNotEmpty(roleArn) || hasStaticCredentials,
        "Either roleArn or static credentials (accessKeyId + secretAccessKey) must be provided");
    Preconditions.checkArgument(
        StringUtils.isEmpty(roleArn) || !hasStaticCredentials,
        "roleArn and static credentials (accessKeyId + secretAccessKey) are mutually exclusive");
  }

  public @Nullable String getRoleArn() {
    return roleArn;
  }

  public @Nullable String getAccessKeyId() {
    return accessKeyId;
  }

  public @Nullable SecretReference getSecretAccessKeyReference() {
    return secretAccessKeyReference;
  }

  @JsonIgnore
  public boolean hasStaticCredentials() {
    return accessKeyId != null && secretAccessKeyReference != null;
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
        // Secret access key deliberately not echoed back, as with OAuth's client secret
        .setAccessKeyId(getAccessKeyId())
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
        .add("accessKeyId", getAccessKeyId())
        .add("hasSecretAccessKey", getSecretAccessKeyReference() != null)
        .toString();
  }
}
