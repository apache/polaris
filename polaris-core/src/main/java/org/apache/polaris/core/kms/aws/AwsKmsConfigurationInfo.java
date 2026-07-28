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
package org.apache.polaris.core.kms.aws;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.net.URI;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.polaris.core.kms.PolarisKmsConfigurationInfo;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;
import org.jspecify.annotations.Nullable;

/** AWS KMS configuration information. */
@PolarisImmutable
@JsonSerialize(as = ImmutableAwsKmsConfigurationInfo.class)
@JsonDeserialize(as = ImmutableAwsKmsConfigurationInfo.class)
@JsonTypeName("AwsKmsConfigurationInfo")
public abstract class AwsKmsConfigurationInfo extends PolarisKmsConfigurationInfo {

  private static final Pattern ROLE_ARN_PATTERN_COMPILED =
      Pattern.compile("^.+:(.*):.+:.*:(.*):.+$");

  public static ImmutableAwsKmsConfigurationInfo.Builder builder() {
    return ImmutableAwsKmsConfigurationInfo.builder();
  }

  @Override
  public KmsType getKmsType() {
    return KmsType.AWS;
  }

  /** AWS role ARN used by Polaris to access AWS KMS, optional when using node identity. */
  @Nullable
  public abstract String getRoleArn();

  /** AWS external ID, optional. */
  @Nullable
  public abstract String getExternalId();

  /** AWS region used by AWS KMS clients. */
  @Nullable
  public abstract String getRegion();

  /** AWS KMS endpoint URI, optional. */
  @Nullable
  public abstract String getEndpoint();

  /** AWS KMS endpoint URI for Polaris server-side access, optional. */
  @Nullable
  public abstract String getEndpointInternal();

  /** AWS STS endpoint URI, optional. */
  @Nullable
  public abstract String getStsEndpoint();

  /** KMS key ARNs this catalog is allowed to use for Iceberg table encryption. */
  public abstract List<String> getAllowedKeyArns();

  @JsonIgnore
  @Nullable
  public URI getEndpointUri() {
    return getEndpoint() == null ? null : URI.create(getEndpoint());
  }

  @JsonIgnore
  @Nullable
  public URI getInternalEndpointUri() {
    return getEndpointInternal() == null ? getEndpointUri() : URI.create(getEndpointInternal());
  }

  /** Returns the STS endpoint if set, defaulting to the internal KMS endpoint otherwise. */
  @JsonIgnore
  @Nullable
  public URI getStsEndpointUri() {
    return getStsEndpoint() == null ? getInternalEndpointUri() : URI.create(getStsEndpoint());
  }

  @JsonIgnore
  @Nullable
  public String getAwsAccountId() {
    String arn = getRoleArn();
    if (arn != null) {
      Matcher matcher = ROLE_ARN_PATTERN_COMPILED.matcher(arn);
      checkState(matcher.matches());
      return matcher.group(2);
    }
    return null;
  }

  @JsonIgnore
  @Nullable
  public String getAwsPartition() {
    String arn = getRoleArn();
    if (arn != null) {
      Matcher matcher = ROLE_ARN_PATTERN_COMPILED.matcher(arn);
      checkState(matcher.matches());
      return matcher.group(1);
    }
    return null;
  }

  @Value.Check
  protected void check() {
    String arn = getRoleArn();
    validateRoleArn(arn);
    if (arn != null) {
      Matcher matcher = ROLE_ARN_PATTERN_COMPILED.matcher(arn);
      if (!matcher.matches()) {
        throw new IllegalArgumentException("ARN does not match the expected role ARN pattern");
      }
    }
  }

  public static void validateRoleArn(String arn) {
    if (arn == null) {
      return;
    }
    if (arn.isEmpty()) {
      throw new IllegalArgumentException("ARN must not be empty");
    }
    checkArgument(
        ROLE_ARN_PATTERN_COMPILED.matcher(arn).matches(), "Invalid role ARN format: %s", arn);
  }
}
