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
package org.apache.polaris.core.storage.aws;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/** Aws Polaris Storage Configuration information */
@PolarisImmutable
@JsonSerialize(as = ImmutableAwsStorageConfigurationInfo.class)
@JsonDeserialize(as = ImmutableAwsStorageConfigurationInfo.class)
@JsonTypeName("AwsStorageConfigurationInfo")
public abstract class AwsStorageConfigurationInfo extends PolarisStorageConfigurationInfo {

  public static ImmutableAwsStorageConfigurationInfo.Builder builder() {
    return ImmutableAwsStorageConfigurationInfo.builder();
  }

  // Technically, it should be ^arn:(aws|aws-cn|aws-us-gov):iam::(\d{12}):role/.+$,
  @JsonIgnore
  public static final String ROLE_ARN_PATTERN = "^arn:(aws|aws-us-gov):iam::(\\d{12}):role/.+$";

  private static final Pattern ROLE_ARN_PATTERN_COMPILED = Pattern.compile(ROLE_ARN_PATTERN);

  @Override
  public StorageType getStorageType() {
    return StorageType.S3;
  }

  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.aws.s3.S3FileIO";
  }

  @Nullable
  public abstract String getRoleARN();

  /** AWS external ID, optional */
  @Nullable
  public abstract String getExternalId();

  /** User ARN for the service principal */
  @Nullable
  public abstract String getUserARN();

  /** AWS region */
  @Nullable
  public abstract String getRegion();

  /** Endpoint URI for S3 API calls */
  @Nullable
  public abstract String getEndpoint();

  /** Internal endpoint URI for S3 API calls */
  @Nullable
  public abstract String getEndpointInternal();

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

  /** Flag indicating whether path-style bucket access should be forced in S3 clients. */
  public abstract @Nullable Boolean getPathStyleAccess();

  /** Endpoint URI for STS API calls */
  @Nullable
  public abstract String getStsEndpoint();

  /** Returns the STS endpoint if set, defaulting to {@link #getEndpointUri()} otherwise. */
  @JsonIgnore
  @Nullable
  public URI getStsEndpointUri() {
    return getStsEndpoint() == null ? getInternalEndpointUri() : URI.create(getStsEndpoint());
  }

  @JsonIgnore
  @Nullable
  public String getAwsAccountId() {
    String arn = getRoleARN();
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
    String arn = getRoleARN();
    if (arn != null) {
      Matcher matcher = ROLE_ARN_PATTERN_COMPILED.matcher(arn);
      checkState(matcher.matches());
      return matcher.group(1);
    }
    return null;
  }

  @Value.Check
  @Override
  protected void check() {
    super.check();
    String arn = getRoleARN();
    validateArn(arn);
    if (arn != null) {
      Matcher matcher = ROLE_ARN_PATTERN_COMPILED.matcher(arn);
      if (!matcher.matches()) {
        throw new IllegalArgumentException("ARN does not match the expected role ARN pattern");
      }
    }
  }

  public static void validateArn(String arn) {
    if (arn == null) {
      return;
    }
    if (arn.isEmpty()) {
      throw new IllegalArgumentException("ARN must not be empty");
    }
    // specifically throw errors for China
    if (arn.contains("aws-cn")) {
      throw new IllegalArgumentException("AWS China is temporarily not supported");
    }
    checkArgument(Pattern.matches(ROLE_ARN_PATTERN, arn), "Invalid role ARN format: %s", arn);
  }
}
