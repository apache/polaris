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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;

/** Aws Polaris Storage Configuration information */
public class AwsStorageConfigurationInfo extends PolarisStorageConfigurationInfo {

  // Technically, it should be ^arn:(aws|aws-cn|aws-us-gov):iam::(\d{12}):role/.+$,
  @JsonIgnore
  public static final String ROLE_ARN_PATTERN = "^arn:(aws|aws-us-gov):iam::(\\d{12}):role/.+$";

  // AWS role to be assumed
  private final @Nonnull String roleARN;

  // AWS external ID, optional
  @JsonProperty(value = "externalId")
  private @Nullable String externalId = null;

  /** User ARN for the service principal */
  @JsonProperty(value = "userARN")
  private @Nullable String userARN = null;

  /** User ARN for the service principal */
  @JsonProperty(value = "region")
  private @Nullable String region = null;

  @JsonCreator
  public AwsStorageConfigurationInfo(
      @JsonProperty(value = "storageType", required = true) @Nonnull StorageType storageType,
      @JsonProperty(value = "allowedLocations", required = true) @Nonnull
          List<String> allowedLocations,
      @JsonProperty(value = "roleARN", required = true) @Nonnull String roleARN,
      @JsonProperty(value = "region", required = false) @Nullable String region) {
    this(storageType, allowedLocations, roleARN, null, region);
  }

  public AwsStorageConfigurationInfo(
      @Nonnull StorageType storageType,
      @Nonnull List<String> allowedLocations,
      @Nonnull String roleARN,
      @Nullable String externalId,
      @Nullable String region) {
    super(storageType, allowedLocations);
    this.roleARN = roleARN;
    this.externalId = externalId;
    this.region = region;
  }

  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.aws.s3.S3FileIO";
  }

  public static void validateArn(String arn) {
    if (arn == null || arn.isEmpty()) {
      throw new IllegalArgumentException("ARN cannot be null or empty");
    }
    // specifically throw errors for China
    if (arn.contains("aws-cn")) {
      throw new IllegalArgumentException("AWS China is temporarily not supported");
    }
    if (!Pattern.matches(ROLE_ARN_PATTERN, arn)) {
      throw new IllegalArgumentException("Invalid role ARN format");
    }
  }

  public @Nonnull String getRoleARN() {
    return roleARN;
  }

  public @Nullable String getExternalId() {
    return externalId;
  }

  public void setExternalId(@Nullable String externalId) {
    this.externalId = externalId;
  }

  public @Nullable String getUserARN() {
    return userARN;
  }

  public void setUserARN(@Nullable String userARN) {
    this.userARN = userARN;
  }

  public @Nullable String getRegion() {
    return region;
  }

  public void setRegion(@Nullable String region) {
    this.region = region;
  }

  @JsonIgnore
  public String getAwsAccountId() {
    return parseAwsAccountId(roleARN);
  }

  @JsonIgnore
  public String getAwsPartition() {
    return parseAwsPartition(roleARN);
  }

  private static String parseAwsAccountId(String arn) {
    validateArn(arn);
    Pattern pattern = Pattern.compile(ROLE_ARN_PATTERN);
    Matcher matcher = pattern.matcher(arn);
    if (matcher.matches()) {
      return matcher.group(2);
    } else {
      throw new IllegalArgumentException("ARN does not match the expected role ARN pattern");
    }
  }

  private static String parseAwsPartition(String arn) {
    validateArn(arn);
    Pattern pattern = Pattern.compile(ROLE_ARN_PATTERN);
    Matcher matcher = pattern.matcher(arn);
    if (matcher.matches()) {
      return matcher.group(1);
    } else {
      throw new IllegalArgumentException("ARN does not match the expected role ARN pattern");
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("storageType", getStorageType())
        .add("storageType", getStorageType().name())
        .add("roleARN", roleARN)
        .add("userARN", userARN)
        .add("externalId", externalId)
        .add("allowedLocation", getAllowedLocations())
        .add("region", region)
        .toString();
  }
}
