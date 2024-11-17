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
import java.util.List;
import java.util.regex.Pattern;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Aws Polaris Storage Configuration information */
public class AwsStorageConfigurationInfo extends PolarisStorageConfigurationInfo {

  // 5 is the approximate max allowed locations for the size of AccessPolicy when LIST is required
  // for allowed read and write locations for subscoping creds.
  @JsonIgnore private static final int MAX_ALLOWED_LOCATIONS = 5;

  // Technically, it should be ^arn:(aws|aws-cn|aws-us-gov):iam::\d{12}:role/.+$,
  @JsonIgnore public static final String ROLE_ARN_PATTERN = "^arn:aws:iam::\\d{12}:role/.+$";

  // AWS role to be assumed
  private final @NotNull String roleARN;

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
      @JsonProperty(value = "storageType", required = true) @NotNull StorageType storageType,
      @JsonProperty(value = "allowedLocations", required = true) @NotNull
          List<String> allowedLocations,
      @JsonProperty(value = "roleARN", required = true) @NotNull String roleARN,
      @JsonProperty(value = "region", required = false) @NotNull String region) {
    this(storageType, allowedLocations, roleARN, null, region);
  }

  public AwsStorageConfigurationInfo(
      @NotNull StorageType storageType,
      @NotNull List<String> allowedLocations,
      @NotNull String roleARN,
      @Nullable String externalId,
      @Nullable String region) {
    super(storageType, allowedLocations);
    this.roleARN = roleARN;
    this.externalId = externalId;
    this.region = region;
    validateMaxAllowedLocations(MAX_ALLOWED_LOCATIONS);
  }

  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.aws.s3.S3FileIO";
  }

  public void validateArn(String arn) {
    if (arn == null || arn.isEmpty()) {
      throw new IllegalArgumentException("ARN cannot be null or empty");
    }
    // specifically throw errors for China and Gov
    if (arn.contains("aws-cn") || arn.contains("aws-us-gov")) {
      throw new IllegalArgumentException("AWS China or Gov Cloud are temporarily not supported");
    }
    if (!Pattern.matches(ROLE_ARN_PATTERN, arn)) {
      throw new IllegalArgumentException("Invalid role ARN format");
    }
  }

  public @NotNull String getRoleARN() {
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
