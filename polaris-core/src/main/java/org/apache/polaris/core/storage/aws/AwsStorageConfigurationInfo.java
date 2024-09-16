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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.OptionalInt;
import java.util.regex.Pattern;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value.Check;
import org.jetbrains.annotations.Nullable;

/** Aws Polaris Storage Configuration information */
@PolarisImmutable
@JsonSerialize(as = ImmutableAwsStorageConfigurationInfo.class)
@JsonDeserialize(as = ImmutableAwsStorageConfigurationInfo.class)
@JsonTypeName("AwsStorageConfigurationInfo")
public abstract class AwsStorageConfigurationInfo extends PolarisStorageConfigurationInfo {

  // 5 is the approximate max allowed locations for the size of AccessPolicy when LIST is required
  // for allowed read and write locations for subscoping creds.
  private static final int MAX_ALLOWED_LOCATIONS = 5;

  private static final String ROLE_ARN_PATTERN =
      "^arn:(aws|aws-cn|aws-us-gov):iam::\\d{12}:role/.+$";

  public static AwsStorageConfigurationInfo of(Iterable<String> allowedLocations, String roleARN) {
    return of(allowedLocations, roleARN, null);
  }

  public static AwsStorageConfigurationInfo of(
      Iterable<String> allowedLocations, String roleARN, @Nullable String externalId) {
    return ImmutableAwsStorageConfigurationInfo.builder()
        .allowedLocations(allowedLocations)
        .roleARN(roleARN)
        .externalId(externalId)
        .build();
  }

  @Override
  public abstract List<String> getAllowedLocations();

  @JsonIgnore
  @Override
  public final StorageType getStorageType() {
    return StorageType.S3;
  }

  @JsonIgnore
  @Override
  public final String getFileIoImplClassName() {
    return "org.apache.iceberg.aws.s3.S3FileIO";
  }

  /** AWS role to be assumed. */
  public abstract String getRoleARN();

  /** AWS external ID, optional. */
  @Nullable
  public abstract String getExternalId();

  @Nullable
  public abstract String getUserARN();

  @Override
  protected OptionalInt getMaxAllowedLocations() {
    return OptionalInt.of(MAX_ALLOWED_LOCATIONS);
  }

  @Check
  @Override
  protected void validate() {
    super.validate();
    validateArn();
  }

  private void validateArn() {
    String arn = getRoleARN();
    if (arn.isEmpty()) {
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
}
