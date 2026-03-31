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
import jakarta.annotation.Nullable;
import java.net.URI;
import java.util.List;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/** AWS S3 Tables storage configuration information. */
@PolarisImmutable
@JsonSerialize(as = ImmutableAwsS3TablesStorageConfigurationInfo.class)
@JsonDeserialize(as = ImmutableAwsS3TablesStorageConfigurationInfo.class)
@JsonTypeName("AwsS3TablesStorageConfigurationInfo")
public abstract class AwsS3TablesStorageConfigurationInfo extends PolarisStorageConfigurationInfo {

  public static ImmutableAwsS3TablesStorageConfigurationInfo.Builder builder() {
    return ImmutableAwsS3TablesStorageConfigurationInfo.builder();
  }

  @Override
  public StorageType getStorageType() {
    return StorageType.S3_TABLES;
  }

  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.aws.s3.S3FileIO";
  }

  /** IAM role ARN used to assume credentials for S3 Tables access. */
  @Nullable
  public abstract String getRoleARN();

  /** AWS region where the S3 Tables bucket resides. */
  @Nullable
  public abstract String getRegion();

  /** Optional external ID for STS AssumeRole trust policy. */
  @Nullable
  public abstract String getExternalId();

  /** KMS Key ARN for server-side encryption, used for writes, optional. */
  @Nullable
  public abstract String getCurrentKmsKey();

  /** List of allowed KMS Key ARNs for reading, optional. */
  @Nullable
  public abstract List<String> getAllowedKmsKeys();

  /** Optional STS endpoint URI override. */
  @Nullable
  public abstract String getStsEndpoint();

  @JsonIgnore
  @Nullable
  public URI getStsEndpointUri() {
    return getStsEndpoint() == null ? null : URI.create(getStsEndpoint());
  }

  @Value.Check
  @Override
  protected void check() {
    super.check();
    String arn = getRoleARN();
    if (arn != null) {
      AwsStorageConfigurationInfo.validateArn(arn);
    }
  }
}
