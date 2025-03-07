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
package org.apache.polaris.core.storage.s3compatible;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;

/**
 * S3-Compatible Storage Configuration. This class holds the parameters needed to connect to
 * S3-compatible storage services such as MinIO, Ceph, Dell ECS, etc.
 */
public class S3CompatibleStorageConfigurationInfo extends PolarisStorageConfigurationInfo {

  // 5 is the approximate max allowed locations for the size of AccessPolicy when LIST is required
  // for allowed read and write locations for sub-scoping credentials.
  @JsonIgnore private static final int MAX_ALLOWED_LOCATIONS = 5;
  private final @Nonnull String s3Endpoint;
  private final @Nullable String s3ProfileName;
  private final @Nullable String s3CredentialsCatalogAccessKeyId;
  private final @Nullable String s3CredentialsCatalogSecretAccessKey;
  private final @Nonnull Boolean s3PathStyleAccess;
  private final @Nullable String s3Region;
  private final @Nullable String s3RoleArn;

  @JsonCreator
  public S3CompatibleStorageConfigurationInfo(
      @JsonProperty(value = "s3Endpoint", required = true) @Nonnull String s3Endpoint,
      @JsonProperty(value = "s3ProfileName", required = false) @Nullable String s3ProfileName,
      @JsonProperty(value = "s3CredentialsCatalogAccessKeyId", required = false) @Nullable
          String s3CredentialsCatalogAccessKeyId,
      @JsonProperty(value = "s3CredentialsCatalogSecretAccessKey", required = false) @Nullable
          String s3CredentialsCatalogSecretAccessKey,
      @JsonProperty(value = "s3PathStyleAccess", required = false, defaultValue = "false") @Nonnull
          Boolean s3PathStyleAccess,
      @JsonProperty(value = "s3Region", required = false) @Nullable String s3Region,
      @JsonProperty(value = "s3RoleArn", required = false) @Nullable String s3RoleArn,
      @JsonProperty(value = "allowedLocations", required = true) @Nonnull
          List<String> allowedLocations) {

    super(StorageType.S3_COMPATIBLE, allowedLocations);
    validateMaxAllowedLocations(MAX_ALLOWED_LOCATIONS);
    this.s3PathStyleAccess = s3PathStyleAccess;
    this.s3Endpoint = s3Endpoint;
    this.s3ProfileName = s3ProfileName;
    this.s3CredentialsCatalogAccessKeyId =
        (s3CredentialsCatalogAccessKeyId == null) ? "" : s3CredentialsCatalogAccessKeyId;
    this.s3CredentialsCatalogSecretAccessKey =
        (s3CredentialsCatalogSecretAccessKey == null) ? "" : s3CredentialsCatalogSecretAccessKey;
    this.s3Region = s3Region;
    this.s3RoleArn = (s3RoleArn == null) ? "" : s3RoleArn;
  }

  public @Nonnull String getS3Endpoint() {
    return this.s3Endpoint;
  }

  public @Nullable String getS3ProfileName() {
    return this.s3ProfileName;
  }

  public @Nonnull Boolean getS3PathStyleAccess() {
    return this.s3PathStyleAccess;
  }

  public @Nullable String getS3CredentialsCatalogAccessKeyId() {
    return this.s3CredentialsCatalogAccessKeyId;
  }

  public @Nullable String getS3CredentialsCatalogSecretAccessKey() {
    return this.s3CredentialsCatalogSecretAccessKey;
  }

  public @Nullable String getS3RoleArn() {
    return this.s3RoleArn;
  }

  public @Nullable String getS3Region() {
    return this.s3Region;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("storageType", getStorageType().name())
        .add("allowedLocation", getAllowedLocations())
        .add("s3Region", getS3Region())
        .add("s3RoleArn", getS3RoleArn())
        .add("s3PathStyleAccess", getS3PathStyleAccess())
        .add("s3Endpoint", getS3Endpoint())
        .add("s3ProfileName", getS3ProfileName())
        .toString();
  }

  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.aws.s3.S3FileIO";
  }
}
