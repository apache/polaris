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
package org.apache.polaris.core.storage.s3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import java.util.List;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Polaris Storage Configuration information for an S3 Compatible solution, MinIO, Dell ECS... */
public class S3StorageConfigurationInfo extends PolarisStorageConfigurationInfo {

  // 5 is the approximate max allowed locations for the size of AccessPolicy when LIST is required
  // for allowed read and write locations for subscoping creds.
  @JsonIgnore private static final int MAX_ALLOWED_LOCATIONS = 5;
  private @NotNull CredsVendingStrategyEnum credsVendingStrategy;
  private @NotNull CredsCatalogAndClientStrategyEnum credsCatalogAndClientStrategy;
  private @NotNull String s3endpoint;
  private @NotNull Boolean s3pathStyleAccess;
  private @NotNull String s3CredentialsCatalogAccessKeyId;
  private @NotNull String s3CredentialsCatalogSecretAccessKey;
  private @Nullable String s3CredentialsClientAccessKeyId;
  private @Nullable String s3CredentialsClientSecretAccessKey;

  // Define how and what the catalog client will receive as credentials
  public static enum CredsVendingStrategyEnum {
    KEYS_SAME_AS_CATALOG,
    KEYS_DEDICATED_TO_CLIENT,
    TOKEN_WITH_ASSUME_ROLE;
  };

  // Define how the access and secret keys will be receive during the catalo creation, if
  // ENV_VAR_NAME, the variable must exist in the Polaris running environement - it is more secured,
  // but less dynamic
  public static enum CredsCatalogAndClientStrategyEnum {
    VALUE,
    ENV_VAR_NAME;
  };

  // Constructor
  @JsonCreator
  public S3StorageConfigurationInfo(
      @JsonProperty(value = "storageType", required = true) @NotNull StorageType storageType,
      @JsonProperty(value = "credsVendingStrategy", required = true) @NotNull
          CredsVendingStrategyEnum credsVendingStrategy,
      @JsonProperty(value = "credsCatalogAndClientStrategy", required = true) @NotNull
          CredsCatalogAndClientStrategyEnum credsCatalogAndClientStrategy,
      @JsonProperty(value = "s3Endpoint", required = true) @NotNull String s3Endpoint,
      @JsonProperty(value = "s3CredentialsCatalogAccessKeyId", required = true) @NotNull
          String s3CredentialsCatalogAccessKeyId,
      @JsonProperty(value = "s3CredentialsCatalogSecretAccessKey", required = true) @NotNull
          String s3CredentialsCatalogSecretAccessKey,
      @JsonProperty(value = "s3CredentialsClientAccessKeyId", required = false) @Nullable
          String s3CredentialsClientAccessKeyId,
      @JsonProperty(value = "s3CredentialsClientSecretAccessKey", required = false) @Nullable
          String s3CredentialsClientSecretAccessKey,
      @JsonProperty(value = "s3PathStyleAccess", required = false) @NotNull
          Boolean s3PathStyleAccess,
      @JsonProperty(value = "allowedLocations", required = true) @NotNull
          List<String> allowedLocations) {

    // Classic super and constructor stuff storing data in private internal properties
    super(storageType, allowedLocations);
    validateMaxAllowedLocations(MAX_ALLOWED_LOCATIONS);
    this.credsVendingStrategy =
        CredsVendingStrategyEnum.valueOf(
            CredsVendingStrategyEnum.class, credsVendingStrategy.name());
    this.credsCatalogAndClientStrategy =
        CredsCatalogAndClientStrategyEnum.valueOf(
            CredsCatalogAndClientStrategyEnum.class, credsCatalogAndClientStrategy.name());
    this.s3pathStyleAccess = s3PathStyleAccess;
    this.s3endpoint = s3Endpoint;

    // The constructor is called multiple time during catalog life
    // to do substitution only once, there is a basic if null test, otherwise affect the data from
    // the "Polaris cache storage"
    // this way the first time the value is retrived from the name of the variable
    // next time the getenv will try to retrive a variable but is using the value as a nome, it will
    // be null, we affect the value provided by "Polaris cache storage"
    if (CredsCatalogAndClientStrategyEnum.ENV_VAR_NAME.equals(credsCatalogAndClientStrategy)) {
      String cai = System.getenv(s3CredentialsCatalogAccessKeyId);
      String cas = System.getenv(s3CredentialsCatalogSecretAccessKey);
      String cli = System.getenv(s3CredentialsClientAccessKeyId);
      String cls = System.getenv(s3CredentialsClientSecretAccessKey);
      this.s3CredentialsCatalogAccessKeyId = (cai != null) ? cai : s3CredentialsCatalogAccessKeyId;
      this.s3CredentialsCatalogSecretAccessKey =
          (cas != null) ? cas : s3CredentialsCatalogSecretAccessKey;
      this.s3CredentialsClientAccessKeyId = (cli != null) ? cli : s3CredentialsClientAccessKeyId;
      this.s3CredentialsClientSecretAccessKey =
          (cls != null) ? cls : s3CredentialsClientSecretAccessKey;
    } else {
      this.s3CredentialsCatalogAccessKeyId = s3CredentialsCatalogAccessKeyId;
      this.s3CredentialsCatalogSecretAccessKey = s3CredentialsCatalogSecretAccessKey;
      this.s3CredentialsClientAccessKeyId = s3CredentialsClientAccessKeyId;
      this.s3CredentialsClientSecretAccessKey = s3CredentialsClientSecretAccessKey;
    }
  }

  public @NotNull CredsVendingStrategyEnum getCredsVendingStrategy() {
    return this.credsVendingStrategy;
  }

  public @NotNull CredsCatalogAndClientStrategyEnum getCredsCatalogAndClientStrategy() {
    return this.credsCatalogAndClientStrategy;
  }

  public @NotNull String getS3Endpoint() {
    return this.s3endpoint;
  }

  public @NotNull Boolean getS3PathStyleAccess() {
    return this.s3pathStyleAccess;
  }

  public @NotNull String getS3CredentialsCatalogAccessKeyId() {
    return this.s3CredentialsCatalogAccessKeyId;
  }

  public @NotNull String getS3CredentialsCatalogSecretAccessKey() {
    return this.s3CredentialsCatalogSecretAccessKey;
  }

  public @Nullable String getS3CredentialsClientAccessKeyId() {
    return this.s3CredentialsClientAccessKeyId;
  }

  public @Nullable String getS3CredentialsClientSecretAccessKey() {
    return this.s3CredentialsClientSecretAccessKey;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("storageType", getStorageType())
        .add("storageType", getStorageType().name())
        .add("allowedLocation", getAllowedLocations())
        .toString();
  }

  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.aws.s3.S3FileIO";
  }
}
