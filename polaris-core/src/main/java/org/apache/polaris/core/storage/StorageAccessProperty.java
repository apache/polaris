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
package org.apache.polaris.core.storage;

import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.jspecify.annotations.Nullable;

/**
 * A subset of Iceberg catalog properties recognized by Polaris.
 *
 * <p>Most of these properties are meant to configure Iceberg FileIO objects for accessing data in
 * storage.
 */
public enum StorageAccessProperty {
  AWS_KEY_ID(
      String.class,
      "s3.access-key-id",
      "AWS access key ID from the STS session",
      true,
      StorageType.AWS),
  AWS_SECRET_KEY(
      String.class,
      "s3.secret-access-key",
      "AWS secret access key from the STS session",
      true,
      StorageType.AWS),
  AWS_TOKEN(
      String.class,
      "s3.session-token",
      "AWS session token from the STS session",
      true,
      StorageType.AWS),
  AWS_SESSION_TOKEN_EXPIRES_AT_MS(
      String.class,
      "s3.session-token-expires-at-ms",
      "expiration time of the session token, in milliseconds since the Unix epoch",
      true,
      true,
      StorageType.AWS),
  AWS_ENDPOINT(
      String.class,
      "s3.endpoint",
      "custom S3 endpoint URL; emitted for S3-compatible stores such as MinIO or Ozone, absent for native AWS S3",
      false,
      StorageType.AWS),
  AWS_PATH_STYLE_ACCESS(
      Boolean.class,
      "s3.path-style-access",
      "set to true when path-style addressing is required; emitted for S3-compatible stores, absent for native AWS S3",
      false,
      StorageType.AWS),
  CLIENT_REGION(
      String.class,
      "client.region",
      "AWS region to use for S3 and STS requests",
      false,
      StorageType.AWS),
  AWS_REFRESH_CREDENTIALS_ENDPOINT(
      String.class,
      AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT,
      "catalog endpoint the client can call to refresh vended credentials before they expire",
      false,
      false,
      StorageType.AWS),

  GCS_ACCESS_TOKEN(
      String.class, "gcs.oauth2.token", "downscoped OAuth2 access token", true, StorageType.GCS),
  GCS_ACCESS_TOKEN_EXPIRES_AT_MS(
      String.class,
      "gcs.oauth2.token-expires-at",
      "expiration time of the access token, in milliseconds since the Unix epoch",
      true,
      true,
      StorageType.GCS),
  GCS_REFRESH_CREDENTIALS_ENDPOINT(
      String.class,
      GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT,
      "catalog endpoint the client can call to refresh vended credentials before they expire",
      false,
      false,
      StorageType.GCS),

  AZURE_SAS_TOKEN_ACCOUNT_HOST(
      String.class,
      "adls.sas-token",
      "SAS token keyed by the full storage DNS name (e.g. `adls.sas-token.myaccount.dfs.core.windows.net`); consumed by Spark via Iceberg's ADLSFileIO",
      true,
      false,
      StorageType.AZURE,
      "account-host"),
  AZURE_SAS_TOKEN_ACCOUNT_NAME(
      String.class,
      "adls.sas-token",
      "SAS token keyed by the storage account name only (e.g. `adls.sas-token.myaccount`); consumed by Iceberg 1.7.x clients",
      true,
      false,
      StorageType.AZURE,
      "account-name"),
  AZURE_SAS_TOKEN_BARE(
      String.class,
      "adls.sas-token",
      "bare SAS token (no suffix); consumed by PyIceberg via adlfs/fsspec",
      true,
      StorageType.AZURE),
  AZURE_ACCOUNT_NAME(
      String.class,
      "adls.account-name",
      "storage account name; consumed by PyIceberg via adlfs/fsspec",
      true,
      StorageType.AZURE),
  AZURE_REFRESH_CREDENTIALS_ENDPOINT(
      String.class,
      AzureProperties.ADLS_REFRESH_CREDENTIALS_ENDPOINT,
      "catalog endpoint the client can call to refresh vended credentials before they expire",
      false,
      false,
      StorageType.AZURE),
  AZURE_SAS_TOKEN_EXPIRES_AT_MS(
      Long.class,
      "adls.sas-token-expires-at-ms",
      "expiration time of the SAS token keyed by the full storage DNS name, in milliseconds since the Unix epoch",
      true,
      true,
      StorageType.AZURE,
      "account-host");

  /** Groups a {@link StorageAccessProperty} by cloud storage provider. */
  public enum StorageType {
    AWS,
    AZURE,
    GCS
  }

  private final Class<?> valueType;
  private final String propertyName;
  private final String description;
  private final boolean isCredential;
  private final boolean isExpirationTimestamp;
  private final StorageType storageType;

  /**
   * For prefix properties the actual key is {@code propertyName + "." + runtimeSuffix}. This field
   * names what that suffix represents (e.g. {@code "account-host"}, {@code "account-name"}) and is
   * used only for documentation purposes.
   */
  private final String suffixLabel;

  StorageAccessProperty(
      Class<?> valueType,
      String propertyName,
      String description,
      boolean isCredential,
      StorageType storageType) {
    this(valueType, propertyName, description, isCredential, false, storageType, null);
  }

  StorageAccessProperty(
      Class<?> valueType,
      String propertyName,
      String description,
      boolean isCredential,
      boolean isExpirationTimestamp,
      StorageType storageType) {
    this(
        valueType,
        propertyName,
        description,
        isCredential,
        isExpirationTimestamp,
        storageType,
        null);
  }

  StorageAccessProperty(
      Class<?> valueType,
      String propertyName,
      String description,
      boolean isCredential,
      boolean isExpirationTimestamp,
      StorageType storageType,
      String suffixLabel) {
    this.valueType = valueType;
    this.propertyName = propertyName;
    this.description = description;
    this.isCredential = isCredential;
    this.isExpirationTimestamp = isExpirationTimestamp;
    this.storageType = storageType;
    this.suffixLabel = suffixLabel;
  }

  /**
   * Returns the property name base. For prefix properties (see {@link #isPrefixProperty()}) the
   * actual runtime key is {@code getPropertyName() + "." + runtimeSuffix}.
   */
  public String getPropertyName() {
    return propertyName;
  }

  public boolean isCredential() {
    return isCredential;
  }

  public boolean isExpirationTimestamp() {
    return isExpirationTimestamp;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  @SuppressWarnings("unused") // consumed by reflection for automatic documentation generation
  public Class<?> getValueType() {
    return valueType;
  }

  @SuppressWarnings("unused") // consumed by reflection for automatic documentation generation
  public String getDescription() {
    return description;
  }

  /** Returns {@code true} when the actual property key is formed by appending a runtime suffix. */
  @SuppressWarnings("unused") // consumed by reflection for automatic documentation generation
  public boolean isPrefixProperty() {
    return suffixLabel != null;
  }

  /**
   * For prefix properties, a human-readable label for the suffix (e.g. {@code "account-host"}).
   * Returns {@code null} for non-prefix properties.
   */
  @SuppressWarnings("unused") // consumed by reflection for automatic documentation generation
  @Nullable
  public String getSuffixLabel() {
    return suffixLabel;
  }
}
