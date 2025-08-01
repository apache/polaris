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
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.aws.s3.signer.S3V4RestSignerClient;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.gcp.GCPProperties;

/**
 * A subset of Iceberg catalog properties recognized by Polaris.
 *
 * <p>Most of these properties are meant to configure Iceberg FileIO objects for accessing data in
 * storage.
 */
public enum StorageAccessProperty {
  AWS_KEY_ID(String.class, "s3.access-key-id", "the aws access key id", true),
  AWS_SECRET_KEY(String.class, "s3.secret-access-key", "the aws access key secret", true),
  AWS_TOKEN(String.class, "s3.session-token", "the aws scoped access token", true),
  AWS_SESSION_TOKEN_EXPIRES_AT_MS(
      String.class,
      "s3.session-token-expires-at-ms",
      "the time the aws session token expires, in milliseconds",
      true,
      true),
  AWS_ENDPOINT(String.class, "s3.endpoint", "the S3 endpoint to use for requests", false),
  AWS_PATH_STYLE_ACCESS(
      Boolean.class, "s3.path-style-access", "whether to use S3 path style access", false),
  CLIENT_REGION(
      String.class,
      "client.region",
      "region to configure client for making requests to AWS",
      false),
  AWS_REFRESH_CREDENTIALS_ENDPOINT(
      String.class,
      AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT,
      "the endpoint to load vended credentials for a table from the catalog",
      false,
      false),
  AWS_REMOTE_SIGNING_ENABLED(
      Boolean.class,
      S3FileIOProperties.REMOTE_SIGNING_ENABLED,
      "whether to enable remote signing for S3 requests",
      false),
  AWS_REMOTE_SIGNER_URI(
      String.class,
      S3V4RestSignerClient.S3_SIGNER_URI,
      "the base URI for the remote signer service, used for signing S3 requests",
      false),
  AWS_REMOTE_SIGNER_ENDPOINT(
      String.class,
      S3V4RestSignerClient.S3_SIGNER_ENDPOINT,
      "the endpoint for the remote signer service, used for signing S3 requests",
      false),

  GCS_ACCESS_TOKEN(String.class, "gcs.oauth2.token", "the gcs scoped access token", true),
  GCS_ACCESS_TOKEN_EXPIRES_AT(
      String.class,
      "gcs.oauth2.token-expires-at",
      "the time the gcs access token expires, in milliseconds",
      true,
      true),
  GCS_REFRESH_CREDENTIALS_ENDPOINT(
      String.class,
      GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT,
      "the endpoint to load vended credentials for a table from the catalog",
      false,
      false),

  // Currently not using ACCESS TOKEN as the ResolvingFileIO is using ADLSFileIO for azure case and
  // it expects for SAS
  AZURE_ACCESS_TOKEN(String.class, "", "the azure scoped access token", true),
  AZURE_SAS_TOKEN(String.class, "adls.sas-token.", "an azure shared access signature token", true),
  AZURE_REFRESH_CREDENTIALS_ENDPOINT(
      String.class,
      AzureProperties.ADLS_REFRESH_CREDENTIALS_ENDPOINT,
      "the endpoint to load vended credentials for a table from the catalog",
      false,
      false),
  EXPIRATION_TIME(
      Long.class,
      "expiration-time",
      "the expiration time for the access token, in milliseconds",
      true,
      true),
  AZURE_SAS_TOKEN_EXPIRES_AT_MS_PREFIX(
      Long.class,
      AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX,
      "The expiration time for the access token, in milliseconds",
      true,
      true);

  private final Class valueType;
  private final String propertyName;
  private final String description;
  private final boolean isCredential;
  private final boolean isExpirationTimestamp;

  StorageAccessProperty(
      Class valueType, String propertyName, String description, boolean isCredential) {
    this(valueType, propertyName, description, isCredential, false);
  }

  StorageAccessProperty(
      Class valueType,
      String propertyName,
      String description,
      boolean isCredential,
      boolean isExpirationTimestamp) {
    this.valueType = valueType;
    this.propertyName = propertyName;
    this.description = description;
    this.isCredential = isCredential;
    this.isExpirationTimestamp = isExpirationTimestamp;
  }

  public String getPropertyName() {
    return propertyName;
  }

  public boolean isCredential() {
    return isCredential;
  }

  public boolean isExpirationTimestamp() {
    return isExpirationTimestamp;
  }
}
