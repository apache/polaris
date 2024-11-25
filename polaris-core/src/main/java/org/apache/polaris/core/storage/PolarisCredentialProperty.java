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

/** Enum of polaris supported credential properties */
public enum PolarisCredentialProperty {
  AWS_KEY_ID(String.class, "s3.access-key-id", "the aws access key id"),
  AWS_SECRET_KEY(String.class, "s3.secret-access-key", "the aws access key secret"),
  AWS_TOKEN(String.class, "s3.session-token", "the aws scoped access token"),
  CLIENT_REGION(
      String.class, "client.region", "region to configure client for making requests to AWS"),

  GCS_ACCESS_TOKEN(String.class, "gcs.oauth2.token", "the gcs scoped access token"),
  GCS_ACCESS_TOKEN_EXPIRES_AT(
      String.class,
      "gcs.oauth2.token-expires-at",
      "the time the gcs access token expires, in milliseconds"),

  // Currently not using ACCESS TOKEN as the ResolvingFileIO is using ADLSFileIO for azure case and
  // it expects for SAS
  AZURE_ACCESS_TOKEN(String.class, "", "the azure scoped access token"),
  AZURE_SAS_TOKEN(String.class, "adls.sas-token.", "an azure shared access signature token"),
  AZURE_ACCOUNT_HOST(
      String.class,
      "the azure storage account host",
      "the azure account name + endpoint that will append to the ADLS_SAS_TOKEN_PREFIX"),
  EXPIRATION_TIME(
      Long.class, "expiration-time", "the expiration time for the access token, in milliseconds");

  private final Class valueType;
  private final String propertyName;
  private final String description;

  /*
  s3.access-key-id`: id for for credentials that provide access to the data in S3
           - `s3.secret-access-key`: secret for credentials that provide access to data in S3
           - `s3.session-token
   */
  PolarisCredentialProperty(Class valueType, String propertyName, String description) {
    this.valueType = valueType;
    this.propertyName = propertyName;
    this.description = description;
  }

  public String getPropertyName() {
    return propertyName;
  }
}
