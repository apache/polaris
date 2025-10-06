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

package org.apache.polaris.core.credentials.connection;

import org.apache.iceberg.aws.AwsProperties;

/**
 * A subset of Iceberg catalog properties recognized by Polaris.
 *
 * <p>Most of these properties are meant to initialize Catalog objects for accessing the remote
 * Catalog service.
 */
public enum ConnectionCredentialProperty {
  AWS_ACCESS_KEY_ID(String.class, AwsProperties.REST_ACCESS_KEY_ID, "the aws access key id", true),
  AWS_SECRET_ACCESS_KEY(
      String.class, AwsProperties.REST_SECRET_ACCESS_KEY, "the aws access key secret", true),
  AWS_SESSION_TOKEN(
      String.class, AwsProperties.REST_SESSION_TOKEN, "the aws scoped access token", true),
  EXPIRATION_TIME(
      Long.class,
      "expiration-time",
      "the expiration time for the access token, in milliseconds",
      false);

  private final Class valueType;
  private final String propertyName;
  private final String description;
  private final boolean isCredential;

  ConnectionCredentialProperty(
      Class valueType, String propertyName, String description, boolean isCredential) {
    this.valueType = valueType;
    this.propertyName = propertyName;
    this.description = description;
    this.isCredential = isCredential;
  }

  public String getPropertyName() {
    return propertyName;
  }

  public boolean isCredential() {
    return isCredential;
  }

  public boolean isExpirationTimestamp() {
    return this == EXPIRATION_TIME;
  }
}
