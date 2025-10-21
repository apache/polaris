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
import org.apache.iceberg.rest.auth.OAuth2Properties;

/**
 * A subset of Iceberg catalog properties recognized by Polaris.
 *
 * <p>Most of these properties are meant to initialize Catalog objects for accessing the remote
 * Catalog service.
 */
public enum CatalogAccessProperty {
  // OAuth
  OAUTH2_CREDENTIAL(String.class, OAuth2Properties.CREDENTIAL, "the OAuth2 credential", true),

  // Bearer
  BEARER_TOKEN(String.class, OAuth2Properties.TOKEN, "the bearer token", true),

  // SigV4
  AWS_ACCESS_KEY_ID(String.class, AwsProperties.REST_ACCESS_KEY_ID, "the aws access key id", true),
  AWS_SECRET_ACCESS_KEY(
      String.class, AwsProperties.REST_SECRET_ACCESS_KEY, "the aws secret access key", true),
  AWS_SESSION_TOKEN(String.class, AwsProperties.REST_SESSION_TOKEN, "the aws session token", true),
  AWS_SESSION_TOKEN_EXPIRES_AT_MS(
      Long.class,
      "rest.session-token-expires-at-ms",
      "the time the aws session token expires, in milliseconds",
      false,
      true),

  // Metadata
  EXPIRES_AT_MS(
      Long.class,
      "rest.expires-at-ms",
      "the expiration time for the access token or the credential, in milliseconds",
      false,
      true);

  private final Class valueType;
  private final String propertyName;
  private final String description;
  private final boolean isCredential;
  private final boolean isExpirationTimestamp;

  CatalogAccessProperty(
      Class valueType, String propertyName, String description, boolean isCredential) {
    this(valueType, propertyName, description, isCredential, false);
  }

  CatalogAccessProperty(
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
