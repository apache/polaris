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
package org.apache.polaris.service.exception;

/** Stable operational categories for outbound BigLake federation failures. */
public enum BigLakeFailureCategory {
  CREDENTIAL_LOADING(false),
  TOKEN_ACQUISITION(false),
  AUTHENTICATION(false),
  AUTHORIZATION(false),
  QUOTA_PROJECT_CONFIGURATION(false),
  INVALID_REMOTE_CATALOG(false),
  INVALID_REMOTE_WAREHOUSE(false),
  UNSUPPORTED_OPERATION(false),
  THROTTLED(true),
  QUOTA_EXHAUSTED(true),
  TIMEOUT(true),
  DNS_FAILURE(true),
  TLS_FAILURE(true),
  CONNECTION_FAILURE(true),
  SERVICE_UNAVAILABLE(true),
  UNKNOWN(false);

  private final boolean retryable;

  BigLakeFailureCategory(boolean retryable) {
    this.retryable = retryable;
  }

  public boolean retryable() {
    return retryable;
  }
}
