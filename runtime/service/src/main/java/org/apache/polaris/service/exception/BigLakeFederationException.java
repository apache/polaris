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

import org.jspecify.annotations.Nullable;

/** A sanitized, actionable Polaris exception for outbound BigLake federation failures. */
public class BigLakeFederationException extends RuntimeException {
  private final BigLakeFailureCategory category;
  private final String errorType;
  private final int responseCode;
  private final int remoteStatus;
  private final @Nullable String googleRequestId;
  private final @Nullable String retryAfter;
  private final int retryCount;

  public BigLakeFederationException(
      BigLakeFailureCategory category,
      String errorType,
      int responseCode,
      int remoteStatus,
      @Nullable String googleRequestId,
      @Nullable String retryAfter,
      int retryCount,
      String message,
      Throwable cause) {
    super(message, cause);
    this.category = category;
    this.errorType = errorType;
    this.responseCode = responseCode;
    this.remoteStatus = remoteStatus;
    this.googleRequestId = googleRequestId;
    this.retryAfter = retryAfter;
    this.retryCount = retryCount;
  }

  public BigLakeFailureCategory category() {
    return category;
  }

  public String errorType() {
    return errorType;
  }

  public int responseCode() {
    return responseCode;
  }

  public int remoteStatus() {
    return remoteStatus;
  }

  public @Nullable String googleRequestId() {
    return googleRequestId;
  }

  public @Nullable String retryAfter() {
    return retryAfter;
  }

  public int retryCount() {
    return retryCount;
  }
}
