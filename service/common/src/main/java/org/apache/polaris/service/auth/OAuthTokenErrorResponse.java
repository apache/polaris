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
package org.apache.polaris.service.auth;

import com.fasterxml.jackson.annotation.JsonProperty;

/** An OAuth Error Token Response as defined by the Iceberg REST API OpenAPI Spec. */
public class OAuthTokenErrorResponse {

  public enum Error {
    invalid_request("The request is invalid"),
    invalid_client("The Client is invalid"),
    invalid_grant("The grant is invalid"),
    unauthorized_client("The client is not authorized"),
    unsupported_grant_type("The grant type is invalid"),
    invalid_scope("The scope is invalid"),
    ;

    final String errorDescription;

    Error(String errorDescription) {
      this.errorDescription = errorDescription;
    }

    public String getErrorDescription() {
      return errorDescription;
    }
  }

  private final String error;
  private final String errorDescription;
  private final String errorUri;

  /** Initlaizes a response from one of the supported errors */
  public OAuthTokenErrorResponse(Error error) {
    this.error = error.name();
    this.errorDescription = error.getErrorDescription();
    this.errorUri = null; // Not yet used
  }

  @JsonProperty("error")
  public String getError() {
    return error;
  }

  @JsonProperty("error_description")
  public String getErrorDescription() {
    return errorDescription;
  }

  @JsonProperty("error_uri")
  public String getErrorUri() {
    return errorUri;
  }
}
