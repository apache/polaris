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

package org.apache.polaris.service.auth.internal.service;

public enum OAuthError {
  invalid_request("The request is invalid"),
  invalid_client("The Client is invalid"),
  invalid_grant("The grant is invalid"),
  unauthorized_client("The client is not authorized"),
  unsupported_grant_type("The grant type is invalid"),
  invalid_scope("The scope is invalid"),
  ;

  final String errorDescription;

  OAuthError(String errorDescription) {
    this.errorDescription = errorDescription;
  }

  public String getErrorDescription() {
    return errorDescription;
  }
}
