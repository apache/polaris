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

import java.util.Optional;

public class TokenResponse {
  private final Optional<OAuthTokenErrorResponse.Error> error;
  private String accessToken;
  private String tokenType;
  private Integer expiresIn;

  public TokenResponse(OAuthTokenErrorResponse.Error error) {
    this.error = Optional.of(error);
  }

  public TokenResponse(String accessToken, String tokenType, int expiresIn) {
    this.accessToken = accessToken;
    this.expiresIn = expiresIn;
    this.tokenType = tokenType;
    this.error = Optional.empty();
  }

  public boolean isValid() {
    return error.isEmpty();
  }

  public OAuthTokenErrorResponse.Error getError() {
    return error.get();
  }

  public String getAccessToken() {
    return accessToken;
  }

  public int getExpiresIn() {
    return expiresIn;
  }

  public String getTokenType() {
    return tokenType;
  }
}
