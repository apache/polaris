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
package org.apache.polaris.service.auth.internal.broker;

import jakarta.annotation.Nullable;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.service.auth.internal.service.OAuthError;
import org.immutables.value.Value;

@PolarisImmutable
public interface TokenResponse {

  static TokenResponse of(String accessToken, String tokenType, int expiresIn) {
    return ImmutableTokenResponse.builder()
        .accessToken(accessToken)
        .tokenType(tokenType)
        .expiresIn(expiresIn)
        .build();
  }

  static TokenResponse of(OAuthError error) {
    return ImmutableTokenResponse.builder().error(error).build();
  }

  @Nullable
  OAuthError getError();

  @Nullable
  String getAccessToken();

  @Value.Default
  default int getExpiresIn() {
    return 0;
  }

  @Nullable
  String getTokenType();
}
