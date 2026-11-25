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
package org.apache.polaris.service.storage;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import java.time.Instant;
import java.util.Date;

/**
 * {@link GoogleCredentials} backed by a statically configured access token (the {@code
 * polaris.storage.gcp.token} option).
 *
 * <p>{@link GoogleCredentials#create(AccessToken)} produces a credential that cannot refresh, so
 * credential vending fails immediately with an {@link IllegalStateException}: {@code
 * GcpCredentialsStorageIntegration} calls {@code refresh()} on the source credential
 * unconditionally before downscoping, so the first vend throws regardless of the token's expiry
 * (see issue #2253). This type supports {@code refresh()} by returning the configured token with a
 * fixed expiry.
 *
 * <p>The expiry is computed once, when the credential is created, and does not move on later
 * refreshes: the token value never changes, so neither does its reported expiry. This only helps
 * configured tokens that do not actually expire on the provider side, such as emulator or test
 * credentials or otherwise long-lived tokens. It does not renew a genuinely short-lived token; once
 * the provider rejects it the GCS call fails at the provider instead, and rotating a real
 * configured token still requires a restart.
 */
final class FixedGcpAccessTokenCredentials extends GoogleCredentials {

  private static final long serialVersionUID = 1L;

  private final String token;
  private final Instant expiry;

  FixedGcpAccessTokenCredentials(String token, Instant expiry) {
    super(GoogleCredentials.newBuilder().setAccessToken(new AccessToken(token, Date.from(expiry))));
    this.token = token;
    this.expiry = expiry;
  }

  @Override
  public AccessToken refreshAccessToken() {
    return new AccessToken(token, Date.from(expiry));
  }
}
