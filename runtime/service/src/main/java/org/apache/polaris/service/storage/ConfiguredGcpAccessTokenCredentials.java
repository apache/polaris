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
import java.time.Clock;
import java.time.Duration;
import java.util.Date;

/**
 * {@link GoogleCredentials} backed by a statically configured access token (the {@code
 * polaris.storage.gcp.token} option).
 *
 * <p>Unlike {@link GoogleCredentials#create(AccessToken)}, which produces a credential that cannot
 * refresh, this type re-stamps the expiry of the configured token on every {@link
 * #refreshAccessToken() refresh}. The token value itself does not change at runtime. Without this,
 * the credential is memoized once with a single expiry stamped at first use, so a long-running
 * server stops being able to access GCS once the configured lifespan elapses and only recovers on
 * restart (see issue #2253).
 *
 * <p>This only helps configured tokens that do not actually expire on the provider side, such as
 * emulator or test credentials or otherwise long-lived tokens. It does not renew a genuinely
 * short-lived token: re-stamping the expiry just stops the local refresh from failing, and the next
 * GCS call would then fail at the provider instead. Rotating a real configured token still requires
 * a restart.
 */
final class ConfiguredGcpAccessTokenCredentials extends GoogleCredentials {

  private static final long serialVersionUID = 1L;

  private final String token;
  private final Duration lifespan;
  private final Clock clock;

  ConfiguredGcpAccessTokenCredentials(String token, Duration lifespan, Clock clock) {
    super(GoogleCredentials.newBuilder().setAccessToken(newAccessToken(token, lifespan, clock)));
    this.token = token;
    this.lifespan = lifespan;
    this.clock = clock;
  }

  @Override
  public AccessToken refreshAccessToken() {
    return newAccessToken(token, lifespan, clock);
  }

  private static AccessToken newAccessToken(String token, Duration lifespan, Clock clock) {
    return new AccessToken(token, new Date(clock.instant().plus(lifespan).toEpochMilli()));
  }
}
