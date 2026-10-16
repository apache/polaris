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
import java.util.function.Supplier;

/**
 * {@link GoogleCredentials} backed by a statically configured access token (the {@code
 * polaris.storage.gcp.token} option).
 *
 * <p>Unlike {@link GoogleCredentials#create(AccessToken)}, which produces a credential that cannot
 * refresh, this type re-reads the configured token and re-stamps its expiry on every {@link
 * #refreshAccessToken() refresh}. Without it the credential is memoized once with a single expiry
 * stamped at first use, so a long-running server stops being able to access GCS once the configured
 * lifespan elapses and only recovers on restart (see issue #2253). This mirrors the self-refreshing
 * behavior of the application-default credentials path.
 */
final class ConfiguredGcpAccessTokenCredentials extends GoogleCredentials {

  private static final long serialVersionUID = 1L;

  private final transient Supplier<String> tokenSupplier;
  private final transient Duration lifespan;
  private final transient Clock clock;

  ConfiguredGcpAccessTokenCredentials(
      Supplier<String> tokenSupplier, Duration lifespan, Clock clock) {
    super(
        GoogleCredentials.newBuilder()
            .setAccessToken(newAccessToken(tokenSupplier, lifespan, clock)));
    this.tokenSupplier = tokenSupplier;
    this.lifespan = lifespan;
    this.clock = clock;
  }

  @Override
  public AccessToken refreshAccessToken() {
    return newAccessToken(tokenSupplier, lifespan, clock);
  }

  private static AccessToken newAccessToken(
      Supplier<String> tokenSupplier, Duration lifespan, Clock clock) {
    return new AccessToken(
        tokenSupplier.get(), new Date(clock.instant().plus(lifespan).toEpochMilli()));
  }
}
