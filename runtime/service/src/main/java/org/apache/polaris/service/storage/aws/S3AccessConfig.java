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

package org.apache.polaris.service.storage.aws;

import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Configuration interface containing parameters for clients accessing S3 services from Polaris
 * servers.
 *
 * <p>Currently, this configuration does not apply to all of Polaris code, but only to select
 * services.
 */
public interface S3AccessConfig {
  /** Default value for {@link #clientsCacheMaxSize()}. */
  int DEFAULT_MAX_STS_CLIENT_CACHE_ENTRIES = 50;

  /** Maximum number of entries to keep in the STS clients cache. */
  OptionalInt clientsCacheMaxSize();

  default int effectiveClientsCacheMaxSize() {
    return clientsCacheMaxSize().orElse(DEFAULT_MAX_STS_CLIENT_CACHE_ENTRIES);
  }

  /** Override the default maximum number of pooled connections. */
  OptionalInt maxHttpConnections();

  /** Override the default connection read timeout. */
  Optional<Duration> readTimeout();

  /** Override the default TCP connect timeout. */
  Optional<Duration> connectTimeout();

  /**
   * Override default connection acquisition timeout. This is the time a request will wait for a
   * connection from the pool.
   */
  Optional<Duration> connectionAcquisitionTimeout();

  /** Override default max idle time of a pooled connection. */
  Optional<Duration> connectionMaxIdleTime();

  /** Override default time-time of a pooled connection. */
  Optional<Duration> connectionTimeToLive();

  /** Override default behavior whether to expect an HTTP/100-Continue. */
  Optional<Boolean> expectContinueEnabled();
}
