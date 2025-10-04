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
package org.apache.polaris.service.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Optional;

@ConfigMapping(prefix = "polaris.authorization")
public interface AuthorizationConfiguration {
  @WithDefault("default")
  String type();

  OpaConfig opa();

  interface OpaConfig {
    Optional<String> url();

    Optional<String> policyPath();

    Optional<Integer> timeoutMs();

    BearerTokenConfig bearerToken();

    @WithDefault("true")
    boolean verifySsl();

    Optional<String> trustStorePath();

    Optional<String> trustStorePassword();
  }

  interface BearerTokenConfig {
    /** Static bearer token value (takes precedence over file-based token) */
    Optional<String> staticValue();

    /** Path to file containing bearer token (used if staticValue is not set) */
    Optional<String> filePath();

    /** How often to refresh file-based bearer tokens (in seconds) */
    @WithDefault("300")
    int refreshInterval();

    /**
     * Whether to automatically detect JWT tokens and use their 'exp' field for refresh timing. If
     * true and the token is a valid JWT with an 'exp' claim, the token will be refreshed based on
     * the expiration time minus the buffer, rather than the fixed refresh interval.
     */
    @WithDefault("true")
    boolean jwtExpirationRefresh();

    /**
     * Buffer time in seconds before JWT expiration to refresh the token. Only used when
     * jwtExpirationRefresh is true and the token is a valid JWT. Default is 60 seconds.
     */
    @WithDefault("60")
    int jwtExpirationBuffer();
  }
}
