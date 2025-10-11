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
package org.apache.polaris.extension.auth.opa;

import static com.google.common.base.Preconditions.checkArgument;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Optional;

/**
 * Configuration for OPA (Open Policy Agent) authorization.
 *
 * <p><strong>Beta Feature:</strong> OPA authorization is currently in Beta and is not a stable
 * release. It may undergo breaking changes in future versions. Use with caution in production
 * environments.
 */
@ConfigMapping(prefix = "polaris.authorization.opa")
public interface OpaAuthorizationConfig {
  Optional<String> url();

  Optional<String> policyPath();

  Optional<AuthenticationConfig> auth();

  Optional<HttpConfig> http();

  /** Validates the complete OPA configuration */
  default void validate() {
    checkArgument(url().isPresent() && !url().get().isBlank(), "OPA URL cannot be null or empty");
    checkArgument(
        policyPath().isPresent() && !policyPath().get().isBlank(),
        "OPA policy path cannot be null or empty");
    checkArgument(auth().isPresent(), "Authentication configuration is required");

    auth().get().validate();
  }

  /** HTTP client configuration for OPA communication. */
  interface HttpConfig {
    @WithDefault("2000")
    int timeoutMs();

    @WithDefault("true")
    boolean verifySsl();

    Optional<String> trustStorePath();

    Optional<String> trustStorePassword();
  }

  /** Authentication configuration for OPA communication. */
  interface AuthenticationConfig {
    /** Type of authentication */
    @WithDefault("none")
    String type();

    /** Bearer token authentication configuration */
    Optional<BearerTokenConfig> bearer();

    default void validate() {
      switch (type()) {
        case "bearer":
          checkArgument(
              bearer().isPresent(), "Bearer configuration is required when type is 'bearer'");
          bearer().get().validate();
          break;
        case "none":
          // No authentication - nothing to validate
          break;
        default:
          throw new IllegalArgumentException(
              "Invalid authentication type: " + type() + ". Supported types: 'bearer', 'none'");
      }
    }
  }

  interface BearerTokenConfig {
    /** Type of bearer token configuration */
    @WithDefault("static-token")
    String type();

    /** Static bearer token configuration */
    Optional<StaticTokenConfig> staticToken();

    /** File-based bearer token configuration */
    Optional<FileBasedConfig> fileBased();

    default void validate() {
      switch (type()) {
        case "static-token":
          checkArgument(
              staticToken().isPresent(),
              "Static token configuration is required when type is 'static-token'");
          staticToken().get().validate();
          break;
        case "file-based":
          checkArgument(
              fileBased().isPresent(),
              "File-based configuration is required when type is 'file-based'");
          fileBased().get().validate();
          break;
        default:
          throw new IllegalArgumentException(
              "Invalid bearer token type: " + type() + ". Must be 'static-token' or 'file-based'");
      }
    }

    /** Configuration for static bearer tokens */
    interface StaticTokenConfig {
      /** Static bearer token value */
      Optional<String> value();

      default void validate() {
        checkArgument(
            value().isPresent() && !value().get().isBlank(),
            "Static bearer token value cannot be null or empty");
      }
    }

    /** Configuration for file-based bearer tokens */
    interface FileBasedConfig {
      /** Path to file containing bearer token */
      Optional<String> path();

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

      default void validate() {
        checkArgument(
            path().isPresent() && !path().get().isBlank(),
            "Bearer token file path cannot be null or empty");
        checkArgument(refreshInterval() > 0, "refreshInterval must be greater than 0");
        checkArgument(jwtExpirationBuffer() > 0, "jwtExpirationBuffer must be greater than 0");
      }
    }
  }
}
