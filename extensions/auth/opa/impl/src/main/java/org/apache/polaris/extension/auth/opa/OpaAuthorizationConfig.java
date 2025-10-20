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

import com.google.common.base.Strings;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
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

  /** Authentication types supported by OPA authorization */
  enum AuthenticationType {
    NONE("none"),
    BEARER("bearer");

    private final String value;

    AuthenticationType(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  /** Bearer token configuration types */
  enum BearerTokenType {
    STATIC_TOKEN("static-token"),
    FILE_BASED("file-based");

    private final String value;

    BearerTokenType(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  Optional<URI> policyUri();

  Optional<AuthenticationConfig> auth();

  Optional<HttpConfig> http();

  /** Validates the complete OPA configuration */
  default void validate() {
    checkArgument(policyUri().isPresent(), "OPA policy URI cannot be null");
    checkArgument(auth().isPresent(), "Authentication configuration is required");

    auth().get().validate();
  }

  /** HTTP client configuration for OPA communication. */
  interface HttpConfig {
    @WithDefault("2000")
    int timeoutMs();

    @WithDefault("true")
    boolean verifySsl();

    Optional<Path> trustStorePath();

    Optional<String> trustStorePassword();
  }

  /** Authentication configuration for OPA communication. */
  interface AuthenticationConfig {
    /** Type of authentication */
    @WithDefault("none")
    AuthenticationType type();

    /** Bearer token authentication configuration */
    Optional<BearerTokenConfig> bearer();

    default void validate() {
      switch (type()) {
        case BEARER:
          checkArgument(
              bearer().isPresent(), "Bearer configuration is required when type is 'bearer'");
          bearer().get().validate();
          break;
        case NONE:
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
    BearerTokenType type();

    /** Static bearer token configuration */
    Optional<StaticTokenConfig> staticToken();

    /** File-based bearer token configuration */
    Optional<FileBasedConfig> fileBased();

    default void validate() {
      switch (type()) {
        case STATIC_TOKEN:
          checkArgument(
              staticToken().isPresent(),
              "Static token configuration is required when type is 'static-token'");
          staticToken().get().validate();
          break;
        case FILE_BASED:
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
      String value();

      default void validate() {
        checkArgument(
            !Strings.isNullOrEmpty(value()), "Static bearer token value cannot be null or empty");
      }
    }

    /** Configuration for file-based bearer tokens */
    interface FileBasedConfig {
      /** Path to file containing bearer token */
      Path path();

      /** How often to refresh file-based bearer tokens (defaults to 5 minutes if not specified) */
      Optional<Duration> refreshInterval();

      /**
       * Whether to automatically detect JWT tokens and use their 'exp' field for refresh timing. If
       * true and the token is a valid JWT with an 'exp' claim, the token will be refreshed based on
       * the expiration time minus the buffer, rather than the fixed refresh interval. Defaults to
       * true if not specified.
       */
      Optional<Boolean> jwtExpirationRefresh();

      /**
       * Buffer time before JWT expiration to refresh the token. Only used when jwtExpirationRefresh
       * is true and the token is a valid JWT. Defaults to 1 minute if not specified.
       */
      Optional<Duration> jwtExpirationBuffer();

      default void validate() {
        checkArgument(path() != null, "Bearer token file path cannot be null");
        checkArgument(
            refreshInterval().isEmpty() || refreshInterval().get().isPositive(),
            "refreshInterval must be positive");
        checkArgument(
            jwtExpirationBuffer().isEmpty() || jwtExpirationBuffer().get().isPositive(),
            "jwtExpirationBuffer must be positive");
      }
    }
  }
}
