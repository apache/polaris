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
package org.apache.polaris.extension.auth.opa.token;

import static com.google.common.base.Preconditions.checkState;

import com.auth0.jwt.JWT;
import com.auth0.jwt.exceptions.JWTDecodeException;
import com.auth0.jwt.interfaces.DecodedJWT;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.polaris.nosql.async.AsyncExec;
import org.apache.polaris.nosql.async.Cancelable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A token provider that reads tokens from a file and automatically reloads them based on a
 * configurable refresh interval or JWT expiration timing.
 *
 * <p>This is particularly useful in Kubernetes environments where tokens are mounted as files and
 * refreshed by external systems (e.g., service account tokens, projected volumes, etc.).
 *
 * <p>The token file is expected to contain the bearer token as plain text. Leading and trailing
 * whitespace will be trimmed.
 *
 * <p>If JWT expiration refresh is enabled and the token is a valid JWT with an 'exp' claim, the
 * provider will automatically refresh the token based on the expiration time minus a configurable
 * buffer, rather than using the fixed refresh interval.
 */
public class FileBearerTokenProvider implements BearerTokenProvider {

  private static final Logger logger = LoggerFactory.getLogger(FileBearerTokenProvider.class);

  private final Path tokenFilePath;
  private final Duration refreshInterval;
  private final boolean jwtExpirationRefresh;
  private final Duration jwtExpirationBuffer;
  private final Supplier<Instant> clock;
  private final AtomicBoolean refreshLock = new AtomicBoolean();
  private final AsyncExec asyncExec;
  private final CompletableFuture<String> initialTokenFuture = new CompletableFuture<>();
  private final long initialTokenWaitMillis;

  private volatile String cachedToken;
  private volatile Instant lastRefresh;
  private volatile Instant nextRefresh;
  private volatile Cancelable<?> refreshTask;

  /**
   * Create a new file-based token provider with JWT expiration support.
   *
   * @param tokenFilePath path to the file containing the bearer token
   * @param refreshInterval how often to check for token file changes (fallback for non-JWT tokens)
   * @param jwtExpirationRefresh whether to use JWT expiration for refresh timing
   * @param jwtExpirationBuffer buffer time before JWT expiration to refresh the token
   * @param clock clock instance for time operations
   * @throws IllegalStateException if the initial token cannot be loaded from the file
   */
  public FileBearerTokenProvider(
      Path tokenFilePath,
      Duration refreshInterval,
      boolean jwtExpirationRefresh,
      Duration jwtExpirationBuffer,
      Duration initialTokenWait,
      AsyncExec asyncExec,
      Supplier<Instant> clock) {
    this.tokenFilePath = tokenFilePath;
    this.refreshInterval = refreshInterval;
    this.jwtExpirationRefresh = jwtExpirationRefresh;
    this.jwtExpirationBuffer = jwtExpirationBuffer;
    this.initialTokenWaitMillis = initialTokenWait.toMillis();
    this.clock = clock;
    this.asyncExec = asyncExec;

    this.nextRefresh = Instant.MIN;
    this.lastRefresh = Instant.MIN;
    // start refreshing the token (immediately)
    scheduleRefreshAttempt(Duration.ZERO);

    checkState(Files.isReadable(tokenFilePath), "OPA token file does not exist or is not readable");

    logger.debug(
        "Created file token provider for path: {} with refresh interval: {}, JWT expiration refresh: {}, JWT buffer: {}, next refresh: {}",
        tokenFilePath,
        refreshInterval,
        jwtExpirationRefresh,
        jwtExpirationBuffer,
        nextRefresh);
  }

  @Override
  public String getToken() {
    String token = cachedToken;
    if (token != null) {
      // Regular case, we have a cached token
      return token;
    }
    // We get here if the cached token is null, which means that the initial token
    // has not been loaded yet.
    // In this case we wait for the configured amount of time
    // (5 seconds in production, much lower in tests).
    try {
      return initialTokenFuture.get(initialTokenWaitMillis, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to read initial OPA bearer token", e);
    }
  }

  @Override
  public void close() {
    cachedToken = null;
    Cancelable<?> task = refreshTask;
    if (task != null) {
      refreshTask.cancel();
    }
  }

  private void refreshTokenAttempt() {
    boolean isInitialRefresh = cachedToken == null;
    Duration delay;
    if (doRefreshToken()) {
      delay = Duration.between(clock.get(), nextRefresh);
      if (isInitialRefresh) {
        // If we have never cached a token, complete the initial token-future to "unblock"
        // getToken() call sites waiting for it.
        initialTokenFuture.complete(cachedToken);
      }
    } else {
      // Token refresh did not succeed, retry soon
      delay = Duration.ofSeconds(1); // TODO: make configurable
    }
    scheduleRefreshAttempt(delay);
  }

  private void scheduleRefreshAttempt(Duration delay) {
    this.refreshTask = asyncExec.schedule(this::refreshTokenAttempt, delay);
  }

  private boolean doRefreshToken() {
    String newToken = loadTokenFromFile();
    // Only update cached token if we successfully loaded a new one
    if (newToken == null) {
      logger.debug("Couldn't load new bearer token from {}, will retry.", tokenFilePath);
      return false;
    }
    cachedToken = newToken;

    lastRefresh = clock.get();

    // Calculate next refresh time based on current token (may be cached)
    nextRefresh = calculateNextRefresh(cachedToken);

    logger.debug(
        "Token refreshed from file: {} (token present: {}), next refresh: {}",
        tokenFilePath,
        cachedToken != null,
        nextRefresh);

    return true;
  }

  /** Calculate when the next refresh should occur based on JWT expiration or fixed interval. */
  private Instant calculateNextRefresh(String token) {
    if (!jwtExpirationRefresh) {
      return lastRefresh.plus(refreshInterval);
    }

    // Attempt to parse as JWT and extract expiration
    Optional<Instant> expiration = getJwtExpirationTime(token);

    if (expiration.isPresent()) {
      // Refresh before expiration minus buffer
      Instant refreshTime = expiration.get().minus(jwtExpirationBuffer);

      // Ensure refresh time is in the future and not too soon (at least 1 second)
      Instant minRefreshTime = clock.get().plus(Duration.ofSeconds(1));
      if (refreshTime.isBefore(minRefreshTime)) {
        logger.warn(
            "JWT expires too soon ({}), using minimum refresh interval instead", expiration.get());
        return lastRefresh.plus(refreshInterval);
      }

      logger.debug(
          "Using JWT expiration-based refresh: token expires at {}, refreshing at {}",
          expiration.get(),
          refreshTime);
      return refreshTime;
    }

    // Fall back to fixed interval (token is not a valid JWT or has no expiration)
    logger.debug("Token is not a valid JWT or has no expiration, using fixed refresh interval");
    return lastRefresh.plus(refreshInterval);
  }

  @Nullable
  private String loadTokenFromFile() {
    try {
      String token = Files.readString(tokenFilePath, StandardCharsets.UTF_8).trim();
      if (!token.isEmpty()) {
        return token;
      }
    } catch (IOException e) {
      logger.debug("Failed to read token from file", e);
    }
    return null;
  }

  /**
   * Extract the expiration time from a JWT token without signature verification.
   *
   * @param token the JWT token string
   * @return the expiration time as an Instant, or empty if not present or invalid
   */
  private Optional<Instant> getJwtExpirationTime(String token) {
    try {
      DecodedJWT decodedJWT = JWT.decode(token);
      Date expiresAt = decodedJWT.getExpiresAt();
      return expiresAt != null ? Optional.of(expiresAt.toInstant()) : Optional.empty();
    } catch (JWTDecodeException e) {
      logger.debug("Failed to decode JWT token: {}", e.getMessage());
      return Optional.empty();
    }
  }
}
