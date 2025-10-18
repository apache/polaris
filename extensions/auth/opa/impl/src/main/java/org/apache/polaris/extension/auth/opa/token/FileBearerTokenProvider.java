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

import com.auth0.jwt.JWT;
import com.auth0.jwt.exceptions.JWTDecodeException;
import com.auth0.jwt.interfaces.DecodedJWT;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
  private final Clock clock;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private volatile String cachedToken;
  private volatile Instant lastRefresh;
  private volatile Instant nextRefresh;
  private volatile boolean closed = false;

  /**
   * Create a new file-based token provider with JWT expiration support.
   *
   * @param tokenFilePath path to the file containing the bearer token
   * @param refreshInterval how often to check for token file changes (fallback for non-JWT tokens)
   * @param jwtExpirationRefresh whether to use JWT expiration for refresh timing
   * @param jwtExpirationBuffer buffer time before JWT expiration to refresh the token
   */
  public FileBearerTokenProvider(
      Path tokenFilePath,
      Duration refreshInterval,
      boolean jwtExpirationRefresh,
      Duration jwtExpirationBuffer) {
    this(
        tokenFilePath,
        refreshInterval,
        jwtExpirationRefresh,
        jwtExpirationBuffer,
        Clock.systemUTC());
  }

  /**
   * Create a new file-based token provider with JWT expiration support and custom clock.
   * Package-private constructor for testing purposes.
   *
   * @param tokenFilePath path to the file containing the bearer token
   * @param refreshInterval how often to check for token file changes (fallback for non-JWT tokens)
   * @param jwtExpirationRefresh whether to use JWT expiration for refresh timing
   * @param jwtExpirationBuffer buffer time before JWT expiration to refresh the token
   * @param clock clock instance for time operations (useful for testing)
   */
  FileBearerTokenProvider(
      Path tokenFilePath,
      Duration refreshInterval,
      boolean jwtExpirationRefresh,
      Duration jwtExpirationBuffer,
      Clock clock) {
    this.tokenFilePath = tokenFilePath;
    this.refreshInterval = refreshInterval;
    this.jwtExpirationRefresh = jwtExpirationRefresh;
    this.jwtExpirationBuffer = jwtExpirationBuffer;
    this.clock = clock;
    this.lastRefresh = Instant.MIN; // Force initial load
    this.nextRefresh = Instant.MIN; // Force initial refresh

    logger.debug(
        "Created file token provider for path: {} with refresh interval: {}, JWT expiration refresh: {}, JWT buffer: {}",
        tokenFilePath,
        refreshInterval,
        jwtExpirationRefresh,
        jwtExpirationBuffer);
  }

  @Override
  @Nullable
  public String getToken() {
    if (closed) {
      logger.warn("Token provider is closed, returning null");
      return null;
    }

    // Check if we need to refresh
    if (shouldRefresh()) {
      refreshToken();
    }

    lock.readLock().lock();
    try {
      return cachedToken;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void close() {
    closed = true;
    lock.writeLock().lock();
    try {
      cachedToken = null;
      logger.info("File token provider closed");
    } finally {
      lock.writeLock().unlock();
    }
  }

  private boolean shouldRefresh() {
    return clock.instant().isAfter(nextRefresh);
  }

  private void refreshToken() {
    lock.writeLock().lock();
    try {
      // Double-check pattern - another thread might have refreshed while we waited for the lock
      if (!shouldRefresh()) {
        return;
      }

      String newToken = loadTokenFromFile();

      // If we couldn't load a token and have no cached token, this is a fatal error
      if (newToken == null && cachedToken == null) {
        throw new RuntimeException(
            "Unable to load bearer token from file: "
                + tokenFilePath
                + ". This is required for OPA authorization.");
      }

      // Only update cached token if we successfully loaded a new one
      if (newToken != null) {
        cachedToken = newToken;
      }
      // If newToken is null but cachedToken exists, we keep using the cached token

      lastRefresh = clock.instant();

      // Calculate next refresh time based on current token (may be cached)
      nextRefresh = calculateNextRefresh(cachedToken);

      logger.debug(
          "Token refreshed from file: {} (token present: {}), next refresh: {}",
          tokenFilePath,
          cachedToken != null && !cachedToken.isEmpty(),
          nextRefresh);

    } finally {
      lock.writeLock().unlock();
    }
  }

  /** Calculate when the next refresh should occur based on JWT expiration or fixed interval. */
  private Instant calculateNextRefresh(@Nullable String token) {
    if (token == null || !jwtExpirationRefresh) {
      // Use fixed interval
      return lastRefresh.plus(refreshInterval);
    }

    // Attempt to parse as JWT and extract expiration
    Optional<Instant> expiration = getJwtExpirationTime(token);

    if (expiration.isPresent()) {
      // Refresh before expiration minus buffer
      Instant refreshTime = expiration.get().minus(jwtExpirationBuffer);

      // Ensure refresh time is in the future and not too soon (at least 1 second)
      Instant minRefreshTime = clock.instant().plus(Duration.ofSeconds(1));
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
    int attempts = 0;
    long deadlineMs =
        clock.millis()
            + 3000; // 3 second deadline for retries (reasonable for CSI driver file mounts)
    String currentCachedToken = cachedToken; // Snapshot of current cache

    while (true) {
      try {
        // Check if file is readable first
        if (!Files.isReadable(tokenFilePath)) {
          // Treat as transient error and retry (could be CSI driver not ready yet)
          throw new IOException("File is not readable: " + tokenFilePath);
        }

        String token = Files.readString(tokenFilePath, StandardCharsets.UTF_8).trim();
        if (!token.isEmpty()) {
          return token;
        }

        // Empty token - treat as transient issue (could be file being written)
        throw new IOException("File contains only whitespace: " + tokenFilePath);
      } catch (IOException e) {
        // Treat file system exceptions as transient (file being rotated, temporary unavailability)
        logger.debug(
            "Transient error reading token file (attempt {}): {}", attempts + 1, e.getMessage());
      }

      // If we have a cached token and it's safe to use, fall back to it
      if (currentCachedToken != null && !currentCachedToken.isEmpty()) {
        logger.warn(
            "Failed to read new token from file after {} attempts, using cached token",
            attempts + 1);
        return currentCachedToken;
      }

      // No cached token available and we can't read from file
      attempts++;
      if (attempts >= 5 || clock.millis() > deadlineMs) {
        // Return null to let refreshToken() decide how to handle this based on cached token state
        logger.debug("Token unavailable after {} attempts", attempts);
        return null;
      }

      // Exponential backoff: 100ms, 200ms, 400ms, 800ms, 1000ms (reasonable for CSI driver
      // scenarios)
      long delayMs = Math.min(100L << (attempts - 1), 1000);
      try {
        Thread.sleep(delayMs);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        logger.debug("Interrupted while reading token, returning null");
        return null;
      }
    }
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
