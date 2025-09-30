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
package org.apache.polaris.core.auth;

import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A token provider that reads tokens from a file and automatically reloads them based on a
 * configurable refresh interval.
 *
 * <p>This is particularly useful in Kubernetes environments where tokens are mounted as files and
 * refreshed by external systems (e.g., service account tokens, projected volumes, etc.).
 *
 * <p>The token file is expected to contain the bearer token as plain text. Leading and trailing
 * whitespace will be trimmed.
 */
public class FileTokenProvider implements TokenProvider {

  private static final Logger logger = LoggerFactory.getLogger(FileTokenProvider.class);

  private final Path tokenFilePath;
  private final Duration refreshInterval;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private volatile String cachedToken;
  private volatile Instant lastRefresh;
  private volatile boolean closed = false;

  /**
   * Create a new file-based token provider.
   *
   * @param tokenFilePath path to the file containing the bearer token
   * @param refreshInterval how often to check for token file changes
   */
  public FileTokenProvider(String tokenFilePath, Duration refreshInterval) {
    this.tokenFilePath = Paths.get(tokenFilePath);
    this.refreshInterval = refreshInterval;
    this.lastRefresh = Instant.MIN; // Force initial load

    logger.info(
        "Created file token provider for path: {} with refresh interval: {}",
        tokenFilePath,
        refreshInterval);
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
    return lastRefresh.plus(refreshInterval).isBefore(Instant.now());
  }

  private void refreshToken() {
    lock.writeLock().lock();
    try {
      // Double-check pattern - another thread might have refreshed while we waited for the lock
      if (!shouldRefresh()) {
        return;
      }

      String newToken = loadTokenFromFile();
      cachedToken = newToken;
      lastRefresh = Instant.now();

      if (logger.isDebugEnabled()) {
        logger.debug(
            "Token refreshed from file: {} (token present: {})",
            tokenFilePath,
            newToken != null && !newToken.isEmpty());
      }

    } finally {
      lock.writeLock().unlock();
    }
  }

  @Nullable
  private String loadTokenFromFile() {
    try {
      if (!Files.exists(tokenFilePath)) {
        logger.warn("Token file does not exist: {}", tokenFilePath);
        return null;
      }

      if (!Files.isReadable(tokenFilePath)) {
        logger.warn("Token file is not readable: {}", tokenFilePath);
        return null;
      }

      String content = Files.readString(tokenFilePath, StandardCharsets.UTF_8);
      String token = content.trim();

      if (token.isEmpty()) {
        logger.warn("Token file is empty: {}", tokenFilePath);
        return null;
      }

      return token;

    } catch (IOException e) {
      logger.error("Failed to read token from file: {}", tokenFilePath, e);
      return null;
    }
  }
}
