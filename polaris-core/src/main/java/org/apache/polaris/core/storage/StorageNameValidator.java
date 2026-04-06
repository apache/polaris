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
package org.apache.polaris.core.storage;

import jakarta.annotation.Nullable;
import java.util.regex.Pattern;

/**
 * Validates storage name references used in {@code polaris.storage.name} property overrides.
 *
 * <p>Storage names are symbolic references to server-side credential configurations (e.g. {@code
 * polaris.storage.aws.<storageName>.access-key}). They must be valid Quarkus config key segments.
 */
public final class StorageNameValidator {

  /** Maximum length for a storage name. */
  public static final int MAX_LENGTH = 128;

  /** Alphanumeric characters, hyphens, and underscores only. */
  private static final Pattern VALID_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]+$");

  private StorageNameValidator() {}

  /**
   * Normalizes blank values to {@code null} and validates non-blank names.
   *
   * @param value the input value
   * @return {@code null} if the input is null, empty, or blank; otherwise the trimmed and validated
   *     value
   * @throws IllegalArgumentException if the normalized non-blank value is invalid
   */
  public static @Nullable String normalizeAndValidate(@Nullable String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    if (trimmed.length() > MAX_LENGTH) {
      throw new IllegalArgumentException(
          String.format(
              "Storage name must not exceed %d characters, got %d", MAX_LENGTH, trimmed.length()));
    }
    if (!VALID_PATTERN.matcher(trimmed).matches()) {
      throw new IllegalArgumentException(
          String.format(
              "Storage name '%s' contains invalid characters. "
                  + "Only alphanumeric characters, hyphens, and underscores are allowed.",
              trimmed));
    }
    return trimmed;
  }
}
