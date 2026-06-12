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

import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;

/**
 * Validates and normalizes user-supplied {@code polaris.storage.name} property values used to
 * select a server-configured named storage profile for credential vending.
 *
 * <p>Blank input (null, empty, or whitespace-only) normalizes to {@code null}, which callers
 * interpret as "clear the override".
 */
public final class StorageNameValidator {

  public static final int MAX_LENGTH = 128;

  private static final Pattern VALID = Pattern.compile("^[a-zA-Z0-9_-]+$");

  private StorageNameValidator() {}

  /**
   * Trim, validate, and normalize a candidate storage name.
   *
   * @param value raw user input
   * @return the trimmed name if valid, or {@code null} if the input is blank
   * @throws IllegalArgumentException if the trimmed value exceeds {@link #MAX_LENGTH} or contains
   *     characters outside {@code [a-zA-Z0-9_-]}
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
          "polaris.storage.name length must be at most " + MAX_LENGTH + " characters");
    }
    if (!VALID.matcher(trimmed).matches()) {
      throw new IllegalArgumentException("polaris.storage.name must match pattern [a-zA-Z0-9_-]+");
    }
    return trimmed;
  }
}
