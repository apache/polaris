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
package org.apache.polaris.core.storage.aws;

import jakarta.annotation.Nonnull;
import java.util.regex.Pattern;

/**
 * Utility class for sanitizing AWS STS role session names.
 *
 * <p>AWS STS role session names must conform to the pattern {@code [\w+=,.@-]*} and have a maximum
 * length of 64 characters. This class provides methods to sanitize arbitrary strings (such as
 * principal names) into valid role session names.
 *
 * @see <a href="https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html">AWS STS
 *     AssumeRole API</a>
 */
public final class AwsRoleSessionNameSanitizer {

  /**
   * AWS STS role session name maximum length. While the AssumedRoleId can be up to 193 characters,
   * the roleSessionName parameter itself is limited to 64 characters.
   */
  static final int MAX_ROLE_SESSION_NAME_LENGTH = 64;

  /**
   * Pattern matching characters that are NOT allowed in AWS STS role session names. AWS allows:
   * alphanumeric characters (a-z, A-Z, 0-9), underscore (_), plus (+), equals (=), comma (,),
   * period (.), at sign (@), and hyphen (-).
   *
   * <p>This pattern matches any character outside this allowed set.
   */
  private static final Pattern INVALID_ROLE_SESSION_NAME_CHARS =
      Pattern.compile("[^a-zA-Z0-9_+=,.@-]");

  /** Default replacement character for invalid characters. */
  private static final String DEFAULT_REPLACEMENT = "_";

  private AwsRoleSessionNameSanitizer() {
    // Utility class to prevent instantiation
  }

  /**
   * Sanitizes a string for use as an AWS STS role session name.
   *
   * <p>This method:
   *
   * <ol>
   *   <li>Replaces any characters not matching {@code [\w+=,.@-]} with underscores
   *   <li>Truncates the result to 64 characters (AWS maximum)
   * </ol>
   *
   * <p>The underscore replacement character was chosen because:
   *
   * <ul>
   *   <li>It is always valid in role session names
   *   <li>It is visually distinct and indicates a substitution occurred
   *   <li>It does not introduce ambiguity (unlike hyphen which is common in names)
   * </ul>
   *
   * @param input the string to sanitize (typically a principal name)
   * @return a sanitized string safe for use as an AWS STS role session name
   */
  public static @Nonnull String sanitize(@Nonnull String input) {
    String sanitized =
        INVALID_ROLE_SESSION_NAME_CHARS.matcher(input).replaceAll(DEFAULT_REPLACEMENT);
    return truncate(sanitized);
  }

  /**
   * Truncates a string to the maximum allowed role session name length.
   *
   * @param input the string to truncate
   * @return the truncated string, or the original if already within limits
   */
  static @Nonnull String truncate(@Nonnull String input) {
    if (input.length() <= MAX_ROLE_SESSION_NAME_LENGTH) {
      return input;
    }
    return input.substring(0, MAX_ROLE_SESSION_NAME_LENGTH);
  }
}
