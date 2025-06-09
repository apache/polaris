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
package org.apache.polaris.core.secrets;

import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for UserSecretReference URNs. Provides the ability to validate, parse, and build
 * URNs.
 */
public final class UserSecretReferenceUrnHelper {

  private UserSecretReferenceUrnHelper() {}

  private static final String URN_SCHEME = "urn";
  private static final String URN_NAMESPACE = "polaris-secret";
  private static final String SECRET_MANAGER_TYPE_REGEX = "([a-zA-Z0-9_-]+)";
  private static final String TYPE_SPECIFIC_IDENTIFIER_REGEX =
      "([a-zA-Z0-9_-]+(?::[a-zA-Z0-9_-]+)*)";

  /**
   * Precompiled regex pattern for validating the secret manager type and type-specific identifier.
   */
  private static final Pattern SECRET_MANAGER_TYPE_PATTERN =
      Pattern.compile("^" + SECRET_MANAGER_TYPE_REGEX + "$");

  private static final Pattern TYPE_SPECIFIC_IDENTIFIER_PATTERN =
      Pattern.compile("^" + TYPE_SPECIFIC_IDENTIFIER_REGEX + "$");

  /**
   * Precompiled regex pattern for validating and parsing UserSecretReference URNs. Expected format:
   * urn:polaris-secret:<secret-manager-type>:<identifier1>(:<identifier2>:...).
   *
   * <p>Groups:
   *
   * <p>Group 1: secret-manager-type (alphanumeric, hyphens, underscores).
   *
   * <p>Group 2: type-specific-identifier (one or more colon-separated alphanumeric components).
   */
  private static final Pattern URN_PATTERN =
      Pattern.compile(
          "^"
              + URN_SCHEME
              + ":"
              + URN_NAMESPACE
              + ":"
              + SECRET_MANAGER_TYPE_REGEX
              + ":"
              + TYPE_SPECIFIC_IDENTIFIER_REGEX
              + "$");

  /**
   * Validates whether the given URN string matches the expected format for UserSecretReference
   * URNs.
   *
   * @param urn The URN string to validate.
   * @return true if the URN is valid, false otherwise.
   */
  public static boolean isValid(@Nonnull String urn) {
    return urn.trim().isEmpty() ? false : URN_PATTERN.matcher(urn).matches();
  }

  public static String getUrnPattern() {
    return URN_PATTERN.toString();
  }

  /**
   * Extracts the secret manager type from a valid URN.
   *
   * @param urn The URN string to parse.
   * @return The secret manager type.
   */
  @Nonnull
  public static String getSecretManagerType(@Nonnull String urn) {
    Matcher matcher = URN_PATTERN.matcher(urn);
    Preconditions.checkState(matcher.matches(), "Invalid secret URN: " + urn);
    return matcher.group(1);
  }

  /**
   * Extracts the type-specific identifier from a valid URN.
   *
   * @param urn The URN string to parse.
   * @return The type-specific identifier.
   */
  @Nonnull
  public static String getTypeSpecificIdentifier(@Nonnull String urn) {
    Matcher matcher = URN_PATTERN.matcher(urn);
    Preconditions.checkState(matcher.matches(), "Invalid secret URN: " + urn);
    return matcher.group(2);
  }

  /**
   * Builds a URN string from the given secret manager type and type-specific identifier. Validates
   * the inputs to ensure they conform to the expected pattern.
   *
   * @param secretManagerType The secret manager type (alphanumeric, hyphens, underscores).
   * @param typeSpecificIdentifier The type-specific identifier (colon-separated alphanumeric
   *     components).
   * @return The constructed URN string.
   */
  @Nonnull
  public static String buildUrn(
      @Nonnull String secretManagerType, @Nonnull String typeSpecificIdentifier) {

    Preconditions.checkArgument(
        !secretManagerType.trim().isEmpty(), "Secret manager type cannot be empty");
    Preconditions.checkArgument(
        SECRET_MANAGER_TYPE_PATTERN.matcher(secretManagerType).matches(),
        "Invalid secret manager type '%s'; must contain only alphanumeric characters, hyphens, and underscores",
        secretManagerType);

    Preconditions.checkArgument(
        !typeSpecificIdentifier.trim().isEmpty(), "Type-specific identifier cannot be empty");
    Preconditions.checkArgument(
        TYPE_SPECIFIC_IDENTIFIER_PATTERN.matcher(typeSpecificIdentifier).matches(),
        "Invalid type-specific identifier '%s'; must be colon-separated alphanumeric components (hyphens and underscores allowed)",
        typeSpecificIdentifier);

    return URN_SCHEME
        + ":"
        + URN_NAMESPACE
        + ":"
        + secretManagerType
        + ":"
        + typeSpecificIdentifier;
  }
}
