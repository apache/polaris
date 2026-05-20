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

import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.core.storage.CredentialVendingContext;

/**
 * Utility class for building a structured AWS STS role session name from configured fields.
 *
 * <p>Session names are constructed as {@code <prefix><field1>-<field2>-...} and truncated to the
 * AWS 64-character limit. The prefix defaults to {@code p-} and is configurable via a {@code
 * prefix-X} token in the field list (see {@link #extractPrefix}).
 *
 * <p>Characters are allocated greedily: each field receives an equal share of the remaining budget.
 * When a field's value is shorter than its share, the unused characters flow to subsequent fields.
 * Each field value is individually sanitized via {@link AwsRoleSessionNameSanitizer} before
 * allocation is applied.
 */
public final class AwsSessionNameBuilder {

  static final String DEFAULT_SESSION_NAME_PREFIX = "p-";

  /** Prefix for config tokens that set the session name prefix, e.g. {@code prefix-myorg}. */
  static final String PREFIX_CONFIG_TOKEN = "prefix-";

  static final String DEFAULT_SESSION_NAME = "PolarisAwsCredentialsStorageIntegration";

  private AwsSessionNameBuilder() {}

  /**
   * Extracts the session name prefix from the raw config token list.
   *
   * <p>A token of the form {@code prefix-X} sets the prefix to {@code X-}. If no such token is
   * present, the default {@code p-} is returned. If multiple tokens match, the first one wins.
   *
   * @param rawConfigTokens the raw list from {@code SESSION_NAME_FIELDS_IN_SUBSCOPED_CREDENTIAL}
   * @return the sanitized prefix string (including trailing {@code -})
   */
  static String extractPrefix(List<String> rawConfigTokens) {
    for (String token : rawConfigTokens) {
      if (token.startsWith(PREFIX_CONFIG_TOKEN)) {
        String key = token.substring(PREFIX_CONFIG_TOKEN.length());
        String sanitized = AwsRoleSessionNameSanitizer.sanitizeOnly(key);
        return sanitized.isEmpty() ? DEFAULT_SESSION_NAME_PREFIX : sanitized + "-";
      }
    }
    return DEFAULT_SESSION_NAME_PREFIX;
  }

  /**
   * Builds a structured role session name using the default prefix {@code p-}.
   *
   * @see #buildSessionName(String, CredentialVendingContext, List, String)
   */
  static String buildSessionName(
      String principalName, CredentialVendingContext context, List<SessionNameField> fields) {
    return buildSessionName(principalName, context, fields, DEFAULT_SESSION_NAME_PREFIX);
  }

  /**
   * Builds a structured role session name from the ordered list of {@link SessionNameField}s.
   *
   * <p>The result is {@code prefix + field1 + "-" + field2 + ...}, guaranteed to be at most {@link
   * AwsRoleSessionNameSanitizer#MAX_ROLE_SESSION_NAME_LENGTH} characters.
   *
   * <p>Character budget (after the prefix):
   *
   * <ol>
   *   <li>Reserve {@code n-1} characters for separators between {@code n} fields.
   *   <li>For each field in order, allocate an equal share of the remaining budget (floor
   *       division). If the field's value is shorter than its share, the unused characters are
   *       carried forward to subsequent fields.
   * </ol>
   *
   * <p>If the field list is empty, the method returns the fallback value {@code
   * PolarisAwsCredentialsStorageIntegration} (the legacy default).
   *
   * @param principalName the name of the principal requesting credentials
   * @param context the credential vending context
   * @param fields the ordered list of fields to include, as resolved from configuration
   * @param prefix the session name prefix (including any trailing separator)
   * @return a valid AWS STS role session name of at most 64 characters
   */
  static String buildSessionName(
      String principalName,
      CredentialVendingContext context,
      List<SessionNameField> fields,
      String prefix) {
    if (fields.isEmpty()) {
      return DEFAULT_SESSION_NAME;
    }

    int n = fields.size();
    int maxLength = AwsRoleSessionNameSanitizer.MAX_ROLE_SESSION_NAME_LENGTH;
    int budgetForValues = maxLength - prefix.length() - (n - 1);

    if (budgetForValues <= 0) {
      return AwsRoleSessionNameSanitizer.truncate(prefix);
    }

    List<String> parts = new ArrayList<>(n);
    int remaining = budgetForValues;
    for (int i = 0; i < n; i++) {
      String sanitized =
          AwsRoleSessionNameSanitizer.sanitizeOnly(fields.get(i).getValue(principalName, context));
      int alloc = remaining / (n - i);
      int used = Math.min(sanitized.length(), alloc);
      parts.add(sanitized.substring(0, used));
      remaining -= used;
    }

    return prefix + String.join("-", parts);
  }
}
