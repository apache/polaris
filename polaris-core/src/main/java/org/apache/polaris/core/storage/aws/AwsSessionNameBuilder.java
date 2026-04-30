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
 * <p>Session names are constructed as {@code p-<field1>-<field2>-...} and truncated to the AWS
 * 64-character limit. Characters are allocated proportionally across fields; any remainder goes to
 * the last field.
 *
 * <p>Each field value is individually sanitized via {@link AwsRoleSessionNameSanitizer} before
 * proportional truncation is applied.
 */
public final class AwsSessionNameBuilder {

  static final String SESSION_NAME_PREFIX = "p-";

  static final String DEFAULT_SESSION_NAME = "PolarisAwsCredentialsStorageIntegration";

  private AwsSessionNameBuilder() {}

  /**
   * Builds a structured role session name from the ordered list of {@link SessionNameField}s.
   *
   * <p>The result is prefixed with {@code p-}, fields are joined with {@code -}, and the whole
   * string is guaranteed to be at most {@link
   * AwsRoleSessionNameSanitizer#MAX_ROLE_SESSION_NAME_LENGTH} characters.
   *
   * <p>Character budget (after the {@code p-} prefix):
   *
   * <ol>
   *   <li>Reserve {@code n-1} characters for separators between {@code n} fields.
   *   <li>Distribute the remaining characters proportionally across fields (floor division).
   *   <li>Any remainder (modulo) is awarded to the last field.
   * </ol>
   *
   * <p>If the field list is empty, the method returns the fallback value {@code
   * PolarisAwsCredentialsStorageIntegration} (the legacy default).
   *
   * @param principalName the name of the principal requesting credentials
   * @param context the credential vending context
   * @param fields the ordered list of fields to include, as resolved from configuration
   * @return a valid AWS STS role session name of at most 64 characters
   */
  public static String buildSessionName(
      String principalName, CredentialVendingContext context, List<SessionNameField> fields) {
    if (fields.isEmpty()) {
      return DEFAULT_SESSION_NAME;
    }

    int n = fields.size();
    int maxLength = AwsRoleSessionNameSanitizer.MAX_ROLE_SESSION_NAME_LENGTH;
    int budgetForValues = maxLength - SESSION_NAME_PREFIX.length() - (n - 1);

    if (budgetForValues <= 0) {
      return AwsRoleSessionNameSanitizer.truncate(SESSION_NAME_PREFIX);
    }

    int basePerField = budgetForValues / n;
    int remainder = budgetForValues % n;

    List<String> parts = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      String sanitized =
          AwsRoleSessionNameSanitizer.sanitizeOnly(fields.get(i).getValue(principalName, context));
      int limit = (i == n - 1) ? basePerField + remainder : basePerField;
      parts.add(sanitized.length() > limit ? sanitized.substring(0, limit) : sanitized);
    }

    return SESSION_NAME_PREFIX + String.join("-", parts);
  }
}
