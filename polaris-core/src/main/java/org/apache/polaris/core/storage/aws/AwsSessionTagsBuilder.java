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
import java.util.regex.Pattern;
import org.apache.polaris.core.storage.CredentialVendingContext;
import software.amazon.awssdk.services.sts.model.Tag;

/**
 * Utility class for building AWS STS session tags from credential vending context. These tags
 * appear in CloudTrail events for correlation between catalog operations and S3 data access.
 */
public final class AwsSessionTagsBuilder {

  // AWS limit for session tag values
  static final int MAX_TAG_VALUE_LENGTH = 256;

  /** Placeholder value used when a tag value is null or empty. */
  static final String TAG_VALUE_UNKNOWN = "unknown";

  /**
   * Pattern matching characters that are NOT allowed in AWS STS session tag values. AWS STS allows:
   * Unicode letters (\p{L}), whitespace (\p{Z}), numbers (\p{N}), and specific special characters
   * _.:/=+\-@
   *
   * <p>This pattern matches any character that is NOT in the allowed set, so we can replace them.
   */
  private static final Pattern INVALID_TAG_VALUE_CHARS =
      Pattern.compile("[^\\p{L}\\p{Z}\\p{N}_.:/=+\\-@]");

  private AwsSessionTagsBuilder() {
    // Utility class - prevent instantiation
  }

  /**
   * Builds a list of AWS STS session tags from the principal name and credential vending context.
   * These tags will appear in CloudTrail events for correlation purposes.
   *
   * <p>The trace ID tag is only included if {@link CredentialVendingContext#traceId()} is present.
   * This is controlled at the source (StorageAccessConfigProvider) based on the
   * INCLUDE_TRACE_ID_IN_SESSION_TAGS feature flag.
   *
   * <p>The request ID tag is only included if {@link CredentialVendingContext#requestId()} is
   * present. This is controlled at the source (StorageAccessConfigProvider) based on the
   * INCLUDE_REQUEST_ID_IN_SESSION_TAGS feature flag.
   *
   * @param principalName the name of the principal requesting credentials
   * @param context the credential vending context containing catalog, namespace, table, roles, and
   *     optionally trace ID and request ID
   * @return a list of STS Tags to attach to the AssumeRole request
   */
  public static List<Tag> buildSessionTags(String principalName, CredentialVendingContext context) {
    List<Tag> tags = new ArrayList<>();

    // Always include all tags with "unknown" placeholder for missing values
    // This ensures consistent tag presence in CloudTrail for correlation
    tags.add(
        Tag.builder()
            .key(CredentialVendingContext.TAG_KEY_PRINCIPAL)
            .value(truncateTagValue(principalName))
            .build());
    tags.add(
        Tag.builder()
            .key(CredentialVendingContext.TAG_KEY_ROLES)
            .value(truncateTagValue(context.activatedRoles().orElse(TAG_VALUE_UNKNOWN)))
            .build());
    tags.add(
        Tag.builder()
            .key(CredentialVendingContext.TAG_KEY_CATALOG)
            .value(truncateTagValue(context.catalogName().orElse(TAG_VALUE_UNKNOWN)))
            .build());
    tags.add(
        Tag.builder()
            .key(CredentialVendingContext.TAG_KEY_NAMESPACE)
            .value(truncateTagValue(context.namespace().orElse(TAG_VALUE_UNKNOWN)))
            .build());
    tags.add(
        Tag.builder()
            .key(CredentialVendingContext.TAG_KEY_TABLE)
            .value(truncateTagValue(context.tableName().orElse(TAG_VALUE_UNKNOWN)))
            .build());

    // Only include trace ID if it's present in the context.
    // The context's traceId is only populated when INCLUDE_TRACE_ID_IN_SESSION_TAGS is enabled.
    // This allows efficient credential caching when trace IDs are not needed in session tags.
    context
        .traceId()
        .ifPresent(
            traceId ->
                tags.add(
                    Tag.builder()
                        .key(CredentialVendingContext.TAG_KEY_TRACE_ID)
                        .value(truncateTagValue(traceId))
                        .build()));

    // Only include request ID if it's present in the context.
    // The context's requestId is only populated when INCLUDE_REQUEST_ID_IN_SESSION_TAGS is enabled.
    // This allows efficient credential caching when request IDs are not needed in session tags.
    context
        .requestId()
        .ifPresent(
            requestId ->
                tags.add(
                    Tag.builder()
                        .key(CredentialVendingContext.TAG_KEY_REQUEST_ID)
                        .value(truncateTagValue(requestId))
                        .build()));

    return tags;
  }

  /**
   * Prepares a tag value for AWS STS by sanitizing invalid characters and truncating to fit within
   * AWS limits. AWS limits session tag values to 256 characters and restricts the allowed character
   * set to Unicode letters, whitespace, numbers, and specific special characters (_.:/=+\-@).
   *
   * <p>Invalid characters are replaced with underscores to maintain value readability while
   * ensuring STS acceptance. Returns "unknown" placeholder for null or empty values to ensure
   * consistent tag presence in CloudTrail.
   *
   * @param value the value to sanitize and potentially truncate
   * @return the sanitized and truncated value, or "unknown" if value is null or empty
   */
  static String truncateTagValue(String value) {
    if (value == null || value.isEmpty()) {
      return TAG_VALUE_UNKNOWN;
    }
    // Sanitize invalid characters first, then truncate
    String sanitized = sanitizeTagValue(value);
    if (sanitized.length() <= MAX_TAG_VALUE_LENGTH) {
      return sanitized;
    }
    return sanitized.substring(0, MAX_TAG_VALUE_LENGTH);
  }

  /**
   * Sanitizes a tag value by replacing characters that are not allowed in AWS STS session tag
   * values. AWS STS allows: Unicode letters, whitespace, numbers, and specific special characters
   * (_.:/=+\-@). Invalid characters are replaced with underscores.
   *
   * @param value the value to sanitize (must not be null)
   * @return the sanitized value with invalid characters replaced by underscores
   */
  static String sanitizeTagValue(String value) {
    return INVALID_TAG_VALUE_CHARS.matcher(value).replaceAll("_");
  }
}
