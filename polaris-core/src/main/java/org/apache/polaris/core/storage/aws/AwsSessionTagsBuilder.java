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

  private AwsSessionTagsBuilder() {
    // Utility class - prevent instantiation
  }

  /**
   * Builds a list of AWS STS session tags from the principal name and credential vending context.
   * These tags will appear in CloudTrail events for correlation purposes.
   *
   * @param principalName the name of the principal requesting credentials
   * @param context the credential vending context containing catalog, namespace, table, and roles
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
    tags.add(
        Tag.builder()
            .key(CredentialVendingContext.TAG_KEY_TRACE_ID)
            .value(truncateTagValue(context.traceId().orElse(TAG_VALUE_UNKNOWN)))
            .build());

    return tags;
  }

  /**
   * Truncates a tag value to fit within AWS STS limits. AWS limits session tag values to 256
   * characters. Returns "unknown" placeholder for null or empty values to ensure consistent tag
   * presence in CloudTrail.
   *
   * @param value the value to potentially truncate
   * @return the truncated value, or "unknown" if value is null or empty
   */
  static String truncateTagValue(String value) {
    if (value == null || value.isEmpty()) {
      return TAG_VALUE_UNKNOWN;
    }
    if (value.length() <= MAX_TAG_VALUE_LENGTH) {
      return value;
    }
    return value.substring(0, MAX_TAG_VALUE_LENGTH);
  }
}
