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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
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
   * Builds a list of AWS STS session tags from the principal name and credential vending context,
   * including only the fields specified in {@code enabledFields}.
   *
   * <p>Only fields present in {@code enabledFields} will be included in the returned list. The
   * trace ID tag is only included if {@link SessionTagField#TRACE_ID} is in {@code enabledFields}
   * and {@link CredentialVendingContext#traceId()} is present (the context's traceId is populated
   * by the caller based on configuration).
   *
   * @param principalName the name of the principal requesting credentials
   * @param context the credential vending context
   * @param enabledFields the set of {@link SessionTagField}s to include
   * @return a list of STS Tags to attach to the AssumeRole request
   */
  public static List<Tag> buildSessionTags(
      String principalName, CredentialVendingContext context, Set<SessionTagField> enabledFields) {
    return enabledFields.stream()
        .map(field -> field.buildTag(principalName, context))
        .flatMap(java.util.Optional::stream)
        .collect(Collectors.toList());
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
