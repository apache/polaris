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

import java.util.Optional;
import org.apache.polaris.core.storage.CredentialVendingContext;
import software.amazon.awssdk.services.sts.model.Tag;

/**
 * Enum representing the supported fields that can be included as AWS STS session tags in credential
 * vending. Each value knows its tag key and how to extract its value from a {@link
 * CredentialVendingContext}.
 *
 * <p>These tags appear in CloudTrail events for correlation between catalog operations and S3 data
 * access. Configure via {@code SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL} using the {@link #configName}
 * of each desired field.
 */
public enum SessionTagField {
  REALM("realm", CredentialVendingContext.TAG_KEY_REALM) {
    @Override
    public Optional<Tag> buildTag(String principalName, CredentialVendingContext context) {
      return Optional.of(tag(context.realm().orElse(AwsSessionTagsBuilder.TAG_VALUE_UNKNOWN)));
    }
  },
  CATALOG("catalog", CredentialVendingContext.TAG_KEY_CATALOG) {
    @Override
    public Optional<Tag> buildTag(String principalName, CredentialVendingContext context) {
      return Optional.of(
          tag(context.catalogName().orElse(AwsSessionTagsBuilder.TAG_VALUE_UNKNOWN)));
    }
  },
  NAMESPACE("namespace", CredentialVendingContext.TAG_KEY_NAMESPACE) {
    @Override
    public Optional<Tag> buildTag(String principalName, CredentialVendingContext context) {
      return Optional.of(tag(context.namespace().orElse(AwsSessionTagsBuilder.TAG_VALUE_UNKNOWN)));
    }
  },
  TABLE("table", CredentialVendingContext.TAG_KEY_TABLE) {
    @Override
    public Optional<Tag> buildTag(String principalName, CredentialVendingContext context) {
      return Optional.of(tag(context.tableName().orElse(AwsSessionTagsBuilder.TAG_VALUE_UNKNOWN)));
    }
  },
  PRINCIPAL("principal", CredentialVendingContext.TAG_KEY_PRINCIPAL) {
    @Override
    public Optional<Tag> buildTag(String principalName, CredentialVendingContext context) {
      return Optional.of(tag(principalName));
    }
  },
  ROLES("roles", CredentialVendingContext.TAG_KEY_ROLES) {
    @Override
    public Optional<Tag> buildTag(String principalName, CredentialVendingContext context) {
      return Optional.of(
          tag(context.activatedRoles().orElse(AwsSessionTagsBuilder.TAG_VALUE_UNKNOWN)));
    }
  },
  /**
   * The OpenTelemetry trace ID. Only emits a tag when {@link CredentialVendingContext#traceId()} is
   * present (the caller populates it only when this field is configured).
   *
   * <p>WARNING: including this field disables credential caching because every request has a unique
   * trace ID.
   */
  TRACE_ID("trace_id", CredentialVendingContext.TAG_KEY_TRACE_ID) {
    @Override
    public Optional<Tag> buildTag(String principalName, CredentialVendingContext context) {
      return context.traceId().map(this::tag);
    }
  };

  /** The string name used in the {@code SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL} config list. */
  public final String configName;

  private final String tagKey;

  SessionTagField(String configName, String tagKey) {
    this.configName = configName;
    this.tagKey = tagKey;
  }

  /**
   * Builds an STS {@link Tag} for this field from the given context, or {@link Optional#empty()} if
   * the field has no value to emit (e.g. {@code trace_id} when no trace is active).
   */
  public abstract Optional<Tag> buildTag(String principalName, CredentialVendingContext context);

  Tag tag(String value) {
    return Tag.builder().key(tagKey).value(AwsSessionTagsBuilder.truncateTagValue(value)).build();
  }

  /** Returns the {@link SessionTagField} for the given config name, or empty if not found. */
  public static Optional<SessionTagField> fromConfigName(String configName) {
    for (SessionTagField field : values()) {
      if (field.configName.equals(configName)) {
        return Optional.of(field);
      }
    }
    return Optional.empty();
  }
}
