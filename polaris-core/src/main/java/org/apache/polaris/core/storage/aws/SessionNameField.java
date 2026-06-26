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

/**
 * Enum representing the supported fields that can be included in the AWS STS role session name
 * during credential vending. Each value knows how to extract its raw string value from a {@link
 * CredentialVendingContext} and principal name.
 *
 * <p>Configure via {@code SESSION_NAME_FIELDS_IN_SUBSCOPED_CREDENTIAL} using the {@link
 * #configName} of each desired field. Fields are joined with {@code -} and prefixed with {@code
 * p-}, then sanitized and truncated to the 64-character AWS STS limit.
 *
 * <p>Supported fields: realm, catalog, namespace, table, principal. Unlike session tags, roles and
 * trace_id are excluded because they are high-cardinality or unsuitable for the session name
 * format.
 */
enum SessionNameField {
  REALM("realm") {
    @Override
    public String getValue(String principalName, CredentialVendingContext context) {
      return context.realm().orElse("");
    }
  },
  CATALOG("catalog") {
    @Override
    public String getValue(String principalName, CredentialVendingContext context) {
      return context.catalogName().orElse("");
    }
  },
  NAMESPACE("namespace") {
    @Override
    public String getValue(String principalName, CredentialVendingContext context) {
      return context.namespace().orElse("");
    }
  },
  TABLE("table") {
    @Override
    public String getValue(String principalName, CredentialVendingContext context) {
      return context.tableName().orElse("");
    }
  },
  /**
   * The name of the principal requesting credentials. Note that only the principal name is
   * included, not its active roles or attributes; two requests from the same principal with
   * different roles will produce the same session name. To include role information in the
   * credential identity, use {@code SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL} with the {@code roles}
   * field instead.
   */
  PRINCIPAL("principal") {
    @Override
    public String getValue(String principalName, CredentialVendingContext context) {
      return principalName;
    }
  };

  /**
   * The string name used in the {@code SESSION_NAME_FIELDS_IN_SUBSCOPED_CREDENTIAL} config list.
   */
  public final String configName;

  SessionNameField(String configName) {
    this.configName = configName;
  }

  /**
   * Extracts the raw (unsanitized, un-truncated) string value for this field from the given
   * context.
   *
   * @param principalName the name of the principal requesting credentials
   * @param context the credential vending context
   * @return the raw field value, or an empty string if unavailable
   */
  public abstract String getValue(String principalName, CredentialVendingContext context);

  /** Returns the {@link SessionNameField} for the given config name, or empty if not found. */
  public static Optional<SessionNameField> fromConfigName(String configName) {
    for (SessionNameField field : values()) {
      if (field.configName.equals(configName)) {
        return Optional.of(field);
      }
    }
    return Optional.empty();
  }
}
