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

import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Context information for credential vending operations. This context is used to provide metadata
 * that can be attached to credentials as session tags (e.g., AWS STS session tags) for audit and
 * correlation purposes in CloudTrail and similar logging systems.
 *
 * <p>When session tags are enabled, this context provides:
 *
 * <ul>
 *   <li>{@code catalogName} - The name of the catalog vending credentials
 *   <li>{@code namespace} - The namespace/database being accessed (e.g., "db.schema")
 *   <li>{@code tableName} - The name of the table being accessed
 *   <li>{@code requestId} - A unique request identifier for correlation with catalog audit logs
 * </ul>
 *
 * <p>These values appear in cloud provider audit logs (e.g., AWS CloudTrail), enabling
 * deterministic correlation between catalog operations and data access events.
 */
@PolarisImmutable
public interface CredentialVendingContext {

  // Default session tag keys for cloud provider credentials (e.g., AWS STS session tags).
  // These appear in cloud audit logs (e.g., CloudTrail) for correlation purposes.
  String TAG_KEY_CATALOG = "polaris:catalog";
  String TAG_KEY_NAMESPACE = "polaris:namespace";
  String TAG_KEY_TABLE = "polaris:table";
  String TAG_KEY_PRINCIPAL = "polaris:principal";
  String TAG_KEY_REQUEST_ID = "polaris:request-id";

  /** The name of the catalog that is vending credentials. */
  Optional<String> catalogName();

  /**
   * The namespace being accessed, represented as a dot-separated string (e.g., "database.schema").
   */
  Optional<String> namespace();

  /** The name of the table being accessed. */
  Optional<String> tableName();

  /**
   * A unique request identifier that can be used to correlate this credential vending operation
   * with catalog audit logs.
   */
  Optional<String> requestId();

  /**
   * Creates a new builder for CredentialVendingContext.
   *
   * @return a new builder instance
   */
  static Builder builder() {
    return ImmutableCredentialVendingContext.builder();
  }

  /**
   * Creates an empty context with no metadata. This is useful when session tags are disabled or
   * when context information is not available.
   *
   * @return an empty context instance
   */
  static CredentialVendingContext empty() {
    return ImmutableCredentialVendingContext.builder().build();
  }

  interface Builder {
    Builder catalogName(Optional<String> catalogName);

    Builder namespace(Optional<String> namespace);

    Builder tableName(Optional<String> tableName);

    Builder requestId(Optional<String> requestId);

    CredentialVendingContext build();
  }
}
