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
package org.apache.polaris.persistence.atomic.dynamodb;

import java.util.Optional;

/**
 * Configuration for the DynamoDB persistence backend, sourced from {@code
 * polaris.persistence.dynamodb.*} (mirroring {@code polaris.persistence.relational.jdbc.*}).
 *
 * <p>The DynamoDB client itself (region, credentials, connection) is configured separately via the
 * AWS SDK; this interface carries only Polaris-level settings for the backend.
 */
public interface DynamoDbConfiguration {

  /** Name of the DynamoDB table that stores Polaris entities and grant records. */
  Optional<String> tableName();

  /**
   * Optional endpoint override for the DynamoDB client, e.g. {@code http://localhost:8000} to
   * target DynamoDB Local in tests or local development. When unset, the AWS SDK resolves the
   * endpoint from the configured region.
   */
  Optional<String> endpointOverride();
}
