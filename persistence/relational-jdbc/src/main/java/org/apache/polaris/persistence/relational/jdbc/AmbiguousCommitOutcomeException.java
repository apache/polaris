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
package org.apache.polaris.persistence.relational.jdbc;

import java.sql.SQLException;

/**
 * Marks a failure thrown from {@code Connection.commit()} whose outcome cannot be confirmed
 * (connection loss, timeout, cancellation). The commit may already have been applied. The original
 * message, SQLSTATE and vendor code are preserved.
 *
 * <p>{@code withRetries} wraps the terminal failure in a plain {@link SQLException}, so callers
 * detect this marker on the cause chain rather than as the top-level type.
 */
final class AmbiguousCommitOutcomeException extends SQLException {
  AmbiguousCommitOutcomeException(SQLException cause) {
    super(cause.getMessage(), cause.getSQLState(), cause.getErrorCode(), cause);
  }
}
