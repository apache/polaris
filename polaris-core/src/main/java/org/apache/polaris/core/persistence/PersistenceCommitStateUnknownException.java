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
package org.apache.polaris.core.persistence;

import jakarta.ws.rs.core.Response;
import org.apache.polaris.core.exceptions.PolarisException;

/**
 * Thrown when a persistence write may have committed but the client cannot confirm the outcome (for
 * example a connection drop or timeout after the database applied an auto-commit update). Named to
 * distinguish it from Iceberg's table-commit {@code CommitStateUnknownException}.
 *
 * <p>Callers must not treat this as a definite failure: newly written metadata files must not be
 * deleted, and clients should not blindly retry the same commit.
 *
 * <p>Note: the simple class name is surfaced to clients as the {@code type} field of the REST error
 * payload (see {@code PolarisExceptionMapper}). Renaming this class changes that wire value, which
 * clients may use to recognize an unknown commit outcome.
 */
public class PersistenceCommitStateUnknownException extends PolarisException {

  public PersistenceCommitStateUnknownException(String message, Throwable cause) {
    super(message, cause);
  }

  public PersistenceCommitStateUnknownException(String message) {
    super(message);
  }

  @Override
  public int httpStatusCode() {
    // Iceberg REST: unknown commit outcome must be 5xx so clients do not treat it as cleanable.
    return Response.Status.INTERNAL_SERVER_ERROR.getStatusCode();
  }
}
