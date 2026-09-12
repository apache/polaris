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
 * Thrown when a persistence write never started because the backing store could not be reached (for
 * example, a JDBC connection could not be acquired before any statement was sent). This is the
 * definite-non-write counterpart to {@link PersistenceCommitStateUnknownException}: the write
 * certainly did not happen, so callers may safely clean up anything they wrote in anticipation of
 * the commit (e.g. a newly written table metadata file) and the request is safe to retry.
 *
 * <p>Note: the simple class name is surfaced to clients as the {@code type} field of the REST error
 * payload (see {@code PolarisExceptionMapper}). Renaming this class changes that wire value.
 */
public class PersistenceWriteNotStartedException extends PolarisException {

  public PersistenceWriteNotStartedException(String message, Throwable cause) {
    super(message, cause);
  }

  public PersistenceWriteNotStartedException(String message) {
    super(message);
  }

  @Override
  public int httpStatusCode() {
    // The write definitely did not happen and the store was unreachable, so this is a transient,
    // safely retryable failure rather than an unknown outcome.
    return Response.Status.SERVICE_UNAVAILABLE.getStatusCode();
  }
}
