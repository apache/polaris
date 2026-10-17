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

import org.apache.polaris.core.exceptions.PolarisServiceUnavailableException;

/**
 * Thrown when a persistence write never started because the backing store could not be reached (for
 * example, a JDBC connection could not be acquired before any statement was sent). This is the
 * definite-non-write counterpart to {@link PersistenceCommitStateUnknownException}: the write
 * certainly did not happen, so callers may safely clean up anything they wrote in anticipation of
 * the commit (e.g. a newly written table metadata file) and the request is safe to retry.
 *
 * <p>Extends {@link PolarisServiceUnavailableException} so it maps to the same transient, retryable
 * HTTP 503 (with a {@code Retry-After} header) and is picked up by anything that special-cases that
 * type.
 *
 * <p>Note: the simple class name is surfaced to clients as the {@code type} field of the REST error
 * payload (see {@code PolarisExceptionMapper}). Renaming this class changes that wire value.
 */
public class PersistenceWriteNotStartedException extends PolarisServiceUnavailableException {

  // The write never reached the store, so the client can retry without delay.
  private static final int RETRY_AFTER_SECONDS = 0;

  public PersistenceWriteNotStartedException(String message, Throwable cause) {
    // "%s" keeps the pre-formatted message literal; it may contain '%' from a driver error.
    super(RETRY_AFTER_SECONDS, "%s", message);
    initCause(cause);
  }

  public PersistenceWriteNotStartedException(String message) {
    super(RETRY_AFTER_SECONDS, "%s", message);
  }
}
