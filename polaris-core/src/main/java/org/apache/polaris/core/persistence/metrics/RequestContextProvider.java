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
package org.apache.polaris.core.persistence.metrics;

import jakarta.annotation.Nullable;

/**
 * Provider interface for obtaining request context fields.
 *
 * <p>This interface allows the JDBC persistence layer to obtain request-scoped context information
 * (principal name, request ID, OpenTelemetry trace/span IDs) without directly depending on
 * runtime/service layer classes.
 *
 * <p>Implementations are provided by the service layer and injected via CDI.
 */
public interface RequestContextProvider {

  /**
   * Gets the principal name from the security context.
   *
   * @return the principal name, or null if not available
   */
  @Nullable
  String getPrincipalName();

  /**
   * Gets the server-generated request ID.
   *
   * @return the request ID, or null if not available
   */
  @Nullable
  String getRequestId();

  /**
   * Gets the OpenTelemetry trace ID.
   *
   * @return the trace ID, or null if not available
   */
  @Nullable
  String getOtelTraceId();

  /**
   * Gets the OpenTelemetry span ID.
   *
   * @return the span ID, or null if not available
   */
  @Nullable
  String getOtelSpanId();

  /** No-op implementation that returns null for all context fields. */
  RequestContextProvider NOOP =
      new RequestContextProvider() {
        @Override
        public String getPrincipalName() {
          return null;
        }

        @Override
        public String getRequestId() {
          return null;
        }

        @Override
        public String getOtelTraceId() {
          return null;
        }

        @Override
        public String getOtelSpanId() {
          return null;
        }
      };
}
