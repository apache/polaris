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

import jakarta.annotation.Nullable;

/**
 * Supplier interface for obtaining the current request ID.
 *
 * <p>This interface allows the JDBC persistence layer to obtain the server-generated request ID
 * without directly depending on runtime/service layer classes.
 */
public interface RequestIdSupplier {

  /**
   * Gets the server-generated request ID for the current request.
   *
   * @return the request ID, or null if not available
   */
  @Nullable
  String getRequestId();

  /** No-op implementation that returns null. */
  RequestIdSupplier NOOP = () -> null;
}
