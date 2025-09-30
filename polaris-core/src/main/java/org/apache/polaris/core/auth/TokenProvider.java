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
package org.apache.polaris.core.auth;

import jakarta.annotation.Nullable;

/**
 * Interface for providing bearer tokens for authentication.
 *
 * <p>Implementations can provide tokens from various sources such as:
 *
 * <ul>
 *   <li>Static string values
 *   <li>Files (with automatic reloading)
 *   <li>External token services
 * </ul>
 */
public interface TokenProvider {

  /**
   * Get the current bearer token.
   *
   * @return the bearer token, or null if no token is available
   */
  @Nullable
  String getToken();

  /**
   * Clean up any resources used by this token provider. Should be called when the provider is no
   * longer needed.
   */
  default void close() {
    // Default implementation does nothing
  }
}
