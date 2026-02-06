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
package org.apache.polaris.service.catalog.iceberg;

/**
 * Thread-local context that carries additional parameters for a {@code RegisterTableRequest} which
 * are not present on the generated Iceberg request type.
 *
 * <p>The current implementation only records the caller's intent to "overwrite" an existing table.
 * Using a ThreadLocal keeps the flag local to the request lifecycle and avoids changing the
 * generated DTOs. Callers must clear the flag (typically in a finally block) to avoid leaking state
 * across requests.
 */
public class RegisterTableRequestContext {
  private static final ThreadLocal<Boolean> SHOULD_OVERWRITE = ThreadLocal.withInitial(() -> false);

  /**
   * Record whether the current request intends to overwrite an existing table.
   *
   * @param overwrite true if an existing table's metadata-location should be replaced
   */
  public static void setOverwrite(boolean overwrite) {
    SHOULD_OVERWRITE.set(overwrite);
  }

  /**
   * Returns whether the current request indicated overwrite semantics. Defaults to {@code false}.
   */
  public static boolean getOverwrite() {
    return SHOULD_OVERWRITE.get();
  }

  /** Clear the thread-local state for the current request. Call this in a finally block. */
  public static void clear() {
    SHOULD_OVERWRITE.remove();
  }
}
