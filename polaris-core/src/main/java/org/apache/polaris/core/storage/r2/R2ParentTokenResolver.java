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
package org.apache.polaris.core.storage.r2;

import java.util.Optional;
import org.jspecify.annotations.Nullable;

/**
 * Resolves the R2 parent token for a catalog. A catalog with a {@code storageName} resolves to the
 * named server-side entry; a catalog without one resolves to the default entry. Absence means the
 * server is not configured for that catalog, and vending must fail.
 */
@FunctionalInterface
public interface R2ParentTokenResolver {

  Optional<R2ParentToken> resolve(@Nullable String storageName);

  /** A resolver with no tokens; every lookup is empty. */
  static R2ParentTokenResolver none() {
    return storageName -> Optional.empty();
  }
}
