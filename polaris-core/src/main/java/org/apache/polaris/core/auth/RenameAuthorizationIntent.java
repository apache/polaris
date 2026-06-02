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

import com.google.common.base.Preconditions;
import org.jspecify.annotations.NonNull;

/** Authorization intent for rename operations over a source and destination securable. */
public record RenameAuthorizationIntent(
    @NonNull PolarisAuthorizableOperation operation,
    @NonNull PolarisSecurable from,
    @NonNull PolarisSecurable to)
    implements AuthorizationIntent {
  public RenameAuthorizationIntent {
    Preconditions.checkNotNull(operation, "operation must be non-null");
    Preconditions.checkNotNull(from, "from must be non-null");
    Preconditions.checkNotNull(to, "to must be non-null");
  }

  @Override
  public @NonNull PolarisAuthorizableOperation getOperation() {
    return operation;
  }
}
