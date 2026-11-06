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
import java.util.List;
import java.util.function.Consumer;
import org.jspecify.annotations.NonNull;

/**
 * Authorization intent for operations with one explicit target.
 *
 * <p>{@code referencedBy} is the chain of securables (typically views) through which the target is
 * being accessed, outermost first, as asserted by the client; it is empty when the request carries
 * no chain. The chain is not visited by {@link #visitSecurables(Consumer)}: an authorizer that
 * wants the chain entities resolved must register them itself in {@code
 * resolveAuthorizationInputs}.
 */
public record SingleTargetAuthorizationIntent(
    @NonNull PolarisAuthorizableOperation operation,
    @NonNull PolarisSecurable target,
    @NonNull List<PolarisSecurable> referencedBy)
    implements AuthorizationIntent {
  public SingleTargetAuthorizationIntent {
    Preconditions.checkNotNull(operation, "operation must be non-null");
    Preconditions.checkNotNull(target, "target must be non-null");
    Preconditions.checkNotNull(referencedBy, "referencedBy must be non-null");
    referencedBy = List.copyOf(referencedBy);
  }

  public SingleTargetAuthorizationIntent(
      @NonNull PolarisAuthorizableOperation operation, @NonNull PolarisSecurable target) {
    this(operation, target, List.of());
  }

  @Override
  public void visitSecurables(Consumer<PolarisSecurable> visitor) {
    visitor.accept(target);
  }
}
