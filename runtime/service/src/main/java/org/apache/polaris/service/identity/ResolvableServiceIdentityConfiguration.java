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

package org.apache.polaris.service.identity;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.polaris.core.identity.resolved.ResolvedServiceIdentity;

/**
 * Represents a service identity configuration that can be resolved into a fully initialized {@link
 * ResolvedServiceIdentity}.
 *
 * <p>This interface allows identity configurations (e.g., AWS IAM) to encapsulate the logic
 * required to construct runtime credentials and metadata needed to authenticate as a
 * Polaris-managed service identity.
 */
public interface ResolvableServiceIdentityConfiguration {
  /**
   * Attempts to resolve this configuration into a {@link ResolvedServiceIdentity}.
   *
   * @return an optional resolved service identity, or empty if resolution fails or is not
   *     configured
   */
  default Optional<? extends ResolvedServiceIdentity> resolve(@Nonnull String realmIdentifier) {
    return Optional.empty();
  }
}
