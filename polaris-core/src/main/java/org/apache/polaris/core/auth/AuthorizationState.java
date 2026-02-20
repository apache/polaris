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

import jakarta.annotation.Nonnull;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;

/**
 * Request-scoped authorization state shared across authorization phases.
 *
 * <p>Distinct from {@link org.apache.polaris.core.context.CallContext}. This state is intended to
 * carry authorization-specific data, such as a {@link PolarisResolutionManifest} created by the
 * caller and populated by the authorizer.
 */
public class AuthorizationState {
  private final AtomicReference<PolarisResolutionManifest> resolutionManifest =
      new AtomicReference<>();

  /** Returns the request-scoped resolution manifest used for authorization. */
  @Nonnull
  public PolarisResolutionManifest getResolutionManifest() {
    PolarisResolutionManifest manifest = resolutionManifest.get();
    if (manifest == null) {
      throw new IllegalStateException("AuthorizationState resolution manifest is not set");
    }
    return manifest;
  }

  /** Sets the request-scoped resolution manifest used for authorization. */
  public void setResolutionManifest(@Nonnull PolarisResolutionManifest resolutionManifest) {
    Objects.requireNonNull(resolutionManifest, "resolutionManifest");
    if (!this.resolutionManifest.compareAndSet(null, resolutionManifest)) {
      throw new IllegalStateException("AuthorizationState resolution manifest already set");
    }
  }
}
