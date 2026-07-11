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

import java.util.Objects;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.jspecify.annotations.NonNull;

/**
 * Authorization state shared across authorization phases for one resolved view.
 *
 * <p>Used to carry authorization-specific data for the current resolution flow, such as a {@link
 * PolarisResolutionManifest} created by the caller and populated by the authorizer.
 */
public class AuthorizationState {
  private final PolarisResolutionManifest resolutionManifest;

  public AuthorizationState(@NonNull PolarisResolutionManifest resolutionManifest) {
    this.resolutionManifest = Objects.requireNonNull(resolutionManifest, "resolutionManifest");
  }

  /** Returns the resolution manifest used for the current authorization flow. */
  @NonNull
  public PolarisResolutionManifest getResolutionManifest() {
    return resolutionManifest;
  }
}
